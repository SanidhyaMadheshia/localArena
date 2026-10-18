// Package events publishes LocalArena domain events to Kafka without ever
// blocking the caller (the game tick loop). See adr/0009.
//
//	Publish (tick goroutine)  --non-blocking-->  bounded queue  -->  pump  --TryProduce-->  franz-go buffer --> Kafka
//	                                                                   |                         | delivery failed / buffer full
//	                                                                   +-- best-effort: drop      +--> durable: disk spool --> replayed later
//
// Classes:
//   - Durable: gameplay facts (match started/completed, scores, joins). Spooled
//     to disk when Kafka is unavailable and replayed when it returns. Event IDs
//     are deterministic, so a replayed duplicate is recognisable downstream.
//   - BestEffort: telemetry. Dropped under any pressure.
//
// Critical economy events do not use this package: they go through the
// PostgreSQL outbox and Debezium (adr/0010).
package events

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/eventid"
)

// Class selects the durability policy of an event.
type Class int

const (
	BestEffort Class = iota
	Durable
)

func (c Class) String() string { return [...]string{"best_effort", "durable"}[c] }

// Topic names (scripts/kafka-topics.sh).
const (
	TopicMatch     = "game.match-events.v1"
	TopicPlayer    = "game.player-events.v1"
	TopicTelemetry = "game.server-telemetry.v1"
	SchemaVersion  = 1
)

var (
	results = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "localarena", Subsystem: "events", Name: "total",
		Help: "Events by class and outcome: queued, delivered, spooled, replayed, dropped_queue_full, dropped_unavailable, dropped_spool_full, failed.",
	}, []string{"class", "result"})
	spoolBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "localarena", Subsystem: "events", Name: "spool_bytes",
		Help: "Bytes of durable events waiting on disk for Kafka.",
	})
	queueLen = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "localarena", Subsystem: "events", Name: "queue_length",
		Help: "Events waiting in the in-memory queue for the producer.",
	})
)

func count(c Class, result string) { results.WithLabelValues(c.String(), result).Inc() }

// Options configures a Publisher.
type Options struct {
	Brokers         []string
	Source          string        // e.g. "gameserver/<host>"
	SpoolDir        string        // empty disables the spool (durable events are then dropped when Kafka is down)
	MaxSpoolBytes   int64         // default 256 MiB
	QueueSize       int           // in-memory queue between Publish and the producer, default 8192
	MaxBuffered     int           // franz-go buffered records before TryProduce fails, default 10000
	DeliveryTimeout time.Duration // per record, after which a durable record is spooled; default 10 s
	ReplayInterval  time.Duration // how often to try draining the spool, default 2 s
}

type item struct {
	rec   *kgo.Record
	class Class
}

// Publisher is safe for concurrent use. A nil *Publisher is a valid no-op.
type Publisher struct {
	opts   Options
	cl     *kgo.Client
	q      chan item
	spool  *spool
	log    *slog.Logger
	cancel context.CancelFunc
	stop   chan struct{} // stops the pump; Close then drains the queue itself (keeps order)
	pumped chan struct{}
	wg     sync.WaitGroup
}

// New starts a publisher. It does not wait for Kafka: an unreachable cluster
// only means events spool or drop, never that the game cannot start.
func New(opts Options, log *slog.Logger) (*Publisher, error) {
	if opts.MaxSpoolBytes == 0 {
		opts.MaxSpoolBytes = 256 << 20
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = 8192
	}
	if opts.MaxBuffered == 0 {
		opts.MaxBuffered = 10_000
	}
	if opts.DeliveryTimeout == 0 {
		opts.DeliveryTimeout = 10 * time.Second
	}
	if opts.ReplayInterval == 0 {
		opts.ReplayInterval = 2 * time.Second
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		// Idempotent producer with acks=all (franz-go defaults): broker-side
		// dedupe of retried batches and no loss while min ISR (2) is met.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(20*time.Millisecond), // batch up to 20 ms: one batch per tick at 20 Hz
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.MaxBufferedRecords(opts.MaxBuffered),
		kgo.RecordDeliveryTimeout(opts.DeliveryTimeout),
		kgo.UnknownTopicRetries(-1),
	)
	if err != nil {
		return nil, err
	}
	p := &Publisher{opts: opts, cl: cl, q: make(chan item, opts.QueueSize), log: log,
		stop: make(chan struct{}), pumped: make(chan struct{})}
	if opts.SpoolDir != "" {
		if p.spool, err = openSpool(opts.SpoolDir, opts.MaxSpoolBytes); err != nil {
			cl.Close()
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.pump()
	if p.spool != nil {
		p.wg.Go(func() { p.replayLoop(ctx) })
	}
	return p, nil
}

// Publish enqueues an event and returns immediately. It reports false if the
// event was dropped because the queue is full (counted in metrics).
func (p *Publisher) Publish(topic, key string, env *eventsv1.Envelope, class Class) bool {
	if p == nil {
		return false
	}
	now := time.Now()
	if env.ProducedAt == nil {
		env.ProducedAt = timestamppb.New(now)
	}
	if env.EventTime == nil {
		env.EventTime = env.ProducedAt
	}
	env.SchemaVersion = SchemaVersion
	if env.Source == "" {
		env.Source = p.opts.Source
	}
	val, err := proto.Marshal(env)
	if err != nil {
		count(class, "failed")
		return false
	}
	// Record timestamp = produce time, NOT event time: franz-go measures the
	// delivery timeout from Record.Timestamp, so a replayed record stamped with
	// its old event time would be failed instantly, forever (Experiment 004).
	// Event time travels in the Envelope; Kafka Streams extracts it from there.
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: val, Timestamp: now,
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(env.EventId)},
			{Key: "content-type", Value: []byte("application/x-protobuf; message=localarena.events.v1.Envelope")},
		}}
	select {
	case p.q <- item{rec, class}:
		count(class, "queued")
		return true
	default:
		count(class, "dropped_queue_full")
		return false
	}
}

// pump moves records from the queue into franz-go without blocking on it.
// Records are bounded by DeliveryTimeout, not by a context.
func (p *Publisher) pump() {
	defer close(p.pumped)
	for {
		select {
		case <-p.stop:
			return
		case it := <-p.q:
			queueLen.Set(float64(len(p.q)))
			p.produce(it)
		}
	}
}

func (p *Publisher) produce(it item) {
	p.cl.TryProduce(context.Background(), it.rec, func(r *kgo.Record, err error) { p.onResult(it.class, r, err) })
}

// onResult runs on franz-go's promise goroutine; it must not block on the client.
func (p *Publisher) onResult(c Class, r *kgo.Record, err error) {
	switch {
	case err == nil:
		count(c, "delivered")
	case errors.Is(err, context.Canceled):
		p.spoolOrDrop(c, r) // shutting down: keep durable events
	case c == BestEffort:
		count(c, "dropped_unavailable")
	default:
		p.spoolOrDrop(c, r)
	}
}

func (p *Publisher) spoolOrDrop(c Class, r *kgo.Record) {
	if c != Durable || p.spool == nil {
		count(c, "dropped_unavailable")
		return
	}
	switch err := p.spool.append(r); {
	case err == nil:
		count(c, "spooled")
	case errors.Is(err, errSpoolFull):
		count(c, "dropped_spool_full")
	default:
		count(c, "failed")
		p.log.Error("event spool write failed", "err", err)
	}
	spoolBytes.Set(float64(p.spool.size()))
}

// replayLoop drains the spool when Kafka is reachable. Replayed records keep
// their event IDs, so a record that was in fact delivered before the failure
// becomes a recognisable duplicate rather than a new fact.
func (p *Publisher) replayLoop(ctx context.Context) {
	t := time.NewTicker(p.opts.ReplayInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if p.spool.size() == 0 && !p.spool.hasPending() {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := p.cl.Ping(pctx)
		cancel()
		if err != nil {
			continue
		}
		n, err := p.spool.drain(func(batch []*kgo.Record) error {
			now := time.Now()
			for _, r := range batch {
				r.Timestamp = now // re-stamp: see Publish
			}
			sctx, cancel := context.WithTimeout(ctx, p.opts.DeliveryTimeout)
			defer cancel()
			return p.cl.ProduceSync(sctx, batch...).FirstErr()
		})
		results.WithLabelValues(Durable.String(), "replayed").Add(float64(n))
		spoolBytes.Set(float64(p.spool.size()))
		if err != nil {
			p.log.Warn("spool replay interrupted; will retry", "replayed", n, "err", err)
		} else if n > 0 {
			p.log.Info("spool replayed", "records", n)
		}
	}
}

// Close flushes what Kafka will accept within ctx, spools the rest, and stops.
func (p *Publisher) Close(ctx context.Context) {
	if p == nil {
		return
	}
	// Stop the pump first so the remaining queue is produced in order, then flush.
	close(p.stop)
	<-p.pumped
	for drained := false; !drained; {
		select {
		case it := <-p.q:
			p.produce(it)
		default:
			drained = true
		}
	}
	if err := p.cl.Flush(ctx); err != nil {
		p.log.Warn("event flush incomplete; undelivered durable events are spooled", "err", err)
	}
	p.cancel()
	p.cl.Close() // fails remaining buffered records -> promises spool them
	p.wg.Wait()
	if p.spool != nil {
		_ = p.spool.close()
	}
}

// EventID is eventid.New (deterministic IDs), re-exported for producers.
func EventID(parts ...string) string { return eventid.New(parts...) }
