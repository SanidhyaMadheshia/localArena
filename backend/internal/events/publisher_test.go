package events

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
)

func scored(match string, player int64, score uint32, tick uint64) *eventsv1.Envelope {
	return &eventsv1.Envelope{
		EventId:       EventID("scored", match, string(rune(player)), string(rune(tick))),
		CorrelationId: match,
		Payload: &eventsv1.Envelope_PlayerScored{PlayerScored: &eventsv1.PlayerScored{
			MatchId: match, PlayerId: player, Score: score, Tick: tick}},
	}
}

func cluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, TopicMatch, TopicTelemetry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func consumeN(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("got %d of %d records", len(out), n)
		}
		out = append(out, fs.Records()...)
	}
	return out
}

func TestDeliversWithKeyAndEnvelope(t *testing.T) {
	c := cluster(t)
	p, err := New(Options{Brokers: c.ListenAddrs(), Source: "test"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		p.Publish(TopicMatch, "m-1", scored("m-1", 7, uint32(i), uint64(i)), Durable) // #nosec G115 -- small test values
	}
	p.Close(t.Context())
	recs := consumeN(t, c.ListenAddrs(), TopicMatch, 10)
	parts := map[int32]bool{}
	for i, r := range recs {
		parts[r.Partition] = true
		var env eventsv1.Envelope
		if err := proto.Unmarshal(r.Value, &env); err != nil || string(r.Key) != "m-1" {
			t.Fatalf("record %d: key=%q err=%v", i, r.Key, err)
		}
		if env.Source != "test" || env.SchemaVersion != 1 || env.ProducedAt == nil || env.GetPlayerScored().GetScore() != uint32(i) { // #nosec G115
			t.Fatalf("envelope %d = %+v", i, &env)
		}
	}
	if len(parts) != 1 {
		t.Fatalf("one key must map to one partition (per-match order), got %v", parts)
	}
}

// Kafka unreachable: Publish never blocks, durable events spool to disk,
// best-effort events drop; a restarted publisher replays the spool when Kafka
// is back, with the original event IDs.
func TestKafkaDownSpoolsThenReplays(t *testing.T) {
	dir := t.TempDir()
	p, err := New(Options{Brokers: []string{"127.0.0.1:1"}, SpoolDir: dir, DeliveryTimeout: time.Second},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ids := map[string]bool{}
	for i := range 2000 {
		env := scored("m-2", int64(i%20), uint32(i), uint64(i)) // #nosec G115
		ids[env.EventId] = true
		p.Publish(TopicMatch, "m-2", env, Durable)
		p.Publish(TopicTelemetry, "srv", &eventsv1.Envelope{EventId: "t"}, BestEffort)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("4000 Publish calls took %v with Kafka down; must not block", d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.spool.size() == 0 || countSpooled(t, dir) < 2000 {
		if time.Now().After(deadline) {
			t.Fatalf("spooled %d of 2000", countSpooled(t, dir))
		}
		time.Sleep(50 * time.Millisecond)
	}
	p.Close(t.Context()) // "crash"/restart boundary: spool stays on disk

	c := cluster(t)
	p2, err := New(Options{Brokers: c.ListenAddrs(), SpoolDir: dir, ReplayInterval: 100 * time.Millisecond}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close(t.Context())
	recs := consumeN(t, c.ListenAddrs(), TopicMatch, 2000)
	for _, r := range recs {
		var env eventsv1.Envelope
		_ = proto.Unmarshal(r.Value, &env)
		if !ids[env.EventId] {
			t.Fatalf("replayed unknown event id %q", env.EventId)
		}
		delete(ids, env.EventId)
	}
	if len(ids) != 0 {
		t.Fatalf("%d events lost", len(ids))
	}
	if n := countTelemetry(t, c); n != 0 {
		t.Fatalf("best-effort events must not be spooled, got %d", n)
	}
}

func TestSpoolIsBounded(t *testing.T) {
	s, err := openSpool(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	r := &kgo.Record{Topic: TopicMatch, Key: []byte("k"), Value: make([]byte, 300)}
	ok := 0
	for range 10 {
		if s.append(r) == nil {
			ok++
		}
	}
	if ok != 3 || s.append(r) != errSpoolFull {
		t.Fatalf("appended %d records of 300 B into 1 KiB", ok)
	}
}

// A crash mid-append leaves a torn frame; replay keeps every complete record.
func TestSpoolToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSpool(dir, 1<<20)
	for i := range 5 {
		_ = s.append(&kgo.Record{Topic: TopicMatch, Key: []byte{byte(i)}, Value: []byte("v")})
	}
	_ = s.close()
	f, _ := os.OpenFile(filepath.Join(dir, activeName), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.Write([]byte{1, 2, 3, 4, 0, 0, 1, 0, 9}) // half a frame
	_ = f.Close()
	recs, err := readAll(filepath.Join(dir, activeName))
	if err != nil || len(recs) != 5 || recs[4].Key[0] != 4 {
		t.Fatalf("recovered %d records, err %v", len(recs), err)
	}
}

func TestEventIDDeterministic(t *testing.T) {
	a, b := EventID("a", "b"), EventID("a", "b")
	if a != b || a == EventID("ab", "") {
		t.Fatal("event ids must be deterministic and unambiguous")
	}
}

func countSpooled(t *testing.T, dir string) int {
	t.Helper()
	recs, _ := readAll(filepath.Join(dir, activeName))
	return len(recs)
}

func countTelemetry(t *testing.T, c *kfake.Cluster) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(TopicTelemetry), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	return len(cl.PollFetches(ctx).Records())
}

// Regression (Experiment 004): records that sat in the spool longer than the
// delivery timeout must still replay; they used to fail instantly forever
// because the delivery deadline was measured from their old timestamp.
func TestReplayAfterDeliveryTimeoutElapsed(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSpool(dir, 1<<20)
	old := time.Now().Add(-time.Hour)
	for i := range 50 {
		_ = s.append(&kgo.Record{Topic: TopicMatch, Key: []byte("m"), Value: []byte{byte(i)}, Timestamp: old})
	}
	_ = s.close()
	c := cluster(t)
	p, err := New(Options{Brokers: c.ListenAddrs(), SpoolDir: dir, DeliveryTimeout: time.Second, ReplayInterval: 50 * time.Millisecond},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(t.Context())
	if recs := consumeN(t, c.ListenAddrs(), TopicMatch, 50); len(recs) != 50 {
		t.Fatalf("replayed %d", len(recs))
	}
}
