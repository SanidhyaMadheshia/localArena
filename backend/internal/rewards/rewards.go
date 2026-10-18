// Package rewards runs the match-reward saga (adr/0012) over Kafka.
//
//	game.match-events.v1 --[planner]--> game.reward-commands.v1 --[saga]--> Postgres steps
//	                                                   ^      |  transient failure (DB down)
//	                                                   |      v
//	                               game.reward-commands-retry.v1 (delayed by not-before header)
//	                                                          |  attempts exhausted / poison
//	                                                          v
//	                                            game.reward-commands-dlq.v1
//
// Delivery is at-least-once everywhere: offsets are committed only after the
// record's work (and any produced follow-up) is durable. Correctness under
// redelivery comes from the deterministic reward_id and state-guarded steps.
package rewards

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/eventid"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

// Topics.
const (
	TopicMatch    = "game.match-events.v1"
	TopicCommands = "game.reward-commands.v1"
	TopicRetry    = "game.reward-commands-retry.v1"
	TopicDLQ      = "game.reward-commands-dlq.v1"
	Season        = "s1"

	// GuestIDBase mirrors match.GuestIDBase: guests earn no credits.
	GuestIDBase      = 1_000_000_000
	CreditsPerPoint  = 10
	headerNotBefore  = "not-before"
	headerError      = "error"
	headerSourceTopc = "source-topic"
)

var outcomes = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "localarena", Subsystem: "rewards", Name: "outcomes_total",
	Help: "Reward saga record outcomes: planned, granted, rejected, compensated, duplicate, retried, dead_lettered.",
}, []string{"outcome"})

// Steps is the saga's view of the database (store.Store satisfies it).
type Steps interface {
	Reserve(context.Context, store.Reward) (string, error)
	Grant(context.Context, store.Reward) error
	Compensate(ctx context.Context, id, reason string) error
}

// Worker runs the planner and saga consumers.
type Worker struct {
	Brokers     []string
	Steps       Steps
	Log         *slog.Logger
	MaxAttempts uint32                             // default 5
	Backoff     func(attempt uint32) time.Duration // default 1s * 2^attempt, cap 60s
	Group       string                             // consumer group prefix, default "localarena-rewards"
}

func (w *Worker) defaults() {
	if w.MaxAttempts == 0 {
		w.MaxAttempts = 5
	}
	if w.Backoff == nil {
		w.Backoff = func(a uint32) time.Duration { return min(time.Second<<min(a, 6), 60*time.Second) }
	}
	if w.Group == "" {
		w.Group = "localarena-rewards"
	}
}

// Run blocks until ctx is cancelled. Each consumer is supervised: if it stops
// on an error (e.g. Kafka unavailable while producing a follow-up), it is
// recreated and resumes from its last committed offset.
func (w *Worker) Run(ctx context.Context) error {
	w.defaults()
	var wg sync.WaitGroup
	for _, c := range []struct {
		group  string
		topics []string
		h      handler
	}{
		{w.Group + "-planner", []string{TopicMatch}, w.plan},
		{w.Group + "-saga", []string{TopicCommands}, w.handle},
		{w.Group + "-retry", []string{TopicRetry}, w.handleRetry},
	} {
		wg.Go(func() {
			for ctx.Err() == nil {
				if err := w.consume(ctx, c.group, c.topics, c.h); err != nil && ctx.Err() == nil {
					w.Log.Warn("consumer stopped; restarting from committed offsets", "group", c.group, "err", err)
					select {
					case <-time.After(2 * time.Second):
					case <-ctx.Done():
					}
				}
			}
		})
	}
	wg.Wait()
	return nil
}

// handler processes one record and may return records to produce; they are
// produced (acks=all) before the source offset is committed.
type handler func(ctx context.Context, r *kgo.Record) ([]*kgo.Record, error)

func (w *Worker) consume(ctx context.Context, group string, topics []string, h handler) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(w.Brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) { w.Log.Warn("fetch error", "topic", t, "partition", p, "err", err) })
		var done []*kgo.Record
		var failed error
		for _, r := range fs.Records() {
			out, err := h(ctx, r)
			if err == nil && len(out) > 0 {
				err = cl.ProduceSync(ctx, out...).FirstErr()
			}
			if err != nil {
				failed = fmt.Errorf("%s offset %d: %w", r.Topic, r.Offset, err)
				break
			}
			done = append(done, r)
		}
		if len(done) > 0 {
			if err := cl.CommitRecords(ctx, done...); err != nil {
				w.Log.Warn("commit failed; records will be redelivered (idempotent)", "err", err)
			}
		}
		cl.AllowRebalance()
		if failed != nil {
			return failed // the failed record and everything after it are redelivered
		}
	}
	return nil
}

func marshal(topic string, key []byte, env *eventsv1.Envelope, hdr ...kgo.RecordHeader) *kgo.Record {
	b, _ := proto.Marshal(env)
	return &kgo.Record{Topic: topic, Key: key, Value: b, Headers: hdr}
}

// plan turns MatchCompleted into one RewardCommand per account player with
// points. reward_id = hash(event_id, player): a redelivered MatchCompleted
// plans the same commands, which the saga then treats as duplicates.
func (w *Worker) plan(_ context.Context, r *kgo.Record) ([]*kgo.Record, error) {
	var env eventsv1.Envelope
	if proto.Unmarshal(r.Value, &env) != nil || env.GetMatchCompleted() == nil {
		return nil, nil // not ours (or invalid: Streams copies those to game.invalid-events.v1)
	}
	mc := env.GetMatchCompleted()
	var out []*kgo.Record
	for _, res := range mc.Results {
		if res.Score == 0 || res.PlayerId >= GuestIDBase || res.PlayerId <= 0 {
			continue
		}
		id := eventid.New("reward", env.EventId, strconv.FormatInt(res.PlayerId, 10))
		cmd := &eventsv1.Envelope{EventId: id, EventTime: env.EventTime, ProducedAt: timestamppb.Now(), SchemaVersion: 1,
			CorrelationId: env.CorrelationId, CausationId: env.EventId, Source: "rewards/planner",
			Payload: &eventsv1.Envelope_RewardCommand{RewardCommand: &eventsv1.RewardCommand{
				RewardId: id, PlayerId: res.PlayerId, Credits: int64(res.Score) * CreditsPerPoint, MatchId: mc.MatchId}}}
		out = append(out, marshal(TopicCommands, []byte(strconv.FormatInt(res.PlayerId, 10)), cmd))
		outcomes.WithLabelValues("planned").Inc()
	}
	return out, nil
}

func (w *Worker) handleRetry(ctx context.Context, r *kgo.Record) ([]*kgo.Record, error) {
	for _, h := range r.Headers {
		if h.Key == headerNotBefore {
			if nb, err := time.Parse(time.RFC3339Nano, string(h.Value)); err == nil {
				// Delay queue: all retries share one backoff schedule per attempt,
				// so waiting on the head of the partition is acceptable here.
				select {
				case <-time.After(time.Until(nb)):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
	}
	return w.handle(ctx, r)
}

// handle runs the saga for one command. Permanent failures end in a terminal
// state (rejected/compensated); transient ones go to the retry topic; a poison
// command or exhausted retries go to the DLQ. It returns an error only if it
// could not even decide (then the record is redelivered).
func (w *Worker) handle(ctx context.Context, r *kgo.Record) ([]*kgo.Record, error) {
	var env eventsv1.Envelope
	if err := proto.Unmarshal(r.Value, &env); err != nil || env.GetRewardCommand() == nil {
		outcomes.WithLabelValues("dead_lettered").Inc()
		return []*kgo.Record{w.dlq(r, "undecodable reward command")}, nil
	}
	c := env.GetRewardCommand()
	rw := store.Reward{ID: c.RewardId, Season: Season, MatchID: c.MatchId, PlayerID: c.PlayerId, Credits: c.Credits}
	if rw.ID == "" || rw.Credits <= 0 {
		outcomes.WithLabelValues("dead_lettered").Inc()
		return []*kgo.Record{w.dlq(r, "invalid reward command")}, nil
	}

	state, err := w.Steps.Reserve(ctx, rw)
	if err != nil {
		return w.retry(r, &env, err)
	}
	switch state {
	case store.SagaRejected:
		outcomes.WithLabelValues("rejected").Inc()
		return nil, nil
	case store.SagaGranted, store.SagaCompensated:
		outcomes.WithLabelValues("duplicate").Inc()
		return nil, nil
	}
	switch err := w.Steps.Grant(ctx, rw); {
	case err == nil:
		outcomes.WithLabelValues("granted").Inc()
		return nil, nil
	case errors.Is(err, store.ErrPlayerGone):
		if cerr := w.Steps.Compensate(ctx, rw.ID, err.Error()); cerr != nil {
			return w.retry(r, &env, cerr) // compensation itself must eventually happen
		}
		outcomes.WithLabelValues("compensated").Inc()
		return nil, nil
	default:
		return w.retry(r, &env, err)
	}
}

// retry republishes the command with attempt+1 and a not-before time, or
// dead-letters it once attempts are exhausted. The saga row (if any) stays in
// "reserved"; the next attempt resumes from there.
func (w *Worker) retry(r *kgo.Record, env *eventsv1.Envelope, cause error) ([]*kgo.Record, error) {
	c := proto.Clone(env).(*eventsv1.Envelope)
	cmd := c.GetRewardCommand()
	cmd.Attempt++
	cmd.LastError = cause.Error()
	if cmd.Attempt > w.MaxAttempts {
		outcomes.WithLabelValues("dead_lettered").Inc()
		return []*kgo.Record{w.dlq(r, fmt.Sprintf("retries exhausted: %v", cause))}, nil
	}
	outcomes.WithLabelValues("retried").Inc()
	nb := time.Now().Add(w.Backoff(cmd.Attempt))
	return []*kgo.Record{marshal(TopicRetry, r.Key, c, kgo.RecordHeader{Key: headerNotBefore, Value: []byte(nb.Format(time.RFC3339Nano))})}, nil
}

func (w *Worker) dlq(r *kgo.Record, reason string) *kgo.Record {
	w.Log.Warn("dead-lettering reward command", "reason", reason, "topic", r.Topic, "offset", r.Offset)
	return &kgo.Record{Topic: TopicDLQ, Key: r.Key, Value: r.Value, Headers: append(r.Headers,
		kgo.RecordHeader{Key: headerError, Value: []byte(reason)},
		kgo.RecordHeader{Key: headerSourceTopc, Value: []byte(fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset))})}
}
