package rewards

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

// fakeSteps is an in-memory saga store with injectable failures.
type fakeSteps struct {
	mu        sync.Mutex
	state     map[string]string
	credits   map[int64]int64
	pool      int64
	transient map[int64]int // player -> number of Grant calls that fail transiently
	gone      map[int64]bool
}

func (f *fakeSteps) Reserve(_ context.Context, r store.Reward) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.state[r.ID]; ok {
		return s, nil
	}
	if f.pool < r.Credits {
		f.state[r.ID] = store.SagaRejected
		return store.SagaRejected, nil
	}
	f.pool -= r.Credits
	f.state[r.ID] = store.SagaReserved
	return store.SagaReserved, nil
}

func (f *fakeSteps) Grant(_ context.Context, r store.Reward) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state[r.ID] != store.SagaReserved {
		return nil
	}
	if f.transient[r.PlayerID] > 0 {
		f.transient[r.PlayerID]--
		return errors.New("db unavailable")
	}
	if f.gone[r.PlayerID] {
		return store.ErrPlayerGone
	}
	f.credits[r.PlayerID] += r.Credits
	f.state[r.ID] = store.SagaGranted
	return nil
}

func (f *fakeSteps) Compensate(_ context.Context, id, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state[id] == store.SagaReserved {
		f.state[id] = store.SagaCompensated
		f.pool += 10 * 7 // test rewards are 7 points
	}
	return nil
}

func (f *fakeSteps) snapshot() (map[string]string, map[int64]int64, int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, cr := map[string]string{}, map[int64]int64{}
	for k, v := range f.state {
		st[k] = v
	}
	for k, v := range f.credits {
		cr[k] = v
	}
	return st, cr, f.pool
}

func TestSagaOverKafka(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, TopicMatch, TopicCommands, TopicRetry, TopicDLQ))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	steps := &fakeSteps{state: map[string]string{}, credits: map[int64]int64{}, pool: 1_000_000,
		transient: map[int64]int{2: 2, 4: 100}, gone: map[int64]bool{3: true}}
	w := &Worker{Brokers: c.ListenAddrs(), Steps: steps, Log: slog.New(slog.DiscardHandler), MaxAttempts: 3,
		Backoff: func(uint32) time.Duration { return 50 * time.Millisecond }, Group: "t"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	cl, _ := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	defer cl.Close()
	// Player 1 ok; 2 transient x2 then ok; 3 gone -> compensation; 4 always failing -> DLQ;
	// a guest (no credits); player 1 again in the duplicate redelivery below.
	results := []*eventsv1.PlayerResult{{PlayerId: 1, Score: 7}, {PlayerId: 2, Score: 7}, {PlayerId: 3, Score: 7},
		{PlayerId: 4, Score: 7}, {PlayerId: GuestIDBase + 5, Score: 7}}
	mc := &eventsv1.Envelope{EventId: "mc-1", Payload: &eventsv1.Envelope_MatchCompleted{MatchCompleted: &eventsv1.MatchCompleted{MatchId: "run/m-1/r0", Results: results}}}
	b, _ := proto.Marshal(mc)
	for range 2 { // the same MatchCompleted delivered twice (producer retry / replay)
		cl.ProduceSync(t.Context(), &kgo.Record{Topic: TopicMatch, Key: []byte("run/m-1/r0"), Value: b})
	}
	cl.ProduceSync(t.Context(), &kgo.Record{Topic: TopicCommands, Key: []byte("9"), Value: []byte{0xff, 0x00}}) // poison

	deadline := time.Now().Add(15 * time.Second)
	for {
		st, credits, _ := steps.snapshot()
		done := credits[1] == 70 && credits[2] == 70 && countState(st, store.SagaCompensated) == 1 && dlqCount(t, c) >= 2
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: states=%v credits=%v dlq=%d", st, credits, dlqCount(t, c))
		}
		time.Sleep(100 * time.Millisecond)
	}
	st, credits, pool := steps.snapshot()
	if credits[1] != 70 {
		t.Fatalf("duplicate MatchCompleted credited player 1 twice: %d", credits[1])
	}
	if credits[GuestIDBase+5] != 0 || len(st) != 4 {
		t.Fatalf("guests must not get rewards; sagas=%v", st)
	}
	// 1 and 2 granted (140), 3 compensated (returned), 4 still reserved after DLQ (needs operator action).
	if pool != 1_000_000-70*3 {
		t.Fatalf("pool = %d", pool)
	}
	// The duplicate MatchCompleted produced a second command with the same
	// reward_id; both copies exhaust retries. Two distinct commands are dead-lettered.
	if n := dlqDistinct(t, c); n != 2 {
		t.Fatalf("distinct dlq keys = %d, want poison + exhausted", n)
	}
}

func countState(st map[string]string, s string) int {
	n := 0
	for _, v := range st {
		if v == s {
			n++
		}
	}
	return n
}

func dlqCount(t *testing.T, c *kfake.Cluster) int { n, _ := dlq(t, c); return n }

func dlqDistinct(t *testing.T, c *kfake.Cluster) int { _, keys := dlq(t, c); return len(keys) }

func dlq(t *testing.T, c *kfake.Cluster) (int, map[string]bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(TopicDLQ), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	n, keys := 0, map[string]bool{}
	for ctx.Err() == nil {
		recs := cl.PollFetches(ctx).Records()
		n += len(recs)
		for _, r := range recs {
			keys[string(r.Key)] = true
			for _, h := range r.Headers {
				if h.Key == headerError && len(h.Value) == 0 {
					t.Fatal("DLQ record without error header")
				}
			}
		}
	}
	return n, keys
}
