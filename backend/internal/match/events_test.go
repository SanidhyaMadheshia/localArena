package match

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/events"
)

// A short round publishes MatchStarted, PlayerJoined, MatchCompleted (and the
// next MatchStarted) with deterministic, distinct IDs keyed by the run ID.
func TestRoundPublishesEvents(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, events.TopicMatch, events.TopicPlayer, events.TopicTelemetry))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pub, err := events.New(events.Options{Brokers: c.ListenAddrs(), Source: "test"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	mg := NewManager(Options{TickHz: 100, RoundTicks: 20, Events: pub, InstanceID: "itest"})
	m, cl, err := mg.Join(42)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mg.Join(42); err != ErrAlreadyPlaying {
		t.Fatalf("second session for account 42: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // > 1 round at 100 Hz
	mg.Leave(m, cl)
	mg.Close()
	pub.Close(t.Context())

	cons, _ := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(events.TopicMatch, events.TopicPlayer),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cons.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	kinds, ids := map[string]int{}, map[string]bool{}
	for ctx.Err() == nil && kinds["completed"] == 0 {
		for _, r := range cons.PollFetches(ctx).Records() {
			var env eventsv1.Envelope
			if proto.Unmarshal(r.Value, &env) != nil {
				t.Fatal("bad envelope")
			}
			if ids[env.EventId] {
				t.Fatalf("duplicate event id %s", env.EventId)
			}
			ids[env.EventId] = true
			switch p := env.Payload.(type) {
			case *eventsv1.Envelope_MatchStarted:
				kinds["started"]++
			case *eventsv1.Envelope_PlayerJoined:
				kinds["joined"]++
				if p.PlayerJoined.PlayerId != 42 || string(r.Key) != "42" {
					t.Fatalf("joined %+v key %s", p, r.Key)
				}
			case *eventsv1.Envelope_MatchCompleted:
				kinds["completed"]++
				if p.MatchCompleted.MatchId != "itest/m-1/r0" || len(p.MatchCompleted.Results) != 1 || string(r.Key) != p.MatchCompleted.MatchId {
					t.Fatalf("completed %+v key %s", p.MatchCompleted, r.Key)
				}
			}
		}
	}
	if kinds["started"] < 2 || kinds["joined"] != 1 || kinds["completed"] < 1 {
		t.Fatalf("events = %v", kinds)
	}
}
