package match

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/engine"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/events"
)

// emitter turns authoritative match state changes into events. It runs on the
// match goroutine; every call is non-blocking (events.Publisher.Publish).
//
// The run ID (instance/match/round) is globally unique, so a server restart
// never reuses a match identity, and event IDs derived from it plus the tick
// are deterministic within a run.
type emitter struct {
	pub        *events.Publisher
	instance   string
	matchID    string
	tickHz     int
	run        string
	roundStart time.Time
	scores     map[engine.PlayerID]uint32
}

func (e *emitter) startRound(round int, seed uint64) {
	e.run = fmt.Sprintf("%s/%s/r%d", e.instance, e.matchID, round)
	e.roundStart = time.Now()
	e.scores = map[engine.PlayerID]uint32{}
	e.publish(events.TopicMatch, e.run, events.Durable, events.EventID("started", e.run), 0,
		&eventsv1.Envelope_MatchStarted{MatchStarted: &eventsv1.MatchStarted{MatchId: e.run, Seed: seed, TickHz: uint32(e.tickHz)}}) // #nosec G115 -- small positive config value
}

// at converts a tick to event time: round start + tick periods. Wall clock is
// read once per round, never per event, so events within a round are ordered
// by tick even if the host clock jumps.
func (e *emitter) at(tick uint64) time.Time {
	return e.roundStart.Add(time.Duration(tick) * time.Second / time.Duration(e.tickHz)) // #nosec G115 -- tick counts are far below int64 overflow
}

func (e *emitter) publish(topic, key string, c events.Class, id string, tick uint64, payload any) {
	if e.pub == nil {
		return
	}
	env := &eventsv1.Envelope{EventId: id, CorrelationId: e.run, EventTime: timestamppb.New(e.at(tick))}
	switch p := payload.(type) {
	case *eventsv1.Envelope_MatchStarted:
		env.Payload = p
	case *eventsv1.Envelope_MatchCompleted:
		env.Payload = p
	case *eventsv1.Envelope_PlayerScored:
		env.Payload = p
	case *eventsv1.Envelope_PlayerJoined:
		env.Payload = p
	case *eventsv1.Envelope_PlayerLeft:
		env.Payload = p
	case *eventsv1.Envelope_TickSample:
		env.Payload = p
	}
	e.pub.Publish(topic, key, env, c)
}

func pid(id engine.PlayerID) string { return strconv.FormatUint(uint64(id), 10) }

func (e *emitter) joined(id engine.PlayerID, tick uint64) {
	e.publish(events.TopicPlayer, pid(id), events.Durable, events.EventID("joined", e.run, pid(id), strconv.FormatUint(tick, 10)), tick,
		&eventsv1.Envelope_PlayerJoined{PlayerJoined: &eventsv1.PlayerJoined{MatchId: e.run, PlayerId: int64(id)}})
}

func (e *emitter) left(id engine.PlayerID, tick uint64, reason string) {
	e.publish(events.TopicPlayer, pid(id), events.Durable, events.EventID("left", e.run, pid(id), strconv.FormatUint(tick, 10)), tick,
		&eventsv1.Envelope_PlayerLeft{PlayerLeft: &eventsv1.PlayerLeft{MatchId: e.run, PlayerId: int64(id), Reason: reason}})
}

// afterStep emits PlayerScored for every changed score (the new total, so a
// duplicate or replay is harmless) and a best-effort tick sample once a second.
func (e *emitter) afterStep(players []engine.Player, tick uint64, tickMicros int64) {
	for _, p := range players {
		if p.Score != e.scores[p.ID] {
			e.scores[p.ID] = p.Score
			e.publish(events.TopicMatch, e.run, events.Durable,
				events.EventID("scored", e.run, pid(p.ID), strconv.FormatUint(tick, 10)), tick,
				&eventsv1.Envelope_PlayerScored{PlayerScored: &eventsv1.PlayerScored{MatchId: e.run, PlayerId: int64(p.ID), Score: p.Score, Tick: tick}})
		}
	}
	if tick%uint64(e.tickHz) == 0 { // #nosec G115 -- positive config value
		e.publish(events.TopicTelemetry, e.instance, events.BestEffort, events.EventID("tick", e.run, strconv.FormatUint(tick, 10)), tick,
			&eventsv1.Envelope_TickSample{TickSample: &eventsv1.TickSample{ServerId: e.instance, MatchId: e.run, Tick: tick,
				Players: uint32(len(players)), TickMicros: uint32(min(tickMicros, 1<<31))}}) // #nosec G115 -- clamped
	}
}

func (e *emitter) completed(players []engine.Player, tick uint64) {
	res := make([]*eventsv1.PlayerResult, len(players))
	for i, p := range players {
		res[i] = &eventsv1.PlayerResult{PlayerId: int64(p.ID), Score: p.Score}
	}
	e.publish(events.TopicMatch, e.run, events.Durable, events.EventID("completed", e.run), tick,
		&eventsv1.Envelope_MatchCompleted{MatchCompleted: &eventsv1.MatchCompleted{MatchId: e.run, FinalTick: tick, Results: res}})
}
