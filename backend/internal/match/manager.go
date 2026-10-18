package match

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/engine"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/events"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/telemetry"
)

// Errors returned by Join.
var (
	ErrClosed         = errors.New("match: manager closed")
	ErrAlreadyPlaying = errors.New("match: player already connected")
)

// GuestIDBase is the first ID for sessions without an account. Account IDs
// (PostgreSQL players.id) are below it, so downstream consumers can tell
// guests apart (e.g. guests earn no credits).
const GuestIDBase engine.PlayerID = 1_000_000_000

// Options configures a Manager.
type Options struct {
	TickHz   int    // fixed simulation rate, default 20
	Capacity int    // players per match, default 20
	Seed     uint64 // base seed; each match derives its own

	Events     *events.Publisher // nil disables event publishing
	RoundTicks uint64            // ticks per round; 0 = endless (tests)
	InstanceID string            // unique per process start; default random

	// ModeFor returns the game mode for a round (feature flag "game-mode");
	// nil means always "classic". Called on the match goroutine at round start.
	ModeFor func(roundID string) string
}

// Manager assigns players to matches, creating matches on demand and stopping
// empty ones. Slot accounting is done under mu so two joins can never overfill
// a match.
type Manager struct {
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc
	nextID atomic.Uint32

	mu      sync.Mutex
	active  map[engine.PlayerID]bool
	matches []*slot
	nextM   int
	closed  bool
	version uint64
	changed chan struct{} // closed and replaced on every membership change
	wg      sync.WaitGroup
}

type slot struct {
	m      *Match
	n      int
	cancel context.CancelFunc
}

// NewManager returns a Manager; call Close to stop all matches.
func NewManager(opts Options) *Manager {
	if opts.TickHz <= 0 {
		opts.TickHz = 20
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 20
	}
	if opts.InstanceID == "" {
		var b [6]byte
		_, _ = rand.Read(b[:])
		opts.InstanceID = hex.EncodeToString(b[:])
	}
	ctx, cancel := context.WithCancel(context.Background())
	mg := &Manager{opts: opts, ctx: ctx, cancel: cancel, changed: make(chan struct{}), active: map[engine.PlayerID]bool{}}
	mg.nextID.Store(uint32(GuestIDBase) - 1)
	return mg
}

// Join places a player into the first match with free capacity. account is
// the PostgreSQL player ID, or 0 for a guest (a guest ID is assigned).
func (mg *Manager) Join(account ...engine.PlayerID) (*Match, *Client, error) {
	id := engine.PlayerID(0)
	if len(account) > 0 && account[0] > 0 && account[0] < GuestIDBase {
		id = account[0]
	}
	mg.mu.Lock()
	if mg.closed {
		mg.mu.Unlock()
		return nil, nil, ErrClosed
	}
	if id == 0 {
		id = engine.PlayerID(mg.nextID.Add(1))
	}
	if mg.active[id] {
		mg.mu.Unlock()
		return nil, nil, ErrAlreadyPlaying
	}
	mg.active[id] = true
	var s *slot
	for _, cand := range mg.matches {
		if cand.n < mg.opts.Capacity {
			s = cand
			break
		}
	}
	if s == nil {
		mg.nextM++
		ctx, cancel := context.WithCancel(mg.ctx)
		m := newMatch(fmt.Sprintf("m-%d", mg.nextM), mg.opts.TickHz, engine.DefaultConfig(mg.opts.Seed+uint64(mg.nextM)), // #nosec G115 -- nextM is a positive counter
			mg.opts.RoundTicks, mg.opts.Events, mg.opts.InstanceID)
		m.modeFor = mg.opts.ModeFor
		s = &slot{m: m, cancel: cancel}
		mg.matches = append(mg.matches, s)
		mg.wg.Go(func() { m.run(ctx) })
		telemetry.Matches.Inc()
	}
	s.n++
	mg.bumpLocked()
	mg.mu.Unlock()

	c := newClient(id)
	select {
	case s.m.join <- c:
	case <-s.m.done:
		mg.release(s.m, id)
		return nil, nil, ErrClosed
	}
	telemetry.Players.Inc()
	return s.m, c, nil
}

// Leave removes a client. It is safe to call after the match has stopped.
func (mg *Manager) Leave(m *Match, c *Client) {
	select {
	case m.leave <- c.ID:
	case <-m.done:
	}
	telemetry.Players.Dec()
	mg.release(m, c.ID)
}

// release frees a slot and stops the match when it becomes empty.
func (mg *Manager) release(m *Match, id engine.PlayerID) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	delete(mg.active, id)
	for i, s := range mg.matches {
		if s.m != m {
			continue
		}
		s.n--
		if s.n == 0 {
			s.cancel()
			mg.matches = append(mg.matches[:i], mg.matches[i+1:]...)
			telemetry.Matches.Dec()
		}
		mg.bumpLocked()
		return
	}
}

func (mg *Manager) bumpLocked() {
	mg.version++
	close(mg.changed)
	mg.changed = make(chan struct{})
}

// MatchStatus is a point-in-time view of one match.
type MatchStatus struct {
	ID      string `json:"id"`
	Players int    `json:"players"`
	Tick    uint64 `json:"tick"`
}

// Status is a point-in-time view of the server.
type Status struct {
	Version uint64        `json:"version"`
	Players int           `json:"players"`
	Matches []MatchStatus `json:"matches"`
}

// Status returns the current status and a channel closed on the next change.
func (mg *Manager) Status() (Status, <-chan struct{}) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	st := Status{Version: mg.version, Matches: make([]MatchStatus, 0, len(mg.matches))}
	for _, s := range mg.matches {
		st.Players += s.n
		st.Matches = append(st.Matches, MatchStatus{ID: s.m.ID, Players: s.n, Tick: s.m.Tick()})
	}
	return st, mg.changed
}

// Close stops all matches and waits for their goroutines to exit.
func (mg *Manager) Close() {
	mg.mu.Lock()
	mg.closed = true
	mg.mu.Unlock()
	mg.cancel()
	mg.wg.Wait()
}
