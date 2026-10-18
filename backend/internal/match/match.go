// Package match runs each match as a single-goroutine actor that owns its
// engine.World. All interaction happens through bounded channels, so the world
// needs no locks and a slow client can never stall the tick loop (adr/0003).
package match

import (
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/engine"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/events"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/telemetry"
)

// Client is a player's handle into a match. Out is a latest-wins mailbox of
// capacity 1: the newest snapshot replaces an unread older one.
type Client struct {
	ID   engine.PlayerID
	Out  chan []byte
	Done chan struct{} // closed by the match when it removes this client
}

func newClient(id engine.PlayerID) *Client {
	return &Client{ID: id, Out: make(chan []byte, 1), Done: make(chan struct{})}
}

// offer delivers b without blocking. Only the match goroutine calls it, so after
// evicting a stale snapshot the second send cannot fail.
func (c *Client) offer(b []byte) {
	select {
	case c.Out <- b:
		telemetry.SnapshotQueued.Inc()
		return
	default:
	}
	select {
	case <-c.Out:
		telemetry.SnapshotReplace.Inc()
	default:
	}
	select {
	case c.Out <- b:
	default:
	}
}

type input struct {
	id engine.PlayerID
	in protocol.Input
}

// Match is a running actor. Create it with Manager.
type Match struct {
	ID     string
	TickHz int
	world  *engine.World // owned by run(); never touch from other goroutines

	join   chan *Client
	leave  chan engine.PlayerID
	inputs chan input
	done   chan struct{}

	tick atomic.Uint64 // published for status readers

	cfg        engine.Config
	roundTicks uint64 // 0 = endless
	ev         emitter
}

// InputQueue bounds pending inputs per match; excess is dropped, not buffered.
const InputQueue = 1024

func newMatch(id string, tickHz int, cfg engine.Config, roundTicks uint64, pub *events.Publisher, instance string) *Match {
	return &Match{
		ID: id, TickHz: tickHz, world: engine.New(cfg), cfg: cfg, roundTicks: roundTicks,
		join:   make(chan *Client),
		leave:  make(chan engine.PlayerID),
		inputs: make(chan input, InputQueue),
		done:   make(chan struct{}),
		ev:     emitter{pub: pub, instance: instance, matchID: id, tickHz: tickHz},
	}
}

// Config returns the immutable world configuration.
func (m *Match) Config() engine.Config { return m.world.Config() }

// Tick returns the latest completed tick.
func (m *Match) Tick() uint64 { return m.tick.Load() }

// Submit enqueues a client input without blocking. It reports false if the
// queue is full; the caller should not retry (a newer input will follow).
func (m *Match) Submit(id engine.PlayerID, in protocol.Input) bool {
	select {
	case m.inputs <- input{id, in}:
		return true
	default:
		telemetry.InputQueueFull.Inc()
		return false
	}
}

func (m *Match) run(ctx context.Context) {
	defer close(m.done)
	period := time.Second / time.Duration(m.TickHz)
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	clients := map[engine.PlayerID]*Client{}
	lastSeq := map[engine.PlayerID]uint32{}
	defer func() {
		for _, c := range clients {
			close(c.Done)
		}
	}()

	var (
		players = make([]engine.Player, 0, 32)
		items   = make([]engine.Item, 0, 16)
		last    = time.Now()
		round   = 0
	)
	m.ev.startRound(round, m.cfg.Seed)
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-m.join:
			clients[c.ID] = c
			m.world.AddPlayer(c.ID)
			m.ev.joined(c.ID, m.world.Tick())
		case id := <-m.leave:
			if c, ok := clients[id]; ok {
				close(c.Done)
				delete(clients, id)
				delete(lastSeq, id)
				m.world.RemovePlayer(id)
				m.ev.left(id, m.world.Tick(), "disconnect")
			}
		case msg := <-m.inputs:
			// Sequence numbers make inputs idempotent and drop reordered/duplicate frames.
			if _, ok := clients[msg.id]; !ok || msg.in.Seq <= lastSeq[msg.id] {
				telemetry.InputStale.Inc()
				continue
			}
			lastSeq[msg.id] = msg.in.Seq
			m.world.SetInput(msg.id, engine.Input{Dx: msg.in.Dx, Dy: msg.in.Dy})
			telemetry.InputApplied.Inc()
		case now := <-ticker.C:
			// time.Ticker drops ticks for slow receivers; count the gap.
			if gap := now.Sub(last); gap > period*3/2 {
				telemetry.TicksMissed.Add(float64(gap/period - 1))
			}
			last = now
			start := time.Now()
			telemetry.TickLag.Observe(start.Sub(now).Seconds())
			m.world.Step()
			m.tick.Store(m.world.Tick())
			players, items = m.broadcast(clients, lastSeq, players[:0], items[:0])
			m.ev.afterStep(players, m.world.Tick(), time.Since(start).Microseconds())
			if m.roundTicks > 0 && m.world.Tick() >= m.roundTicks {
				// Round over: publish the result, then a fresh world with the same players.
				m.ev.completed(players, m.world.Tick())
				round++
				cfg := m.cfg
				cfg.Seed += uint64(round) // #nosec G115 -- round is a small positive counter
				m.world = engine.New(cfg)
				for id := range clients {
					m.world.AddPlayer(id)
				}
				m.ev.startRound(round, cfg.Seed)
			}
			d := time.Since(start)
			telemetry.Ticks.Inc()
			telemetry.TickDuration.Observe(d.Seconds())
			if d > period {
				telemetry.TickOverruns.Inc()
			}
		}
	}
}

// broadcast encodes the shared world view once and wraps it per client with
// that client's ack. Returns the scratch slices for reuse.
func (m *Match) broadcast(clients map[engine.PlayerID]*Client, acks map[engine.PlayerID]uint32,
	players []engine.Player, items []engine.Item) ([]engine.Player, []engine.Item) {
	players = m.world.Players(players)
	items = m.world.Items(items)
	ps := make([]protocol.PlayerState, len(players))
	for i, p := range players {
		ps[i] = protocol.PlayerState{ID: uint32(p.ID), X: p.Pos.X, Y: p.Pos.Y, S: p.Score}
	}
	is := make([][2]int32, len(items))
	for i, it := range items {
		is[i] = [2]int32{it.Pos.X, it.Pos.Y}
	}
	pj, _ := json.Marshal(ps) // cannot fail for these types
	ij, _ := json.Marshal(is)

	// {"t":"snap","tick":N,"ack":A,"players":[...],"items":[...]}
	head := strconv.AppendUint([]byte(`{"t":"snap","tick":`), m.world.Tick(), 10)
	head = append(head, `,"ack":`...)
	tail := append(append(append(append([]byte(`,"players":`), pj...), `,"items":`...), ij...), '}')
	for id, c := range clients {
		b := make([]byte, 0, len(head)+10+len(tail))
		b = append(b, head...)
		b = strconv.AppendUint(b, uint64(acks[id]), 10)
		b = append(b, tail...)
		c.offer(b)
	}
	return players, items
}
