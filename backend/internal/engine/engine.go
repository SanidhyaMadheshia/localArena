// Package engine is the deterministic, authoritative LocalArena simulation.
//
// Determinism rules (see adr/0001):
//   - integer world coordinates only (no floats in state),
//   - players are always processed in ascending ID order,
//   - randomness comes only from a seeded PCG owned by the World,
//   - no wall-clock reads; time advances only via Step (one fixed tick).
//
// The engine is not goroutine-safe: it is owned by exactly one match actor.
package engine

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
	"slices"
)

// Config describes world rules. All distances are in world units.
type Config struct {
	Width, Height int32
	Speed         int32 // units moved per tick on each axis
	PlayerRadius  int32
	ItemRadius    int32
	Items         int // number of collectible items kept alive
	Seed          uint64
}

// DefaultConfig is a 1000x1000 arena tuned for 20 Hz ticks.
func DefaultConfig(seed uint64) Config {
	return Config{Width: 1000, Height: 1000, Speed: 10, PlayerRadius: 12, ItemRadius: 8, Items: 10, Seed: seed}
}

// PlayerID is assigned by the server, never by the client.
type PlayerID uint32

// Input is the only thing a client influences: a desired direction.
// Dx/Dy are clamped to [-1, 1]; anything else is ignored.
type Input struct {
	Dx, Dy int8
}

func (in Input) clamp() Input {
	c := func(v int8) int8 { return max(-1, min(1, v)) }
	return Input{Dx: c(in.Dx), Dy: c(in.Dy)}
}

// Vec is an integer position.
type Vec struct{ X, Y int32 }

// Player is authoritative per-player state.
type Player struct {
	ID    PlayerID
	Pos   Vec
	Score uint32
	input Input // held input applied every tick until replaced
}

// Item is a collectible worth one point.
type Item struct{ Pos Vec }

// World is the full simulation state.
type World struct {
	cfg     Config
	tick    uint64
	rng     *rand.Rand
	players map[PlayerID]*Player
	order   []PlayerID // sorted IDs, the deterministic iteration order
	items   []Item
}

// New builds a world and spawns the initial items from the seed.
func New(cfg Config) *World {
	w := &World{
		cfg:     cfg,
		rng:     rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		players: make(map[PlayerID]*Player),
	}
	w.items = make([]Item, cfg.Items)
	for i := range w.items {
		w.items[i].Pos = w.randomPos(cfg.ItemRadius)
	}
	return w
}

func (w *World) randomPos(margin int32) Vec {
	return Vec{
		X: margin + w.rng.Int32N(w.cfg.Width-2*margin),
		Y: margin + w.rng.Int32N(w.cfg.Height-2*margin),
	}
}

// Config returns the world rules.
func (w *World) Config() Config { return w.cfg }

// Tick returns the number of completed ticks.
func (w *World) Tick() uint64 { return w.tick }

// AddPlayer spawns a player at a seeded random position. It is a no-op if the ID exists.
func (w *World) AddPlayer(id PlayerID) {
	if _, ok := w.players[id]; ok {
		return
	}
	w.players[id] = &Player{ID: id, Pos: w.randomPos(w.cfg.PlayerRadius)}
	i, _ := slices.BinarySearch(w.order, id)
	w.order = slices.Insert(w.order, i, id)
}

// RemovePlayer deletes a player and its state.
func (w *World) RemovePlayer(id PlayerID) {
	if _, ok := w.players[id]; !ok {
		return
	}
	delete(w.players, id)
	if i, ok := slices.BinarySearch(w.order, id); ok {
		w.order = slices.Delete(w.order, i, i+1)
	}
}

// SetInput replaces a player's held input. Unknown players are ignored.
func (w *World) SetInput(id PlayerID, in Input) {
	if p, ok := w.players[id]; ok {
		p.input = in.clamp()
	}
}

// NumPlayers returns the current player count.
func (w *World) NumPlayers() int { return len(w.order) }

// Step advances the simulation by exactly one fixed tick.
func (w *World) Step() {
	r := w.cfg.PlayerRadius
	for _, id := range w.order {
		p := w.players[id]
		p.Pos.X = clamp(p.Pos.X+int32(p.input.Dx)*w.cfg.Speed, r, w.cfg.Width-r)
		p.Pos.Y = clamp(p.Pos.Y+int32(p.input.Dy)*w.cfg.Speed, r, w.cfg.Height-r)
	}
	// Pickups: lowest player ID wins ties, which is deterministic and documented.
	reach := int64(w.cfg.PlayerRadius + w.cfg.ItemRadius)
	for i := range w.items {
		for _, id := range w.order {
			p := w.players[id]
			dx, dy := int64(p.Pos.X-w.items[i].Pos.X), int64(p.Pos.Y-w.items[i].Pos.Y)
			if dx*dx+dy*dy <= reach*reach {
				p.Score++
				w.items[i].Pos = w.randomPos(w.cfg.ItemRadius)
				break
			}
		}
	}
	w.tick++
}

func clamp(v, lo, hi int32) int32 { return max(lo, min(hi, v)) }

// Players appends players in ID order to dst (allocation-free when dst has capacity).
func (w *World) Players(dst []Player) []Player {
	for _, id := range w.order {
		dst = append(dst, *w.players[id])
	}
	return dst
}

// Items appends the current items to dst.
func (w *World) Items(dst []Item) []Item { return append(dst, w.items...) }

// Hash is an FNV-1a digest of the full observable state, used by determinism tests and replays.
func (w *World) Hash() uint64 {
	h := fnv.New64a()
	var b [8]byte
	put := func(v uint64) { binary.LittleEndian.PutUint64(b[:], v); _, _ = h.Write(b[:]) }
	put(w.tick)
	for _, id := range w.order {
		p := w.players[id]
		put(uint64(p.ID))
		put(uint64(uint32(p.Pos.X))<<32 | uint64(uint32(p.Pos.Y))) // #nosec G115 -- bit reinterpretation of int32 coordinates for hashing
		put(uint64(p.Score))
	}
	for _, it := range w.items {
		put(uint64(uint32(it.Pos.X))<<32 | uint64(uint32(it.Pos.Y))) // #nosec G115 -- bit reinterpretation of int32 coordinates for hashing
	}
	return h.Sum64()
}
