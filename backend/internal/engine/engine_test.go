package engine

import (
	"math/rand/v2"
	"testing"
)

// script drives a world with a reproducible pseudo-random input sequence.
func script(seed uint64, players, ticks int) uint64 {
	w := New(DefaultConfig(seed))
	for i := 1; i <= players; i++ {
		w.AddPlayer(PlayerID(i))
	}
	in := rand.New(rand.NewPCG(seed, 42))
	for range ticks {
		for i := 1; i <= players; i++ {
			w.SetInput(PlayerID(i), Input{Dx: int8(in.IntN(3) - 1), Dy: int8(in.IntN(3) - 1)})
		}
		w.Step()
	}
	return w.Hash()
}

func TestDeterministicReplay(t *testing.T) {
	a, b := script(7, 20, 2000), script(7, 20, 2000)
	if a != b {
		t.Fatalf("same seed and inputs diverged: %x != %x", a, b)
	}
	if c := script(8, 20, 2000); c == a {
		t.Fatalf("different seeds produced identical state %x", c)
	}
}

func TestPlayersIteratedInIDOrder(t *testing.T) {
	w := New(DefaultConfig(1))
	for _, id := range []PlayerID{3, 1, 2} {
		w.AddPlayer(id)
	}
	ps := w.Players(nil)
	for i, want := range []PlayerID{1, 2, 3} {
		if ps[i].ID != want {
			t.Fatalf("order = %v, want ascending IDs", ps)
		}
	}
}

func TestBoundsClamp(t *testing.T) {
	w := New(DefaultConfig(1))
	w.AddPlayer(1)
	w.SetInput(1, Input{Dx: -1, Dy: 1})
	for range 500 {
		w.Step()
	}
	p := w.Players(nil)[0]
	r := w.cfg.PlayerRadius
	if p.Pos.X != r || p.Pos.Y != w.cfg.Height-r {
		t.Fatalf("expected clamp to (%d,%d), got %+v", r, w.cfg.Height-r, p.Pos)
	}
}

func TestInputIsClamped(t *testing.T) {
	w := New(DefaultConfig(1))
	w.AddPlayer(1)
	w.players[1].Pos = Vec{500, 500}
	w.SetInput(1, Input{Dx: 100, Dy: -100}) // a cheating client asking for speed 100
	w.Step()
	p := w.Players(nil)[0].Pos
	if p.X != 500+w.cfg.Speed || p.Y != 500-w.cfg.Speed {
		t.Fatalf("moved to %+v, max step is %d", p, w.cfg.Speed)
	}
}

func TestPickupScoresAndRespawns(t *testing.T) {
	w := New(DefaultConfig(3))
	w.AddPlayer(1)
	w.players[1].Pos = w.items[0].Pos // place the player on an item
	old := w.items[0].Pos
	w.Step()
	if s := w.players[1].Score; s != 1 {
		t.Fatalf("score = %d, want 1", s)
	}
	if w.items[0].Pos == old {
		t.Fatalf("item did not respawn")
	}
}

func TestPickupTieGoesToLowestID(t *testing.T) {
	w := New(DefaultConfig(3))
	w.AddPlayer(2)
	w.AddPlayer(1)
	w.players[1].Pos, w.players[2].Pos = w.items[0].Pos, w.items[0].Pos
	w.Step()
	if w.players[1].Score != 1 || w.players[2].Score != 0 {
		t.Fatalf("tie-break wrong: p1=%d p2=%d", w.players[1].Score, w.players[2].Score)
	}
}

func TestRemovePlayer(t *testing.T) {
	w := New(DefaultConfig(1))
	w.AddPlayer(1)
	w.AddPlayer(2)
	w.RemovePlayer(1)
	w.RemovePlayer(99) // unknown is a no-op
	w.SetInput(1, Input{Dx: 1})
	w.Step()
	if w.NumPlayers() != 1 || w.order[0] != 2 {
		t.Fatalf("remove failed: %v", w.order)
	}
}

func BenchmarkStep20Players(b *testing.B) {
	w := New(DefaultConfig(1))
	for i := 1; i <= 20; i++ {
		w.AddPlayer(PlayerID(i))
		w.SetInput(PlayerID(i), Input{Dx: 1, Dy: -1})
	}
	b.ReportAllocs()
	for b.Loop() {
		w.Step()
	}
}
