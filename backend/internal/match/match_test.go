package match

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func recvSnap(t *testing.T, c *Client) protocol.Snapshot {
	t.Helper()
	select {
	case b := <-c.Out:
		var s protocol.Snapshot
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatalf("bad snapshot %q: %v", b, err)
		}
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot")
	}
	return protocol.Snapshot{}
}

func TestSnapshotsAckInputs(t *testing.T) {
	mg := NewManager(Options{TickHz: 100})
	defer mg.Close()
	m, c, err := mg.Join()
	if err != nil {
		t.Fatal(err)
	}
	m.Submit(c.ID, protocol.Input{T: "input", Seq: 5, Dx: 1})
	m.Submit(c.ID, protocol.Input{T: "input", Seq: 3, Dx: -1}) // reordered: must be ignored
	var s protocol.Snapshot
	for s.Ack != 5 {
		s = recvSnap(t, c)
	}
	for range 3 {
		if s = recvSnap(t, c); s.Ack != 5 {
			t.Fatalf("ack regressed to %d", s.Ack)
		}
	}
	mg.Leave(m, c)
}

func TestSlowClientMailboxIsBounded(t *testing.T) {
	mg := NewManager(Options{TickHz: 200})
	defer mg.Close()
	m, c, _ := mg.Join()
	time.Sleep(100 * time.Millisecond) // many ticks while nobody reads
	if n := len(c.Out); n != 1 {
		t.Fatalf("mailbox len = %d, want 1", n)
	}
	if s := recvSnap(t, c); s.Tick < 5 {
		t.Fatalf("expected a recent snapshot, got tick %d", s.Tick)
	}
	mg.Leave(m, c)
}

func TestCapacityCreatesNewMatch(t *testing.T) {
	mg := NewManager(Options{Capacity: 2})
	defer mg.Close()
	type pair struct {
		m *Match
		c *Client
	}
	var ps []pair
	for range 5 {
		m, c, err := mg.Join()
		if err != nil {
			t.Fatal(err)
		}
		ps = append(ps, pair{m, c})
	}
	if st, _ := mg.Status(); len(st.Matches) != 3 || st.Players != 5 {
		t.Fatalf("status = %+v, want 3 matches / 5 players", st)
	}
	for _, p := range ps {
		mg.Leave(p.m, p.c)
	}
	if st, _ := mg.Status(); len(st.Matches) != 0 {
		t.Fatalf("empty matches not stopped: %+v", st)
	}
}

func TestConcurrentJoinNeverOverfills(t *testing.T) {
	mg := NewManager(Options{Capacity: 20})
	defer mg.Close()
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			if _, _, err := mg.Join(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	st, _ := mg.Status()
	if len(st.Matches) != 10 {
		t.Fatalf("got %d matches, want 10", len(st.Matches))
	}
	for _, ms := range st.Matches {
		if ms.Players != 20 {
			t.Fatalf("match %s has %d players", ms.ID, ms.Players)
		}
	}
}

func TestStatusChangeNotifies(t *testing.T) {
	mg := NewManager(Options{})
	defer mg.Close()
	_, ch := mg.Status()
	m, c, _ := mg.Join()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("no change notification")
	}
	mg.Leave(m, c)
}

func TestCloseReleasesClients(t *testing.T) {
	mg := NewManager(Options{})
	m, c, _ := mg.Join()
	mg.Close()
	select {
	case <-c.Done:
	case <-time.After(time.Second):
		t.Fatal("client not released on close")
	}
	mg.Leave(m, c) // must not block after close
	if _, _, err := mg.Join(); err != ErrClosed {
		t.Fatalf("join after close: %v", err)
	}
}

func TestModeFlagPerRound(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	mg := NewManager(Options{TickHz: 200, RoundTicks: 10, ModeFor: func(round string) string {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, round)
		if len(seen)%2 == 0 {
			return "frenzy"
		}
		return "classic"
	}})
	defer mg.Close()
	m, c, _ := mg.Join()
	if m.Mode() != "classic" {
		t.Fatalf("round 0: mode %s", m.Mode())
	}
	time.Sleep(80 * time.Millisecond) // ~1.6 rounds at 200 Hz
	mg.Leave(m, c)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 2 || seen[0] != "m-1/r0" || seen[1] != "m-1/r1" {
		t.Fatalf("flag evaluated with %v", seen)
	}
	if ApplyMode(m.cfg, "frenzy").Items != 30 || ApplyMode(m.cfg, "unknown").Items != 10 {
		t.Fatal("ApplyMode")
	}
}

func TestDrainRefusesJoinsAndWaitsForPlayers(t *testing.T) {
	mg := NewManager(Options{})
	defer mg.Close()
	m, c, _ := mg.Join()
	done := make(chan int)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		done <- mg.Drain(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	if !mg.Draining() {
		t.Fatal("not draining")
	}
	if _, _, err := mg.Join(); err != ErrDraining {
		t.Fatalf("join while draining: %v", err)
	}
	select {
	case <-done:
		t.Fatal("drain returned while a player was connected")
	case <-time.After(300 * time.Millisecond):
	}
	mg.Leave(m, c) // the player's match ends
	if left := <-done; left != 0 {
		t.Fatalf("left = %d", left)
	}
}
