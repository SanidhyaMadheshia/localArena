package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

// 20 players race for 3 free seats: exactly 3 get in, count matches rows.
func TestLobbyLastSeats(t *testing.T) {
	ctx := ctxT(t)
	lobby, err := st.CreateLobby(ctx, newPlayer(t, 0), 4)
	if err != nil {
		t.Fatal(err)
	}
	players := make([]int64, 20)
	for i := range players {
		players[i] = newPlayer(t, 0)
	}
	got := tally(run(len(players), func(i int) error { return st.JoinLobby(ctx, lobby, players[i]) }))
	if got["ok"] != 3 || got[store.ErrLobbyFull.Error()] != 17 {
		t.Fatalf("results = %v", got)
	}
	members, _ := st.LobbyMembers(ctx, lobby)
	if n := scalar[int32](t, st.Primary, `SELECT member_count FROM lobbies WHERE id = $1`, lobby); int(n) != len(members) || n != 4 {
		t.Fatalf("member_count %d, rows %d", n, len(members))
	}
}

// One player joining 10 lobbies at once ends up in exactly one.
func TestDuplicateJoinAcrossLobbies(t *testing.T) {
	ctx := ctxT(t)
	player := newPlayer(t, 0)
	lobbies := make([]int64, 10)
	for i := range lobbies {
		var err error
		if lobbies[i], err = st.CreateLobby(ctx, newPlayer(t, 0), 10); err != nil {
			t.Fatal(err)
		}
	}
	got := tally(run(len(lobbies), func(i int) error { return st.JoinLobby(ctx, lobbies[i], player) }))
	if got["ok"] != 1 || got[store.ErrAlreadyInLobby.Error()] != 9 {
		t.Fatalf("results = %v", got)
	}
	if err := st.LeaveLobby(ctx, player); err != nil {
		t.Fatal(err)
	}
	if err := st.JoinLobby(ctx, lobbies[0], player); err != nil {
		t.Fatalf("rejoin after leave: %v", err)
	}
}

// Two transactions lock rows in opposite order; Postgres must abort one with 40P01.
func TestDeadlockIsDetected(t *testing.T) {
	ctx := ctxT(t)
	a, b := newPlayer(t, 100), newPlayer(t, 100)
	var locked sync.WaitGroup
	locked.Add(2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, order := range [][2]int64{{a, b}, {b, a}} {
		wg.Go(func() {
			errs[i] = pgx.BeginFunc(ctx, st.Primary, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `UPDATE profiles SET credits = credits + 1 WHERE player_id = $1`, order[0]); err != nil {
					return err
				}
				locked.Done()
				locked.Wait() // both hold their first lock
				_, err := tx.Exec(ctx, `UPDATE profiles SET credits = credits + 1 WHERE player_id = $1`, order[1])
				return err
			})
		})
	}
	wg.Wait()
	var pe *pgconn.PgError
	deadlocks := 0
	for _, err := range errs {
		if errors.As(err, &pe) && pe.Code == "40P01" {
			deadlocks++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if deadlocks != 1 {
		t.Fatalf("deadlocks = %d, want exactly 1 victim (errs %v)", deadlocks, errs)
	}
}

// Opposite-direction gifts: unordered locking deadlocks so often under
// contention that bounded retries do not save it; ordered locking never
// deadlocks. Credits are conserved either way (aborted transactions roll back).
func TestGiftLockOrdering(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := map[bool]string{false: "unordered", true: "ordered"}[ordered]
		t.Run(name, func(t *testing.T) {
			ctx := ctxT(t)
			a, b := newPlayer(t, 10_000), newPlayer(t, 10_000)
			before := testutil.ToFloat64(store.TxRetries.WithLabelValues("deadlock"))
			errs := run(20, func(i int) error {
				if i%2 == 0 {
					return st.Gift(ctx, a, b, 1, ordered)
				}
				return st.Gift(ctx, b, a, 1, ordered)
			})
			got := tally(errs)
			deadlocks := testutil.ToFloat64(store.TxRetries.WithLabelValues("deadlock")) - before
			total := scalar[int64](t, st.Primary, `SELECT sum(credits) FROM profiles WHERE player_id IN ($1, $2)`, a, b)
			t.Logf("%s: results %v, %v deadlock retries, total credits %d", name, got, deadlocks, total)
			if total != 20_000 {
				t.Fatalf("credits not conserved: %d", total)
			}
			if ordered && (deadlocks != 0 || got["ok"] != 20) {
				t.Fatalf("ordered locking: %v deadlocks, results %v", deadlocks, got)
			}
			if !ordered && deadlocks == 0 {
				t.Fatal("expected unordered locking to deadlock under contention")
			}
		})
	}
}

// With every connection busy, Acquire waits only as long as the caller's
// context allows, and the wait is visible in pool stats.
func TestPoolExhaustionTimesOut(t *testing.T) {
	ctx := ctxT(t)
	p, err := store.NewPool(ctx, primaryURL, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { _, _ = p.Exec(ctx, `SELECT pg_sleep(1)`) })
	}
	time.Sleep(200 * time.Millisecond) // let both sleepers take a connection
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = p.Exec(short, `SELECT 1`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded while pool is exhausted, got %v", err)
	}
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("waited %v, should be bounded by the 200 ms context", waited)
	}
	wg.Wait()
	if s := p.Stat(); s.EmptyAcquireCount() == 0 {
		t.Fatalf("expected empty acquires in stats: %+v", s)
	}
}

// PgBouncer (default_pool_size=10, query_wait_timeout=5s): 12 clients each
// hold a server connection for 6 s, so 2 wait past the timeout and are rejected.
func TestPgBouncerQueueWaitTimeout(t *testing.T) {
	if bouncerURL == "" || testing.Short() {
		t.Skip("LOCALARENA_TEST_PGBOUNCER_URL not set or -short")
	}
	ctx := ctxT(t)
	p, err := pgxpool.New(ctx, bouncerURL+"?pool_max_conns=12")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	got := tally(run(12, func(int) error {
		_, err := p.Exec(ctx, `SELECT pg_sleep(6)`)
		return err
	}))
	t.Logf("pgbouncer results: %v", got)
	if got["ok"] != 10 {
		t.Fatalf("want 10 served, got %v", got)
	}
}
