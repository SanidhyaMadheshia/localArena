package store_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/eventid"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

func TestRewardSagaSteps(t *testing.T) {
	ctx := ctxT(t)
	if _, err := st.Primary.Exec(ctx, `UPDATE reward_pool SET remaining = 1000 WHERE season = 's1'`); err != nil {
		t.Fatal(err)
	}
	player := newPlayer(t, 0)
	r := store.Reward{ID: eventid.New("rw", fmt.Sprint(uniq.Add(1))), Season: "s1", MatchID: "m", PlayerID: player, Credits: 300}

	// Happy path, then every step replayed (redelivery): credits applied once.
	for range 3 {
		if s, err := st.Reserve(ctx, r); err != nil || (s != store.SagaReserved && s != store.SagaGranted) {
			t.Fatalf("reserve: %s %v", s, err)
		}
		if err := st.Grant(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if c := scalar[int64](t, st.Primary, `SELECT credits FROM profiles WHERE player_id = $1`, player); c != 300 {
		t.Fatalf("credits = %d, want 300 (exactly once)", c)
	}
	if n, _ := st.PoolRemaining(ctx, "s1"); n != 700 {
		t.Fatalf("pool = %d", n)
	}
	if n := scalar[int64](t, st.Primary, `SELECT count(*) FROM outbox WHERE id = $1::uuid`, r.ID); n != 1 {
		t.Fatalf("RewardGranted outbox rows = %d", n)
	}

	// Player gone: grant fails permanently, compensation returns the reservation (once).
	gone := store.Reward{ID: eventid.New("rw", fmt.Sprint(uniq.Add(1))), Season: "s1", MatchID: "m", PlayerID: 999_999_999, Credits: 200}
	if s, _ := st.Reserve(ctx, gone); s != store.SagaReserved {
		t.Fatalf("reserve gone: %s", s)
	}
	if err := st.Grant(ctx, gone); !errors.Is(err, store.ErrPlayerGone) {
		t.Fatalf("grant gone: %v", err)
	}
	for range 2 {
		if err := st.Compensate(ctx, gone.ID, "player does not exist"); err != nil {
			t.Fatal(err)
		}
	}
	if s, reason, _ := st.SagaStatus(ctx, gone.ID); s != store.SagaCompensated || reason == "" {
		t.Fatalf("state %s %q", s, reason)
	}
	if n, _ := st.PoolRemaining(ctx, "s1"); n != 700 {
		t.Fatalf("pool after compensation = %d, want 700", n)
	}
	if err := st.Grant(ctx, gone); err != nil {
		t.Fatalf("grant after compensation must be a no-op: %v", err)
	}

	// Pool too small: rejected, nothing reserved.
	big := store.Reward{ID: eventid.New("rw", fmt.Sprint(uniq.Add(1))), Season: "s1", MatchID: "m", PlayerID: player, Credits: 5000}
	if s, _ := st.Reserve(ctx, big); s != store.SagaRejected {
		t.Fatalf("over-budget reserve: %s", s)
	}
	if n, _ := st.PoolRemaining(ctx, "s1"); n != 700 {
		t.Fatalf("pool after rejection = %d", n)
	}
}
