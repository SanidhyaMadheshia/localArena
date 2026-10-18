package shard

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

func TestJumpBalanceAndMinimalMovement(t *testing.T) {
	const keys = 100_000
	counts := make([]int, 2)
	moved := 0
	for k := range uint64(keys) {
		a := Jump(mix(k), 2)
		counts[a]++
		if b := Jump(mix(k), 3); b != a {
			moved++
			if b != 2 {
				t.Fatalf("key %d moved between existing shards %d->%d", k, a, b)
			}
		}
	}
	for i, c := range counts {
		if math.Abs(float64(c)/keys-0.5) > 0.01 {
			t.Fatalf("shard %d has %d keys, want ~50%%", i, c)
		}
	}
	// Growing 2 -> 3 should move ~1/3 of keys, all to the new shard.
	if f := float64(moved) / keys; math.Abs(f-1.0/3) > 0.01 {
		t.Fatalf("moved %.3f of keys, want ~0.333", f)
	}
	t.Logf("balance %v, moved on 2->3: %.1f%%", counts, 100*float64(moved)/keys)
}

// Integration: LOCALARENA_TEST_SHARD_URLS="postgres://...:5440/localarena,postgres://...:5441/localarena"
// (docker compose --profile shards up -d pg-shard0 pg-shard1).
func TestRouterAgainstTwoShards(t *testing.T) {
	urls := strings.Split(os.Getenv("LOCALARENA_TEST_SHARD_URLS"), ",")
	if len(urls) != 2 {
		t.Skip("LOCALARENA_TEST_SHARD_URLS not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pools := make([]*pgxpool.Pool, 2)
	for i, u := range urls {
		if err := migrations.Up(ctx, u); err != nil {
			t.Fatal(err)
		}
		p, err := pgxpool.New(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if _, err := p.Exec(ctx, `TRUNCATE players CASCADE`); err != nil {
			t.Fatal(err)
		}
		pools[i] = p
	}
	r := New(pools...)
	for id := int64(1); id <= 100; id++ {
		if err := r.CreatePlayer(ctx, id, fmt.Sprintf("player_%d", id), id*10); err != nil {
			t.Fatal(err)
		}
	}
	for i, p := range pools {
		var n int
		_ = p.QueryRow(ctx, `SELECT count(*) FROM players`).Scan(&n)
		t.Logf("shard %d holds %d players", i, n)
		if n < 30 || n > 70 {
			t.Fatalf("shard %d holds %d of 100 players", i, n)
		}
	}
	if c, err := r.Credits(ctx, 42); err != nil || c != 420 {
		t.Fatalf("routed read: %d %v", c, err)
	}
	top, err := r.TopCredits(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{100, 99, 98, 97, 96} {
		if top[i].PlayerID != want {
			t.Fatalf("scatter-gather top = %v", top)
		}
	}
	// The cost of sharding: the same username is accepted on both shards.
	_ = r.CreatePlayer(ctx, 1001, "dup_name", 0)
	other := int64(1002)
	for r.ShardOf(other) == r.ShardOf(1001) {
		other++
	}
	if err := r.CreatePlayer(ctx, other, "dup_name", 0); err != nil {
		t.Fatalf("expected cross-shard duplicate username to be accepted (no global UNIQUE): %v", err)
	}
}
