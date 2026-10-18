// Package shard is a two-shard (N-shard) routing prototype for player data.
//
// Routing uses Jump Consistent Hash (Lamping & Veach, 2014): no ring or
// lookup table, perfectly balanced, and growing from N to N+1 shards moves
// only ~1/(N+1) of keys. Player IDs are global (assigned outside the shards),
// so the shard tables insert explicit IDs.
//
// What sharding costs (see adr/0006): no cross-shard transactions, no
// cross-shard UNIQUE (username uniqueness needs a separate directory), and
// queries without the shard key become scatter-gather.
package shard

import (
	"cmp"
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// Jump maps key to a bucket in [0, buckets).
func Jump(key uint64, buckets int) int {
	var b, j int64 = -1, 0
	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// mix spreads sequential IDs before hashing (SplitMix64 finalizer).
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Router routes player-keyed queries to shards.
type Router struct{ shards []*pgxpool.Pool }

// New returns a router over the given shard pools (order defines shard numbers).
func New(shards ...*pgxpool.Pool) *Router { return &Router{shards: shards} }

// ShardOf returns the shard index for a player.
func (r *Router) ShardOf(playerID int64) int { return Jump(mix(uint64(playerID)), len(r.shards)) }

// For returns the pool owning playerID.
func (r *Router) For(playerID int64) *pgxpool.Pool { return r.shards[r.ShardOf(playerID)] }

// CreatePlayer writes the player and profile on its owning shard, atomically
// (single-shard transaction).
func (r *Router) CreatePlayer(ctx context.Context, id int64, username string, credits int64) error {
	return pgx.BeginFunc(ctx, r.For(id), func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO players (id, username) OVERRIDING SYSTEM VALUE VALUES ($1, $2)`, id, username); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO profiles (player_id, display_name, credits) VALUES ($1, $2, $3)`, id, username, credits)
		return err
	})
}

// Credits reads one player's credits from its shard (single-shard query).
func (r *Router) Credits(ctx context.Context, id int64) (int64, error) {
	var c int64
	err := r.For(id).QueryRow(ctx, `SELECT credits FROM profiles WHERE player_id = $1`, id).Scan(&c)
	return c, err
}

// Ranked is a leaderboard row.
type Ranked struct {
	PlayerID int64
	Credits  int64
}

// TopCredits is scatter-gather: ask every shard for its local top n in
// parallel, then merge. Correct because the global top n is a subset of the
// union of local top n; latency is that of the slowest shard.
func (r *Router) TopCredits(ctx context.Context, n int) ([]Ranked, error) {
	parts := make([][]Ranked, len(r.shards))
	g, ctx := errgroup.WithContext(ctx)
	for i, p := range r.shards {
		g.Go(func() error {
			rows, err := p.Query(ctx, `SELECT player_id, credits FROM profiles ORDER BY credits DESC, player_id LIMIT $1`, n)
			if err != nil {
				return err
			}
			parts[i], err = pgx.CollectRows(rows, pgx.RowToStructByPos[Ranked])
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	all := slices.Concat(parts...)
	slices.SortFunc(all, func(a, b Ranked) int {
		return cmp.Or(cmp.Compare(b.Credits, a.Credits), cmp.Compare(a.PlayerID, b.PlayerID))
	})
	return all[:min(n, len(all))], nil
}
