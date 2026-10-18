package store_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

// TestExperimentQueryPlans prints EXPLAIN ANALYZE output and latency for
// Experiment 002: missing index (schema v3) vs indexed (v4), N+1 vs join, and
// partition pruning. Run with LOCALARENA_EXPERIMENT=1.
func TestExperimentQueryPlans(t *testing.T) {
	if os.Getenv("LOCALARENA_EXPERIMENT") == "" {
		t.Skip("set LOCALARENA_EXPERIMENT=1")
	}
	ctx := ctxT(t)
	dsn := recreateDB(ctx, primaryURL, "localarena_explain")
	prov, db, err := migrations.Provider(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := prov.UpTo(ctx, 3); err != nil { // schema without match_players(player_id) index
		t.Fatal(err)
	}
	p, err := store.NewPool(ctx, dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := store.New(p, nil)

	// 5,000 players; 60,000 matches of 10 players over Sep-Dec 2026 (600k rows).
	seed := []string{
		`INSERT INTO players (username) SELECT 'user' || g FROM generate_series(1, 5000) g`,
		`INSERT INTO profiles (player_id, display_name, credits) SELECT id, username, 1000 FROM players`,
		`INSERT INTO matches (started_at, ended_at)
		 SELECT ts, ts + interval '5 min' FROM (
		   SELECT timestamptz '2026-09-01' + (g * interval '173 seconds') AS ts FROM generate_series(1, 60000) g) x`,
		`INSERT INTO match_players (match_id, started_at, player_id, score)
		 SELECT m.id, m.started_at, ((m.id * 7 + k * 499) % 5000) + 1, (m.id + k) % 50
		 FROM matches m CROSS JOIN generate_series(0, 9) k`,
		`ANALYZE`,
	}
	for _, q := range seed {
		if _, err := p.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	explain := func(title, sql string, args ...any) {
		rows, err := p.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, SUMMARY ON) "+sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for rows.Next() {
			var l string
			_ = rows.Scan(&l)
			lines = append(lines, l)
		}
		fmt.Printf("\n#### %s\n```text\n%s\n```\n", title, strings.Join(lines, "\n"))
	}
	lat := func(name string, n int, fn func() error) {
		d := make([]time.Duration, n)
		for i := range d {
			st := time.Now()
			if err := fn(); err != nil {
				t.Fatal(err)
			}
			d[i] = time.Since(st)
		}
		slices.Sort(d)
		fmt.Printf("| %s | %d | %.2f | %.2f | %.2f |\n", name, n, ms(d[n/2]), ms(d[n*95/100]), ms(d[n*99/100]))
	}
	const player = int64(1234)
	hist := func(fn func(context.Context, *pgxpool.Pool, int64, int) ([]store.MatchSummary, error)) func() error {
		return func() error { _, err := fn(ctx, p, player, 20); return err }
	}

	raw := func(sql string) func() error {
		return func() error {
			rows, err := p.Query(ctx, sql, player, 20)
			if err != nil {
				return err
			}
			rows.Close()
			return rows.Err()
		}
	}
	explain("History (limit-first), schema v3: no match_players(player_id) index", store.HistorySQL, player, 20)
	fmt.Printf("\n| query | schema | runs | p50 ms | p95 ms | p99 ms |\n|---|---|---:|---:|---:|---:|\n")
	lat("history limit-last | v3 no index", 50, raw(store.HistorySQLLimitLast))
	lat("history limit-first | v3 no index", 50, hist(s.History))

	if _, err := prov.UpTo(ctx, 4); err != nil { // CREATE INDEX CONCURRENTLY
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `ANALYZE match_players`); err != nil {
		t.Fatal(err)
	}
	lat("history limit-last | v4 indexed", 500, raw(store.HistorySQLLimitLast))
	lat("history limit-first (1 query) | v4 indexed", 500, hist(s.History))
	lat("history N+1 (21 queries) | v4 indexed", 500, hist(s.HistoryNPlusOne))
	explain("History limit-last, schema v4", store.HistorySQLLimitLast, player, 20)
	explain("History limit-first, schema v4", store.HistorySQL, player, 20)
	explain("Partition pruning: one month of matches",
		`SELECT count(*) FROM matches WHERE started_at >= '2026-11-01' AND started_at < '2026-12-01'`)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
