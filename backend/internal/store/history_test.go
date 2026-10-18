package store_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

func TestHistoryNPlusOneVsJoin(t *testing.T) {
	ctx := ctxT(t)
	me := newPlayer(t, 0)
	others := make([]int64, 9)
	for i := range others {
		others[i] = newPlayer(t, 0)
	}
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for m := range 20 {
		scores := []store.MatchPlayer{{PlayerID: me, Score: m}}
		for i, o := range others {
			scores = append(scores, store.MatchPlayer{PlayerID: o, Score: i})
		}
		start := base.Add(time.Duration(m) * time.Hour)
		if _, err := st.RecordMatch(ctx, start, start.Add(10*time.Minute), scores); err != nil {
			t.Fatal(err)
		}
	}
	count := func(fn func() ([]store.MatchSummary, error)) ([]store.MatchSummary, int64) {
		before := store.QueryCount.Load()
		out, err := fn()
		if err != nil {
			t.Fatal(err)
		}
		return out, store.QueryCount.Load() - before
	}
	naive, nq := count(func() ([]store.MatchSummary, error) { return st.HistoryNPlusOne(ctx, st.Primary, me, 20) })
	joined, jq := count(func() ([]store.MatchSummary, error) { return st.History(ctx, st.Primary, me, 20) })
	if nq != 21 || jq != 1 {
		t.Fatalf("queries: n+1=%d (want 21), join=%d (want 1)", nq, jq)
	}
	for i := range naive {
		naive[i].StartedAt, joined[i].StartedAt = naive[i].StartedAt.UTC(), joined[i].StartedAt.UTC()
	}
	if !reflect.DeepEqual(naive, joined) {
		t.Fatalf("results differ:\n%+v\n%+v", naive[0], joined[0])
	}
	if len(joined) != 20 || joined[0].Score != 19 || len(joined[0].Players) != 10 {
		t.Fatalf("unexpected history head %+v", joined[0])
	}
}

// Pause WAL replay to make replica staleness deterministic, then show that
// ReaderFor falls back to the primary until the replica has the write.
func TestStaleReplicaReadAndReadYourWrites(t *testing.T) {
	if replicaURL == "" {
		t.Skip("LOCALARENA_TEST_REPLICA_URL not set")
	}
	ctx := ctxT(t)
	player := newPlayer(t, 100)
	// Wait until the replica has the player at all.
	lsn, _ := st.WriteLSN(ctx)
	if _, ok := st.ReaderFor(ctx, lsn, 5*time.Second); !ok {
		t.Fatal("replica never caught up")
	}
	if _, err := st.Replica.Exec(ctx, `SELECT pg_wal_replay_pause()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = st.Replica.Exec(context.Background(), `SELECT pg_wal_replay_resume()`) })

	if _, err := st.Primary.Exec(ctx, `UPDATE profiles SET credits = 777 WHERE player_id = $1`, player); err != nil {
		t.Fatal(err)
	}
	lsn, _ = st.WriteLSN(ctx)
	if p, _ := st.Profile(ctx, st.Replica, player); p.Credits != 100 {
		t.Fatalf("expected a stale read of 100 from the paused replica, got %d", p.Credits)
	}
	reader, onReplica := st.ReaderFor(ctx, lsn, 100*time.Millisecond)
	if onReplica {
		t.Fatal("ReaderFor chose a replica that has not replayed the write")
	}
	if p, _ := st.Profile(ctx, reader, player); p.Credits != 777 {
		t.Fatalf("read-your-writes violated: %d", p.Credits)
	}
	if _, err := st.Replica.Exec(ctx, `SELECT pg_wal_replay_resume()`); err != nil {
		t.Fatal(err)
	}
	reader, onReplica = st.ReaderFor(ctx, lsn, 5*time.Second)
	if p, _ := st.Profile(ctx, reader, player); !onReplica || p.Credits != 777 {
		t.Fatalf("after resume: replica=%v credits=%d", onReplica, p.Credits)
	}
}

// Every migration rolls back cleanly, and the expand step (00005) is
// compatible with old code but not the other way round: deploy order is
// migrate first, then roll out code that uses the new column.
func TestMigrationsRoundTripAndCompatibility(t *testing.T) {
	ctx := ctxT(t)
	dsn := recreateDB(ctx, primaryURL, "localarena_migtest")
	prov, db, err := migrations.Provider(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := prov.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := prov.DownTo(ctx, 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	if _, err := prov.UpTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	p, err := store.NewPool(ctx, dsn, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := store.New(p, nil)
	id, err := s.CreatePlayer(ctx, "compat_user", 10)
	if err != nil {
		t.Fatal(err)
	}
	const oldCodeQuery = `SELECT pr.display_name, pr.credits FROM profiles pr WHERE pr.player_id = $1`
	var name string
	var credits int64
	// New code on old schema: fails (undefined column).
	_, err = s.Profile(ctx, p, id)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "42703" {
		t.Fatalf("new code on v4 schema: want undefined_column, got %v", err)
	}
	if _, err := prov.Up(ctx); err != nil {
		t.Fatal(err)
	}
	// Old code on new schema: still works.
	if err := p.QueryRow(ctx, oldCodeQuery, id).Scan(&name, &credits); err != nil || credits != 10 {
		t.Fatalf("old code on v5 schema: %v", err)
	}
	if _, err := s.Profile(ctx, p, id); err != nil {
		t.Fatalf("new code on v5 schema: %v", err)
	}
}
