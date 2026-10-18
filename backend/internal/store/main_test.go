package store_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

// Integration tests run against real Postgres (docker compose up -d postgres postgres-replica pgbouncer):
//
//	LOCALARENA_TEST_DATABASE_URL  primary, direct (required; tests skip without it)
//	LOCALARENA_TEST_REPLICA_URL   streaming replica (optional)
//	LOCALARENA_TEST_PGBOUNCER_URL pgbouncer (optional)
//
// TestMain recreates a dedicated database so runs are isolated and repeatable.
var (
	primaryURL, replicaURL, bouncerURL string
	st                                 *store.Store
	uniq                               atomic.Int64
)

const testDB = "localarena_test"

func TestMain(m *testing.M) {
	base := os.Getenv("LOCALARENA_TEST_DATABASE_URL")
	if base == "" {
		fmt.Println("LOCALARENA_TEST_DATABASE_URL not set; integration tests will skip")
		os.Exit(m.Run())
	}
	ctx := context.Background()
	primaryURL = recreateDB(ctx, base, testDB)
	if err := migrations.Up(ctx, primaryURL); err != nil {
		panic(err)
	}
	p, err := store.NewPool(ctx, primaryURL, 50)
	if err != nil {
		panic(err)
	}
	var r *pgxpool.Pool
	if u := os.Getenv("LOCALARENA_TEST_REPLICA_URL"); u != "" {
		replicaURL = withDB(u, testDB)
		if r, err = store.NewPool(ctx, replicaURL, 10); err != nil {
			panic(err)
		}
	}
	bouncerURL = os.Getenv("LOCALARENA_TEST_PGBOUNCER_URL")
	st = store.New(p, r)
	code := m.Run()
	p.Close()
	if r != nil {
		r.Close()
	}
	os.Exit(code)
}

func withDB(dsn, db string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		panic(err)
	}
	u.Path = "/" + db
	return u.String()
}

func recreateDB(ctx context.Context, admin, db string) string {
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close(ctx) }()
	for _, q := range []string{`DROP DATABASE IF EXISTS ` + db + ` WITH (FORCE)`, `CREATE DATABASE ` + db} {
		if _, err := c.Exec(ctx, q); err != nil {
			panic(err)
		}
	}
	return withDB(admin, db)
}

// ctxT is called first by every integration test; it skips without a database.
func ctxT(t *testing.T) context.Context {
	if st == nil {
		t.Skip("LOCALARENA_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newPlayer(t *testing.T, credits int64) int64 {
	t.Helper()
	id, err := st.CreatePlayer(ctxT(t), fmt.Sprintf("p%d_%d", time.Now().UnixNano()%1e6, uniq.Add(1)), credits)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func newCosmetic(t *testing.T, price int64, stock *int32) int64 {
	t.Helper()
	var id int64
	err := st.Primary.QueryRow(ctxT(t), `INSERT INTO cosmetics (sku, name, price, stock) VALUES ($1, 'test', $2, $3) RETURNING id`,
		fmt.Sprintf("sku-%d", uniq.Add(1)), price, stock).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func scalar[T any](t *testing.T, q *pgxpool.Pool, sql string, args ...any) T {
	t.Helper()
	var v T
	if err := q.QueryRow(ctxT(t), sql, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

func ptr[T any](v T) *T { return &v }
