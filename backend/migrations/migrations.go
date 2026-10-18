// Package migrations embeds the goose SQL migrations and applies them.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed *.sql
var FS embed.FS

// Provider returns a goose provider for dsn. The caller must Close the DB.
func Provider(dsn string) (*goose.Provider, *sql.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, nil, err
	}
	// Migrations (index builds, backfills) may legitimately run longer than
	// the application's 2 s statement_timeout set by 00007.
	cfg.RuntimeParams["statement_timeout"] = "0"
	db := stdlib.OpenDB(*cfg)
	// A Postgres advisory lock serializes concurrent migrators (e.g. two Jobs or
	// a Job plus a manual run): the loser waits instead of racing on DDL.
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(5, 60))
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, FS, goose.WithSessionLocker(locker))
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("goose provider: %w", err)
	}
	return p, db, nil
}

// Up applies all pending migrations.
func Up(ctx context.Context, dsn string) error {
	p, db, err := Provider(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = p.Up(ctx)
	return err
}
