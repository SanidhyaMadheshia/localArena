// Command migrate applies LocalArena schema migrations (goose, embedded SQL).
//
//	migrate up | down | down-to N | status | version
//
// DATABASE_URL must point directly at Postgres (not PgBouncer).
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: migrate up|down|down-to N|status|version")
		os.Exit(2)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := run(ctx, dsn, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string, args []string) error {
	p, db, err := migrations.Provider(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	// Retry the first connection: in Compose/Kubernetes the DB may still be starting.
	for i := 0; ; i++ {
		if err = db.PingContext(ctx); err == nil || i == 30 {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return err
	}
	switch args[0] {
	case "up":
		// Failed-migration drill (Experiment 007): fail like a bad migration would,
		// after connecting, so the release's pre-upgrade hook fails.
		if os.Getenv("MIGRATE_FAIL_FOR_DRILL") == "1" {
			_, err := db.ExecContext(ctx, `ALTER TABLE profiles ADD COLUMN drill_bad integer NOT NULL CHECK (drill_bad > 0)`)
			return fmt.Errorf("drill migration failed (as intended): %w", err)
		}
		res, err := p.Up(ctx)
		for _, r := range res {
			fmt.Println(r)
		}
		return err
	case "down":
		r, err := p.Down(ctx)
		if r != nil {
			fmt.Println(r)
		}
		return err
	case "down-to":
		if len(args) < 2 {
			return fmt.Errorf("down-to needs a version")
		}
		v, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return err
		}
		res, err := p.DownTo(ctx, v)
		for _, r := range res {
			fmt.Println(r)
		}
		return err
	case "status":
		st, err := p.Status(ctx)
		for _, s := range st {
			fmt.Printf("%-8s %-40s %s\n", s.State, s.Source.Path, s.AppliedAt.Format(time.RFC3339))
		}
		return err
	case "version":
		v, err := p.GetDBVersion(ctx)
		fmt.Println(v)
		return err
	}
	return fmt.Errorf("unknown command %q", args[0])
}
