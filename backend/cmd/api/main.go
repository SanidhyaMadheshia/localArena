// Command api serves the LocalArena HTTP API backed by PostgreSQL.
//
//	DATABASE_URL  primary (or PgBouncer) DSN, required
//	REPLICA_URL   read replica DSN, optional
//	DB_MAX_CONNS  pool size per process (default 10)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/api"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("api exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	maxConns := int32(10)
	if v, err := strconv.ParseInt(os.Getenv("DB_MAX_CONNS"), 10, 32); err == nil && v > 0 {
		maxConns = int32(v)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	primary, err := store.NewPool(ctx, dsn, maxConns)
	if err != nil {
		return err
	}
	defer primary.Close()
	pools := map[string]*pgxpool.Pool{"primary": primary}
	var replica *pgxpool.Pool
	if u := os.Getenv("REPLICA_URL"); u != "" {
		if replica, err = store.NewPool(ctx, u, maxConns); err != nil {
			return err
		}
		defer replica.Close()
		pools["replica"] = replica
	}
	prometheus.MustRegister(store.NewPoolCollector(pools))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8081"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(store.New(primary, replica), log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", addr, "replica", replica != nil, "max_conns", maxConns)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
