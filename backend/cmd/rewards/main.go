// Command rewards runs the match-reward saga worker (adr/0012).
//
//	KAFKA_BROKERS  comma-separated brokers (required)
//	DATABASE_URL   Postgres primary or PgBouncer (required)
//	METRICS_ADDR   Prometheus listener (default :9102)
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/rewards"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	brokers, dsn := os.Getenv("KAFKA_BROKERS"), os.Getenv("DATABASE_URL")
	if brokers == "" || dsn == "" {
		log.Error("KAFKA_BROKERS and DATABASE_URL are required")
		os.Exit(2)
	}
	pool, err := store.NewPool(ctx, dsn, 5)
	if err != nil {
		log.Error("db", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9102"
	}
	srv := &http.Server{Addr: addr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics", "err", err)
		}
	}()
	log.Info("reward saga worker starting", "brokers", brokers)
	w := &rewards.Worker{Brokers: strings.Split(brokers, ","), Steps: store.New(pool, nil), Log: log}
	_ = w.Run(ctx)
	_ = srv.Close()
	log.Info("stopped")
}
