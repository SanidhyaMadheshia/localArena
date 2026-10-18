// Command api serves the LocalArena HTTP API backed by PostgreSQL.
//
//	DATABASE_URL  primary (or PgBouncer) DSN, required
//	REPLICA_URL   read replica DSN, optional
//	DB_MAX_CONNS  pool size per process (default 10)
//	VALKEY_ADDR   host:port of Valkey; empty disables caching and global rate limits
//	CACHE_SINGLEFLIGHT  "0" disables the in-process stampede guard (experiments)
//	TRUST_PROXY   "1" when behind Envoy: client IP comes from X-Forwarded-For
//	DRAIN_SECONDS how long /readyz fails before shutdown (default 5)
//	PROFILE_TTL   profile cache TTL (Go duration, default 30s; experiments use 1s)
//	PROFILE_LOAD_DELAY  experiments only: extra latency added to each profile cache fill
//	OIDC_ISSUER, OIDC_JWKS_URL  enable /v2 (token auth); JWKS URL defaults to the issuer's certs endpoint
//	OPA_URL             policy engine for /v2 (required with OIDC; v2 fails closed without it)
//	GAME_TICKET_SECRET  >= 32 bytes, shared with the gameserver (from SOPS)
//	LEGACY_V1           "1" keeps the unauthenticated /v1 routes alongside /v2
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
	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/authz"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
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

	c := cache.New(cache.Options{
		Addr:         os.Getenv("VALKEY_ADDR"),
		Singleflight: os.Getenv("CACHE_SINGLEFLIGHT") != "0",
	}, log)
	defer c.Close()
	ttl, _ := time.ParseDuration(os.Getenv("PROFILE_TTL")) // 0 -> default
	delay, _ := time.ParseDuration(os.Getenv("PROFILE_LOAD_DELAY"))
	opts := api.Options{Cache: c, ProfileTTL: ttl, ProfileLoadDelay: delay, TrustProxy: os.Getenv("TRUST_PROXY") == "1",
		LegacyV1: os.Getenv("LEGACY_V1") == "1"}
	if iss := os.Getenv("OIDC_ISSUER"); iss != "" {
		jwks := os.Getenv("OIDC_JWKS_URL")
		if jwks == "" {
			jwks = iss + "/protocol/openid-connect/certs"
		}
		opts.Auth = auth.NewVerifier(ctx, iss, jwks, "localarena-api")
		if u := os.Getenv("OPA_URL"); u != "" {
			opts.Authz = authz.New(u)
		}
		if sec := os.Getenv("GAME_TICKET_SECRET"); sec != "" {
			if opts.Tickets, err = auth.NewTickets([]byte(sec)); err != nil {
				return err
			}
		}
		if sec := os.Getenv("WEBHOOK_SIGNING_SECRET"); sec != "" {
			opts.WebhookSecrets = append(opts.WebhookSecrets, []byte(sec))
			if prev := os.Getenv("WEBHOOK_SIGNING_SECRET_PREVIOUS"); prev != "" {
				opts.WebhookSecrets = append(opts.WebhookSecrets, []byte(prev))
			}
		}
		log.Info("v2 auth enabled", "issuer", iss, "opa", opts.Authz != nil, "legacy_v1", opts.LegacyV1)
	}
	apiSrv := api.New(store.New(primary, replica), log, opts)
	drain := 5 * time.Second
	if v, err := strconv.Atoi(os.Getenv("DRAIN_SECONDS")); err == nil && v >= 0 {
		drain = time.Duration(v) * time.Second
	}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8081"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           apiSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second, // > 3 s request budget, so handlers finish their own error replies
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
	// Graceful drain: fail readiness first so Envoy health checks (2 s
	// interval x 2) and Kubernetes endpoints stop routing here, keep serving
	// in-flight and straggler requests, then stop accepting and wait for the rest.
	log.Info("draining", "for", drain)
	apiSrv.Drain()
	time.Sleep(drain)
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdown)
	log.Info("stopped", "err", err)
	return err
}
