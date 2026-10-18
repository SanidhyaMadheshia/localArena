// Command gameserver runs authoritative LocalArena matches over WebSocket.
//
// Public listener (-addr): /ws, /status, /status/stream, /healthz, /metrics, static client.
// Debug listener (-debug-addr, loopback by default): /debug/pprof/*.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/events"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/flags"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/match"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/server"
)

func main() {
	var (
		addr      = flag.String("addr", env("ADDR", ":8080"), "public listen address")
		debugAddr = flag.String("debug-addr", env("DEBUG_ADDR", "127.0.0.1:6060"), "pprof listen address; empty disables")
		tickHz    = flag.Int("tick-hz", 20, "simulation ticks per second")
		capacity  = flag.Int("capacity", 20, "players per match")
		seed      = flag.Uint64("seed", 1, "base world seed")
		webDir    = flag.String("web", env("WEB_DIR", ""), "static client directory")
		origins   = flag.String("allowed-origins", env("ALLOWED_ORIGINS", ""), "comma-separated extra WebSocket origin hosts")
		logLevel  = flag.String("log-level", env("LOG_LEVEL", "info"), "debug|info|warn|error")
		brokers   = flag.String("kafka", env("KAFKA_BROKERS", ""), "comma-separated Kafka brokers; empty disables events")
		spoolDir  = flag.String("spool-dir", env("EVENT_SPOOL_DIR", ""), "durable event spool directory (used while Kafka is unavailable)")
		roundSecs = flag.Int("round-seconds", 120, "match round length; each round ends with MatchCompleted")
		guests    = flag.Bool("allow-guests", env("ALLOW_GUESTS", "") == "1", "with GAME_TICKET_SECRET set, still accept anonymous guests")
	)
	flag.Parse()

	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*logLevel))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var pub *events.Publisher
	if *brokers != "" {
		host, _ := os.Hostname()
		var err error
		pub, err = events.New(events.Options{Brokers: split(*brokers), Source: "gameserver/" + host, SpoolDir: *spoolDir}, log)
		if err != nil {
			log.Error("events", "err", err)
			os.Exit(1)
		}
		log.Info("publishing events", "brokers", *brokers, "spool", *spoolDir)
	}
	var tickets *auth.Tickets
	if sec := os.Getenv("GAME_TICKET_SECRET"); sec != "" {
		var err error
		if tickets, err = auth.NewTickets([]byte(sec)); err != nil {
			log.Error("tickets", "err", err)
			os.Exit(1)
		}
		log.Info("game tickets required", "allow_guests", *guests)
	}
	fl := flags.New(env("FLAGD_URL", ""))
	modeFor := func(round string) string {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond) // bounded stall at round start only
		defer cancel()
		return fl.String(ctx, "game-mode", round, "classic")
	}
	mgr := match.NewManager(match.Options{ModeFor: modeFor, TickHz: *tickHz, Capacity: *capacity, Seed: *seed,
		Events: pub, RoundTicks: uint64(max(*roundSecs, 0) * *tickHz)}) // #nosec G115 -- clamped non-negative
	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(mgr, log, server.Config{WebDir: *webDir, AllowedOrigins: split(*origins), Tickets: tickets, AllowGuests: *guests}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// Request contexts derive from ctx so long polls and SSE streams end on shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	if *debugAddr != "" {
		dbg := &http.Server{Addr: *debugAddr, Handler: pprofMux(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Info("debug listening", "addr", *debugAddr)
			if err := dbg.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("debug server", "err", err)
			}
		}()
		defer func() { _ = dbg.Close() }()
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("gameserver listening", "addr", *addr, "tick_hz", *tickHz, "capacity", *capacity)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		log.Error("listen", "err", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Stop accepting connections, then end matches; that closes every WebSocket
	// (hijacked connections are not tracked by http.Server.Shutdown).
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	mgr.Close()
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	pub.Close(flushCtx) // undelivered durable events stay in the spool for the next start
	log.Info("stopped")
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func split(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// pprofMux mounts pprof explicitly instead of via http.DefaultServeMux, so the
// profiler can never leak onto the public listener.
func pprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
