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
	)
	flag.Parse()

	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*logLevel))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mgr := match.NewManager(match.Options{TickHz: *tickHz, Capacity: *capacity, Seed: *seed})
	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(mgr, log, server.Config{WebDir: *webDir, AllowedOrigins: split(*origins)}).Handler(),
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
