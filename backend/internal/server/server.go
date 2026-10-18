// Package server exposes the game server over HTTP: WebSocket gameplay,
// status (JSON, long poll, SSE), health probes, metrics and the static client.
package server

import (
	"log/slog"
	"net/http"
	"net/url"
	"slices"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/match"
)

// Config configures the HTTP surface.
type Config struct {
	WebDir         string   // static client directory; empty disables
	AllowedOrigins []string // extra Origin hosts allowed for WebSocket (e.g. "localhost:5173")
	// Tickets, when set, makes /ws require ?ticket= (minted by the API's
	// POST /v2/game-tickets) and ignores ?player=. Guests are refused.
	Tickets *auth.Tickets
	// AllowGuests keeps anonymous sessions when Tickets is set (bots/experiments).
	AllowGuests bool
	// TrustPlayerParam accepts ?player=<id> without a ticket. LAB ONLY: lets
	// load bots play as accounts (rewards drills). Off by default (game day
	// finding: account bots had silently become guests once tickets were on).
	TrustPlayerParam bool
}

// Server holds HTTP handlers.
type Server struct {
	mgr      *match.Manager
	log      *slog.Logger
	upgrader websocket.Upgrader
	cfg      Config
}

// New builds a Server.
func New(mgr *match.Manager, log *slog.Logger, cfg Config) *Server {
	s := &Server{mgr: mgr, log: log, cfg: cfg}
	s.upgrader = websocket.Upgrader{ReadBufferSize: 512, WriteBufferSize: 4096, CheckOrigin: s.checkOrigin}
	return s
}

// checkOrigin allows same-origin and non-browser clients (no Origin header),
// plus an explicit allowlist. Never allow all origins: that enables
// cross-site WebSocket hijacking once auth exists (week 5).
func (s *Server) checkOrigin(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	u, err := url.Parse(o)
	if err != nil {
		return false
	}
	return u.Host == r.Host || slices.Contains(s.cfg.AllowedOrigins, u.Host)
}

// Handler returns the public router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.serveWS)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /status/stream", s.handleStatusStream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	// Readiness fails while draining so Services/LBs stop sending new players here.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.mgr.Draining() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	if s.cfg.WebDir != "" {
		mux.Handle("GET /", http.FileServer(http.Dir(s.cfg.WebDir)))
	}
	return mux
}
