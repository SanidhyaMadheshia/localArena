// Package api is the LocalArena HTTP API (profiles, history, lobbies, store).
//
// SECURITY: there is no authentication yet. player_id in requests is trusted,
// so any caller can act as any player. Keycloak/JWT arrives in week 5; do not
// expose this service beyond localhost/cluster until then.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/ratelimit"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

// Read-your-writes headers: writes return the primary WAL LSN; clients echo it
// on reads so a lagging replica is never used for them.
const (
	HeaderWriteLSN  = "X-Write-LSN"
	HeaderReadAfter = "X-Read-After-LSN"
	requestTimeout  = 3 * time.Second
	maxBody         = 4 << 10
)

// Options tunes caching and rate limits.
type Options struct {
	Cache      *cache.Cache // may be nil or disabled
	ProfileTTL time.Duration
	// ProfileLoadDelay adds latency to profile cache fills (experiments only).
	ProfileLoadDelay time.Duration
	TrustProxy       bool // take the client IP from X-Forwarded-For (set by Envoy)
	PurchaseRL       ratelimit.Rule
	CreateRL         ratelimit.Rule
	PresenceTTL      time.Duration
}

// Server holds dependencies.
type Server struct {
	st       *store.Store
	log      *slog.Logger
	opts     Options
	limiter  *ratelimit.Limiter
	draining atomic.Bool
}

// New returns a Server.
func New(st *store.Store, log *slog.Logger, opts ...Options) *Server {
	o := Options{}
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Cache == nil {
		o.Cache = cache.New(cache.Options{}, log) // disabled: every lookup goes to the database
	}
	if o.ProfileTTL <= 0 {
		o.ProfileTTL = 30 * time.Second
	}
	if o.PresenceTTL <= 0 {
		o.PresenceTTL = 30 * time.Second
	}
	if o.PurchaseRL.Rate == 0 {
		o.PurchaseRL = ratelimit.Rule{Rate: 2, Burst: 5}
	}
	if o.CreateRL.Rate == 0 {
		o.CreateRL = ratelimit.Rule{Rate: 5, Burst: 20}
	}
	return &Server{st: st, log: log, opts: o, limiter: ratelimit.New(o.Cache)}
}

// Drain makes /readyz fail so load balancers stop sending new requests
// before shutdown (graceful drain, adr/0007).
func (s *Server) Drain() { s.draining.Store(true) }

// Handler returns the router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/players", s.createPlayer)
	mux.HandleFunc("GET /v1/players/{id}", s.getProfile)
	mux.HandleFunc("GET /v1/players/{id}/matches", s.getHistory)
	mux.HandleFunc("DELETE /v1/players/{id}/lobby", s.leaveLobby)
	mux.HandleFunc("POST /v1/matches", s.recordMatch)
	mux.HandleFunc("GET /v1/cosmetics", s.listCosmetics)
	mux.HandleFunc("POST /v1/purchases", s.purchase)
	mux.HandleFunc("POST /v1/lobbies", s.createLobby)
	mux.HandleFunc("POST /v1/lobbies/{id}/members", s.joinLobby)
	mux.HandleFunc("GET /v1/lobbies/{id}", s.getLobby)
	mux.HandleFunc("PUT /v1/players/{id}/presence", s.heartbeat)
	mux.HandleFunc("GET /v1/presence", s.presence)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
	return withTimeout(mux)
}

// withTimeout bounds every request's database work; pool waits included.
func withTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ready reports whether *this replica* should receive traffic. It fails only
// while draining. A shared dependency (Postgres, Valkey) being down is not a
// reason: every replica would fail together, the load balancer would have no
// endpoints left, and cache-served reads would stop too (Experiment 003,
// node loss). Dependency state is reported in the body for humans and alerts.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if err := s.st.Primary.Ping(ctx); err != nil {
		_, _ = w.Write([]byte("ready (degraded: primary unavailable)\n"))
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// fail maps domain errors to HTTP status codes; unknown errors are logged, not leaked.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, store.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrUsernameTaken), errors.Is(err, store.ErrAlreadyOwned),
		errors.Is(err, store.ErrAlreadyInLobby), errors.Is(err, store.ErrLobbyFull),
		errors.Is(err, store.ErrLobbyClosed), errors.Is(err, store.ErrOutOfStock):
		status = http.StatusConflict
	case errors.Is(err, store.ErrInsufficientCredits):
		status = http.StatusPaymentRequired
	case errors.Is(err, store.ErrIdempotencyMismatch):
		status = http.StatusUnprocessableEntity
	case errors.Is(err, store.ErrConflict), errors.Is(err, context.DeadlineExceeded), store.IsUnavailable(err):
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
		noProxyRetry(w)
	}
	msg := err.Error()
	if status == http.StatusServiceUnavailable {
		msg = "temporarily unavailable"
	}
	if status == http.StatusInternalServerError {
		s.log.Error("request failed", "path", r.URL.Path, "err", err)
		msg = "internal error"
	}
	writeJSON(w, status, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return false
	}
	return true
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

// stampLSN attaches the read-your-writes token after a successful write.
func (s *Server) stampLSN(w http.ResponseWriter, r *http.Request) {
	if lsn, err := s.st.WriteLSN(r.Context()); err == nil {
		w.Header().Set(HeaderWriteLSN, lsn)
	}
}

// clientIP returns the caller's address. Behind Envoy (TrustProxy) it is the
// rightmost X-Forwarded-For entry, the one Envoy itself appended; entries to
// its left are client-controlled and must not be trusted.
func (s *Server) clientIP(r *http.Request) string {
	if s.opts.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// allow applies a rate-limit rule; on rejection it writes 429 + Retry-After.
func (s *Server) allow(w http.ResponseWriter, r *http.Request, key string, rule ratelimit.Rule) bool {
	d := s.limiter.Allow(r.Context(), key, rule)
	w.Header().Set("X-RateLimit-Backend", d.Backend)
	if d.Allowed {
		return true
	}
	secs := int(d.RetryAfter.Seconds()) + 1
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
	return false
}

func profileKey(id int64) string { return "profile:v1:" + strconv.FormatInt(id, 10) }
func lobbyKey(id int64) string   { return "lobby:v1:" + strconv.FormatInt(id, 10) }

// invalidate drops cache entries after a committed write. Failure is logged,
// not returned: the write succeeded, and the entry expires within its TTL.
func (s *Server) invalidate(r *http.Request, keys ...string) {
	for _, k := range keys {
		if err := s.opts.Cache.Invalidate(r.Context(), k); err != nil && s.opts.Cache.Enabled() {
			s.log.Warn("cache invalidation failed; stale until TTL", "key", k, "err", err)
		}
	}
}

// noProxyRetry marks a response as caused by a shared dependency (database,
// Valkey), not by this replica. Envoy does not retry responses carrying
// x-envoy-ratelimited, so a dependency outage is not multiplied by the proxy
// trying every replica (Experiment 003). Clients still see Retry-After.
func noProxyRetry(w http.ResponseWriter) { w.Header().Set("X-Envoy-Ratelimited", "dependency") }
