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
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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

// Server holds dependencies.
type Server struct {
	st  *store.Store
	log *slog.Logger
}

// New returns a Server.
func New(st *store.Store, log *slog.Logger) *Server { return &Server{st: st, log: log} }

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

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.st.Primary.Ping(r.Context()); err != nil {
		http.Error(w, "primary unavailable", http.StatusServiceUnavailable)
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
