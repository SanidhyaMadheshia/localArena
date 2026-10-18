package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

const startingCredits = 1000

func (s *Server) createPlayer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !s.allow(w, r, "create:ip:"+s.clientIP(r), s.opts.CreateRL) {
		return
	}
	if n := len(req.Username); n < 3 || n > 32 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username must be 3-32 characters"})
		return
	}
	key := r.Header.Get("Idempotency-Key") // optional; makes client retries safe
	if len(key) > 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key must be at most 64 chars"})
		return
	}
	id, replayed, err := s.st.CreatePlayerIdempotent(r.Context(), key, req.Username, startingCredits)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.stampLSN(w, r)
	code := http.StatusCreated
	if replayed {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]int64{"id": id})
}

// getProfile is cache-aside over the primary. Fills read the primary so a
// lagging replica can never be copied into the cache. A client carrying a
// read-your-writes token bypasses the cache and uses LSN-aware replica routing.
func (s *Server) getProfile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if lsn := r.Header.Get(HeaderReadAfter); lsn != "" {
		reader, fromReplica := s.st.ReaderFor(r.Context(), lsn, 200*time.Millisecond)
		p, err := s.st.Profile(r.Context(), reader, id)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		w.Header().Set("X-Served-By", map[bool]string{true: "replica", false: "primary"}[fromReplica])
		writeJSON(w, http.StatusOK, p)
		return
	}
	p, hit, err := cache.GetOrLoad(r.Context(), s.opts.Cache, profileKey(id), s.opts.ProfileTTL,
		func(ctx context.Context) (store.Profile, error) {
			if d := s.opts.ProfileLoadDelay; d > 0 {
				time.Sleep(d)
			}
			return s.st.Profile(ctx, s.st.Primary, id)
		})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("X-Served-By", map[bool]string{true: "cache", false: "primary"}[hit])
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) getHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 || limit > 100 {
		limit = 20
	}
	fetch := s.st.History
	if r.URL.Query().Get("mode") == "n1" { // deliberately slow path for Experiment 002
		fetch = s.st.HistoryNPlusOne
	}
	out, err := fetch(r.Context(), s.st.Replica, id, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) recordMatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		StartedAt time.Time           `json:"started_at"`
		EndedAt   time.Time           `json:"ended_at"`
		Players   []store.MatchPlayer `json:"players"`
	}
	if !decode(w, r, &req) {
		return
	}
	if len(req.Players) == 0 || len(req.Players) > 20 || !req.EndedAt.After(req.StartedAt) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "need 1-20 players and ended_at > started_at"})
		return
	}
	id, err := s.st.RecordMatch(r.Context(), req.StartedAt, req.EndedAt, req.Players)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

func (s *Server) listCosmetics(w http.ResponseWriter, r *http.Request) {
	out, err := s.st.Cosmetics(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// purchase requires an Idempotency-Key header so client retries are safe.
func (s *Server) purchase(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 64 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header (1-64 chars) is required"})
		return
	}
	var req struct {
		PlayerID   int64  `json:"player_id"`
		CosmeticID int64  `json:"cosmetic_id"`
		Strategy   string `json:"strategy,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !s.allow(w, r, "purchase:player:"+strconv.FormatInt(req.PlayerID, 10), s.opts.PurchaseRL) {
		return
	}
	strategy := store.Conditional
	if req.Strategy != "" {
		var err error
		if strategy, err = store.ParseStrategy(req.Strategy); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}
	rc, err := s.st.Purchase(r.Context(), req.PlayerID, req.CosmeticID, key, strategy)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidate(r, profileKey(req.PlayerID)) // after commit: credits and items changed
	s.stampLSN(w, r)
	code := http.StatusCreated
	if rc.Replayed {
		code = http.StatusOK
	}
	writeJSON(w, code, rc)
}

func (s *Server) createLobby(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OwnerID  int64 `json:"owner_id"`
		Capacity int   `json:"capacity"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Capacity < 2 || req.Capacity > 20 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "capacity must be 2-20"})
		return
	}
	id, err := s.st.CreateLobby(r.Context(), req.OwnerID, req.Capacity)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

func (s *Server) joinLobby(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		PlayerID int64 `json:"player_id"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := s.st.JoinLobby(r.Context(), id, req.PlayerID); err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidate(r, lobbyKey(id))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) leaveLobby(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	lobby, err := s.st.LeaveLobby(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.invalidate(r, lobbyKey(lobby))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getLobby(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	l, hit, err := cache.GetOrLoad(r.Context(), s.opts.Cache, lobbyKey(id), 10*time.Second,
		func(ctx context.Context) (store.Lobby, error) { return s.st.GetLobby(ctx, id) })
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("X-Served-By", map[bool]string{true: "cache", false: "primary"}[hit])
	writeJSON(w, http.StatusOK, l)
}

// heartbeat marks a player online for PresenceTTL. Presence is ephemeral and
// lives only in Valkey; when Valkey is down the API reports 503 for writes and
// "unknown" for reads instead of pretending.
func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.opts.Cache.Touch(r.Context(), "presence:"+strconv.FormatInt(id, 10), "1", s.opts.PresenceTTL); err != nil {
		w.Header().Set("Retry-After", "1")
		noProxyRetry(w)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "presence temporarily unavailable"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) presence(w http.ResponseWriter, r *http.Request) {
	raw := strings.Split(r.URL.Query().Get("ids"), ",")
	if len(raw) == 0 || len(raw) > 100 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ids: 1-100 comma-separated player ids"})
		return
	}
	keys := make([]string, len(raw))
	for i, v := range raw {
		id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || id <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id " + v})
			return
		}
		keys[i] = "presence:" + strconv.FormatInt(id, 10)
	}
	out := make(map[string]string, len(raw))
	online, err := s.opts.Cache.Exists(r.Context(), keys)
	for i, v := range raw {
		switch {
		case err != nil:
			out[strings.TrimSpace(v)] = "unknown"
		case online[i]:
			out[strings.TrimSpace(v)] = "online"
		default:
			out[strings.TrimSpace(v)] = "offline"
		}
	}
	writeJSON(w, http.StatusOK, out)
}
