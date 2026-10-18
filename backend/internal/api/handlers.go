package api

import (
	"net/http"
	"strconv"
	"time"

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
	if n := len(req.Username); n < 3 || n > 32 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username must be 3-32 characters"})
		return
	}
	id, err := s.st.CreatePlayer(r.Context(), req.Username, startingCredits)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.stampLSN(w, r)
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

// getProfile reads from the replica unless the client's last write is newer.
func (s *Server) getProfile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	reader, fromReplica := s.st.ReaderFor(r.Context(), r.Header.Get(HeaderReadAfter), 200*time.Millisecond)
	p, err := s.st.Profile(r.Context(), reader, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	w.Header().Set("X-Served-By", map[bool]string{true: "replica", false: "primary"}[fromReplica])
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
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) leaveLobby(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.st.LeaveLobby(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
