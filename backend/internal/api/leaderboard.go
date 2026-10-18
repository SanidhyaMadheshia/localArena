package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Leaderboard is a read-only, public view for the web client: the Kafka
// Streams interactive query (global top N, scatter-gather; adr/0011) joined
// with display names from Postgres. Streams state is derived data, so on any
// Streams error the client gets 503 and keeps its last board (degrade, not fail).
type leaderRow struct {
	PlayerID int64  `json:"player_id"`
	Name     string `json:"name"`
	Total    int64  `json:"total"`
}

var streamsClient = &http.Client{Timeout: 800 * time.Millisecond}

func (s *Server) leaderboard(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil || n < 1 || n > 50 {
		n = 10
	}
	if s.opts.StreamsURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leaderboard not configured"})
		return
	}
	rows, err := fetchTop(r.Context(), s.opts.StreamsURL, n)
	if err != nil {
		s.log.Warn("leaderboard unavailable", "err", err)
		w.Header().Set("Retry-After", "5")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leaderboard temporarily unavailable"})
		return
	}
	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.PlayerID
	}
	names, err := s.st.DisplayNames(r.Context(), ids)
	if err != nil { // names are decoration: still serve the board
		s.log.Warn("leaderboard names", "err", err)
	}
	for i := range rows {
		if rows[i].Name = names[rows[i].PlayerID]; rows[i].Name == "" {
			rows[i].Name = fmt.Sprintf("player %d", rows[i].PlayerID)
		}
	}
	w.Header().Set("Cache-Control", "public, max-age=2")
	writeJSON(w, http.StatusOK, map[string]any{"top": rows})
}

func fetchTop(ctx context.Context, base string, n int) ([]leaderRow, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/top?n="+strconv.Itoa(n), nil)
	if err != nil {
		return nil, err
	}
	resp, err := streamsClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("streams /top: %s", resp.Status)
	}
	var raw []struct {
		PlayerID json.Number `json:"player_id"`
		Total    int64       `json:"total"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]leaderRow, 0, len(raw))
	for _, x := range raw {
		id, err := x.PlayerID.Int64()
		if err != nil {
			continue
		}
		out = append(out, leaderRow{PlayerID: id, Total: x.Total})
	}
	return out, nil
}
