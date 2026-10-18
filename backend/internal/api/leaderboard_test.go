package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchTop(t *testing.T) {
	streams := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/top" || r.URL.Query().Get("n") != "3" {
			t.Errorf("unexpected request %s", r.URL)
		}
		_, _ = w.Write([]byte(`[{"player_id":7,"total":42},{"player_id":"20","total":41}]`))
	}))
	defer streams.Close()
	rows, err := fetchTop(t.Context(), streams.URL, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0] != (leaderRow{PlayerID: 7, Total: 42}) || rows[1].PlayerID != 20 {
		t.Fatalf("rows = %+v", rows)
	}
}

// Streams down (rebalancing, restoring, stopped): 503 + Retry-After, never 500.
func TestLeaderboardDegrades(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	for _, url := range []string{"", down.URL} {
		s := &Server{log: slog.New(slog.DiscardHandler), opts: Options{StreamsURL: url}}
		rec := httptest.NewRecorder()
		s.leaderboard(rec, httptest.NewRequest(http.MethodGet, "/v2/leaderboard?n=5", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("streams=%q: status %d", url, rec.Code)
		}
	}
}
