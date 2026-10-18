package flags

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFallbacks(t *testing.T) {
	var nilClient *Client
	if nilClient.String(t.Context(), "game-mode", "m", "classic") != "classic" {
		t.Fatal("nil client")
	}
	for name, h := range map[string]http.HandlerFunc{
		"404":  func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) },
		"bool": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"value":true}`)) },
		"slow": func(w http.ResponseWriter, _ *http.Request) { time.Sleep(300 * time.Millisecond) },
		"junk": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`<html>`)) },
	} {
		srv := httptest.NewServer(h)
		if v := New(srv.URL).String(t.Context(), "game-mode", "m", "classic"); v != "classic" {
			t.Errorf("%s: %q", name, v)
		}
		srv.Close()
	}
}

func TestFlagdRollout(t *testing.T) {
	u := os.Getenv("LOCALARENA_TEST_FLAGD_URL")
	if u == "" {
		t.Skip("LOCALARENA_TEST_FLAGD_URL not set")
	}
	c, counts := New(u), map[string]int{}
	for i := range 500 {
		v := c.String(t.Context(), "game-mode", "match-"+string(rune('a'+i%26))+string(rune(i)), "classic")
		counts[v]++
		a, b := c.String(t.Context(), "game-mode", "match-sticky", "classic"), c.String(t.Context(), "game-mode", "match-sticky", "classic")
		if a != b {
			t.Fatal("same targeting key must get the same variant")
		}
	}
	if f := counts["frenzy"]; f < 50 || f > 150 {
		t.Fatalf("frenzy share %d/500, want ~20%%: %v", f, counts)
	}
	t.Logf("variants: %v", counts)
}
