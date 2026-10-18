package authz

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFailClosed(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"500":       func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) },
		"undefined": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }, // policy not loaded
		"garbage":   func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`not json`)) },
		"slow":      func(w http.ResponseWriter, _ *http.Request) { time.Sleep(400 * time.Millisecond) },
	}
	for name, h := range cases {
		srv := httptest.NewServer(h)
		err := New(srv.URL).Check(t.Context(), "purchase", Subject{PlayerID: 1, Roles: []string{"player"}}, Resource{PlayerID: 1})
		srv.Close()
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("%s: err = %v, want ErrUnavailable", name, err)
		}
	}
	if err := New("http://127.0.0.1:1").Check(t.Context(), "purchase", Subject{}, Resource{}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("unreachable: %v", err)
	}
}

// Against a real OPA (LOCALARENA_TEST_OPA_URL=http://localhost:8181).
func TestRealPolicy(t *testing.T) {
	u := os.Getenv("LOCALARENA_TEST_OPA_URL")
	if u == "" {
		t.Skip("LOCALARENA_TEST_OPA_URL not set")
	}
	c := New(u)
	p := Subject{PlayerID: 1, Roles: []string{"player"}}
	if err := c.Check(t.Context(), "purchase", p, Resource{PlayerID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := c.Check(t.Context(), "purchase", p, Resource{PlayerID: 2}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("other player's purchase: %v", err)
	}
	p.Banned = true
	if err := c.Check(t.Context(), "purchase", p, Resource{PlayerID: 1}); !errors.Is(err, ErrForbidden) || err.Error() != "forbidden: banned" {
		t.Fatalf("banned: %v", err)
	}
}
