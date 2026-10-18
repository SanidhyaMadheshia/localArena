package api_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/api"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/auth/authtest"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/authz"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/webhook"
)

type v2 struct {
	t   *testing.T
	url string
	idp *authtest.IDP
}

func (c v2) do(method, path, token, body string, hdr ...string) (int, map[string]any, http.Header) {
	c.t.Helper()
	req, _ := http.NewRequestWithContext(c.t.Context(), method, c.url+path, bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return resp.StatusCode, m, resp.Header
}

// Needs LOCALARENA_TEST_DATABASE_URL and LOCALARENA_TEST_OPA_URL (docker compose --profile security up -d opa).
func newV2(t *testing.T, legacy bool, fetch *http.Client) v2 {
	opa := os.Getenv("LOCALARENA_TEST_OPA_URL")
	if opa == "" {
		t.Skip("LOCALARENA_TEST_OPA_URL not set")
	}
	dsn := testDSN(t, "localarena_apiv2")
	pool, err := store.NewPool(t.Context(), dsn, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	idp := authtest.New(t)
	tk, _ := auth.NewTickets([]byte("0123456789abcdef0123456789abcdef"))
	srv := api.New(store.New(pool, nil), slog.New(slog.DiscardHandler), api.Options{
		Auth:  auth.NewVerifier(context.Background(), authtest.Issuer, idp.URL, authtest.Audience),
		Authz: authz.New(opa), Tickets: tk, LegacyV1: legacy, AvatarFetch: fetch})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return v2{t, ts.URL, idp}
}

func TestV2AuthFlow(t *testing.T) {
	c := newV2(t, false, nil)
	alice := c.idp.Token(t, "sub-alice", "alice", "player")
	mallory := c.idp.Token(t, "sub-mallory", "mallory", "player")
	admin := c.idp.Token(t, "sub-admin", "ada", "player", "admin")

	if code, _, h := c.do("GET", "/v2/me", "", ""); code != 401 || h.Get("WWW-Authenticate") == "" {
		t.Fatalf("no token: %d", code)
	}
	if code, _, _ := c.do("GET", "/v2/me", alice+"x", ""); code != 401 {
		t.Fatalf("tampered token: %d", code)
	}
	code, m, h := c.do("GET", "/v2/me", alice, "")
	if code != 200 || h.Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(h.Get("Content-Security-Policy"), "default-src 'none'") {
		t.Fatalf("me: %d %v", code, m)
	}
	aliceID := int64(m["profile"].(map[string]any)["player_id"].(float64))
	_, m2, _ := c.do("GET", "/v2/me", alice, "") // second login maps to the same player
	if int64(m2["profile"].(map[string]any)["player_id"].(float64)) != aliceID {
		t.Fatal("same sub mapped to a different player")
	}

	// The acting player comes from the token: a body player_id is rejected as unknown.
	if code, _, _ := c.do("POST", "/v2/purchases", alice, fmt.Sprintf(`{"player_id":%d,"cosmetic_id":1}`, aliceID+1), "Idempotency-Key", "k1"); code != 400 {
		t.Fatalf("body player_id accepted: %d", code)
	}
	if code, _, _ := c.do("POST", "/v2/purchases", alice, `{"cosmetic_id":1}`, "Idempotency-Key", "k1"); code != 201 {
		t.Fatalf("purchase: %d", code)
	}

	// Only admins ban, and a ban takes effect immediately for a still-valid token.
	_, mm, _ := c.do("GET", "/v2/me", mallory, "")
	malloryID := int64(mm["profile"].(map[string]any)["player_id"].(float64))
	ban := fmt.Sprintf(`{"player_id":%d,"reason":"cheating"}`, malloryID)
	if code, _, _ := c.do("POST", "/v2/admin/bans", alice, ban); code != 403 {
		t.Fatalf("player banned someone: %d", code)
	}
	if code, _, _ := c.do("POST", "/v2/admin/bans", admin, ban); code != 204 {
		t.Fatalf("admin ban: %d", code)
	}
	if code, m, _ := c.do("POST", "/v2/purchases", mallory, `{"cosmetic_id":1}`, "Idempotency-Key", "k2"); code != 403 {
		t.Fatalf("banned purchase: %d %v", code, m)
	}
	if code, _, _ := c.do("POST", "/v2/game-tickets", mallory, ""); code != 403 {
		t.Fatalf("banned ticket: %d", code)
	}
	if code, m, _ := c.do("POST", "/v2/game-tickets", alice, ""); code != 201 || m["ticket"] == "" {
		t.Fatalf("ticket: %d", code)
	}
	if code, _, _ := c.do("POST", "/v1/players", "", `{"username":"legacy"}`); code != 404 {
		t.Fatalf("v1 must be off when LegacyV1=false: %d", code)
	}

	// Stored XSS attempt: kept verbatim as data, served only as escaped JSON.
	xss := `<img src=x onerror=alert(1)>`
	if code, _, _ := c.do("PUT", "/v2/me/profile", alice, fmt.Sprintf(`{"display_name":%q}`, xss)); code != 204 {
		t.Fatalf("display name: %d", code)
	}
	req, _ := http.NewRequestWithContext(t.Context(), "GET", c.url+fmt.Sprintf("/v2/players/%d", aliceID), nil)
	resp, _ := http.DefaultClient.Do(req)
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if bytes.Contains(raw, []byte("<img")) || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("raw HTML in response: %s", raw)
	}
	if code, _, _ := c.do("PUT", "/v2/me/profile", alice, `{"display_name":"evil\u202ename"}`); code != 400 {
		t.Fatalf("bidi override accepted: %d", code)
	}

	// SQL injection attempt in a path parameter is a 400, not a query.
	if code, _, _ := c.do("GET", "/v2/players/1%20OR%201=1", "", ""); code != 400 {
		t.Fatalf("sqli path: %d", code)
	}
	// Oversized body (4 KiB cap).
	if code, _, _ := c.do("PUT", "/v2/me/profile", alice, `{"display_name":"`+strings.Repeat("a", 8000)+`"}`); code != 400 {
		t.Fatalf("oversized body: %d", code)
	}
	// CORS: known origin echoed, unknown origin gets nothing.
	if _, _, h := c.do("OPTIONS", "/v2/me", "", "", "Origin", "https://localhost:8443"); h.Get("Access-Control-Allow-Origin") != "https://localhost:8443" {
		t.Fatal("allowed origin not echoed")
	}
	if _, _, h := c.do("OPTIONS", "/v2/me", "", "", "Origin", "https://evil.example"); h.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unknown origin allowed")
	}
}

func TestV1DeprecationHeaders(t *testing.T) {
	c := newV2(t, true, nil)
	_, _, h := c.do("GET", "/v1/cosmetics", "", "")
	if h.Get("Deprecation") == "" || h.Get("Sunset") == "" || !strings.Contains(h.Get("Link"), "successor-version") {
		t.Fatalf("v1 headers: %v", h)
	}
}

// OPA down: v2 fails closed (503), never "allow".
func TestV2FailsClosedWithoutOPA(t *testing.T) {
	dsn := testDSN(t, "localarena_apiv2b")
	pool, _ := store.NewPool(t.Context(), dsn, 2)
	t.Cleanup(pool.Close)
	idp := authtest.New(t)
	srv := api.New(store.New(pool, nil), slog.New(slog.DiscardHandler), api.Options{
		Auth: auth.NewVerifier(context.Background(), authtest.Issuer, idp.URL, authtest.Audience), Authz: authz.New("http://127.0.0.1:1")})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	c := v2{t, ts.URL, idp}
	if code, _, _ := c.do("POST", "/v2/purchases", idp.Token(t, "s", "u", "player"), `{"cosmetic_id":1}`, "Idempotency-Key", "k"); code != 503 {
		t.Fatalf("OPA down: %d, want 503", code)
	}
}

func TestSSRFGuard(t *testing.T) {
	for _, u := range []string{"http://example.com/a.png", "file:///etc/passwd", "gopher://x", "https://user:pw@example.com/a",
		"https://127.0.0.1/a.png", "https://localhost/a.png", "https://169.254.169.254/latest/meta-data/",
		"https://[::1]/a", "https://[::ffff:10.0.0.1]/a", "https://10.0.0.5/a", "https://example.com:6379/"} {
		_, _, err := api.FetchAvatar(t.Context(), nil, u)
		var ue *api.UnsafeURLError
		if !errors.As(err, &ue) {
			t.Errorf("%s: err = %v, want UnsafeURLError", u, err)
		}
	}
}

// Content checks with a trusted test client (the SSRF guard would refuse the loopback test server).
func TestAvatarContentValidation(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok.png":
			w.Header().Set("Content-Type", "text/html") // declared type is ignored; bytes are sniffed
			_, _ = w.Write(png)
		case "/html":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write([]byte("<html><script>alert(1)</script></html>"))
		case "/big":
			_, _ = w.Write(append(png, make([]byte, api.MaxAvatarBytes)...))
		}
	}))
	defer srv.Close()
	cl := srv.Client()
	cl.Transport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test server
	if b, ct, err := api.FetchAvatar(t.Context(), cl, srv.URL+"/ok.png"); err != nil || ct != "image/png" || len(b) != len(png) {
		t.Fatalf("png: %v %s", err, ct)
	}
	if _, _, err := api.FetchAvatar(t.Context(), cl, srv.URL+"/html"); err == nil {
		t.Fatal("HTML served as image/png accepted")
	}
	if _, _, err := api.FetchAvatar(t.Context(), cl, srv.URL+"/big"); err == nil {
		t.Fatal("oversized avatar accepted")
	}
}

func TestMatchResultWebhook(t *testing.T) {
	opa := os.Getenv("LOCALARENA_TEST_OPA_URL")
	if opa == "" {
		t.Skip("LOCALARENA_TEST_OPA_URL not set")
	}
	dsn := testDSN(t, "localarena_apiwh")
	pool, _ := store.NewPool(t.Context(), dsn, 2)
	t.Cleanup(pool.Close)
	st := store.New(pool, nil)
	pid, _ := st.CreatePlayer(t.Context(), "wh_player", 0)
	secret := []byte("webhook-secret-webhook-secret-xx")
	idp := authtest.New(t)
	srv := api.New(st, slog.New(slog.DiscardHandler), api.Options{Auth: auth.NewVerifier(context.Background(), authtest.Issuer, idp.URL, authtest.Audience),
		Authz: authz.New(opa), WebhookSecrets: [][]byte{secret}})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	start := time.Now().Add(-10 * time.Minute).UTC()
	body := []byte(fmt.Sprintf(`{"started_at":%q,"ended_at":%q,"players":[{"player_id":%d,"score":9}]}`,
		start.Format(time.RFC3339), start.Add(5*time.Minute).Format(time.RFC3339), pid))
	post := func(sig, delivery string, b []byte) int {
		req, _ := http.NewRequestWithContext(t.Context(), "POST", ts.URL+"/v2/webhooks/match-results", bytes.NewReader(b))
		req.Header.Set(webhook.HeaderSignature, sig)
		req.Header.Set(webhook.HeaderDelivery, delivery)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	d := webhook.NewDeliveryID()
	sig := webhook.Sign(secret, time.Now(), d, body)
	if c := post(sig, d, body); c != 201 {
		t.Fatalf("first delivery: %d", c)
	}
	if c := post(sig, d, body); c != 200 { // replay inside the window: recognised, not re-applied
		t.Fatalf("replay: %d", c)
	}
	if c := post(sig, d, append([]byte(" "), body...)); c != 401 {
		t.Fatalf("tampered: %d", c)
	}
	d2 := webhook.NewDeliveryID()
	if c := post(webhook.Sign(secret, time.Now().Add(-10*time.Minute), d2, body), d2, body); c != 401 {
		t.Fatalf("stale signature: %d", c)
	}
	var n int
	_ = pool.QueryRow(t.Context(), `SELECT count(*) FROM match_players WHERE player_id = $1`, pid).Scan(&n)
	if n != 1 {
		t.Fatalf("match recorded %d times, want exactly 1", n)
	}
}
