package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/api"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/ratelimit"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

// testDSN creates a fresh migrated database for one test.
func testDSN(t *testing.T, name string) string {
	t.Helper()
	admin := os.Getenv("LOCALARENA_TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("LOCALARENA_TEST_DATABASE_URL not set")
	}
	ctx := t.Context()
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`, `CREATE DATABASE ` + name} {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	_ = c.Close(ctx)
	cfg, _ := pgx.ParseConfig(admin)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, name)
	if err := migrations.Up(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	return dsn
}

type client struct {
	t   *testing.T
	url string
}

func (c client) do(method, path, body string, hdr map[string]string) (int, map[string]any, http.Header) {
	c.t.Helper()
	req, _ := http.NewRequestWithContext(c.t.Context(), method, c.url+path, bytes.NewBufferString(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
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

func newServer(t *testing.T, db string, opts api.Options) (client, *api.Server) {
	t.Helper()
	dsn := testDSN(t, db)
	pool, err := store.NewPool(t.Context(), dsn, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	srv := api.New(store.New(pool, nil), slog.New(slog.DiscardHandler), opts)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return client{t, ts.URL}, srv
}

// End-to-end over HTTP against real Postgres. Needs LOCALARENA_TEST_DATABASE_URL.
func TestAPIPurchaseFlow(t *testing.T) {
	ctx := t.Context()
	dsn := testDSN(t, "localarena_apitest")
	pool, err := store.NewPool(ctx, dsn, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ts := httptest.NewServer(api.New(store.New(pool, nil), slog.New(slog.DiscardHandler)).Handler())
	defer ts.Close()

	do := func(method, path, body string, hdr map[string]string) (int, map[string]any, http.Header) {
		req, _ := http.NewRequestWithContext(ctx, method, ts.URL+path, bytes.NewBufferString(body))
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return resp.StatusCode, m, resp.Header
	}

	code, m, h := do("POST", "/v1/players", `{"username":"alice"}`, nil)
	if code != 201 || h.Get(api.HeaderWriteLSN) == "" {
		t.Fatalf("create player: %d %v", code, m)
	}
	id := int64(m["id"].(float64))
	if code, _, _ = do("POST", "/v1/players", `{"username":"alice"}`, nil); code != 409 {
		t.Fatalf("duplicate username: %d", code)
	}
	ik := map[string]string{"Idempotency-Key": "create-bob"}
	_, m1, _ := do("POST", "/v1/players", `{"username":"bob"}`, ik)
	if code, m2, _ := do("POST", "/v1/players", `{"username":"bob"}`, ik); code != 200 || m1["id"] != m2["id"] {
		t.Fatalf("retried create: %d %v vs %v", code, m2, m1)
	}
	body := fmt.Sprintf(`{"player_id":%d,"cosmetic_id":3}`, id) // skin-legend: price 900, stock 1
	if code, _, _ = do("POST", "/v1/purchases", body, nil); code != 400 {
		t.Fatalf("missing Idempotency-Key: %d", code)
	}
	key := map[string]string{"Idempotency-Key": "buy-1"}
	if code, m, _ = do("POST", "/v1/purchases", body, key); code != 201 || m["replayed"] != false {
		t.Fatalf("purchase: %d %v", code, m)
	}
	if code, m, _ = do("POST", "/v1/purchases", body, key); code != 200 || m["replayed"] != true {
		t.Fatalf("replayed purchase: %d %v", code, m)
	}
	if code, _, _ = do("POST", "/v1/purchases", fmt.Sprintf(`{"player_id":%d,"cosmetic_id":2}`, id), map[string]string{"Idempotency-Key": "buy-2"}); code != 402 {
		t.Fatalf("insufficient credits: %d", code)
	}
	code, m, _ = do("GET", fmt.Sprintf("/v1/players/%d", id), "", nil)
	if code != 200 || m["credits"].(float64) != 100 || len(m["items"].([]any)) != 1 {
		t.Fatalf("profile: %d %v", code, m)
	}
	start := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	match := fmt.Sprintf(`{"started_at":%q,"ended_at":%q,"players":[{"player_id":%d,"score":7}]}`,
		start.Format(time.RFC3339), start.Add(5*time.Minute).Format(time.RFC3339), id)
	if code, _, _ = do("POST", "/v1/matches", match, nil); code != 201 {
		t.Fatalf("record match: %d", code)
	}
	if code, _, _ = do("POST", "/v1/lobbies", fmt.Sprintf(`{"owner_id":%d,"capacity":4}`, id), nil); code != 201 {
		t.Fatalf("create lobby: %d", code)
	}
	if code, _, _ = do("POST", "/v1/lobbies", fmt.Sprintf(`{"owner_id":%d,"capacity":4}`, id), nil); code != 409 {
		t.Fatalf("second lobby for same owner: %d", code)
	}
	if code, _, _ = do("POST", "/v1/players", `{"username":"x","extra":1}`, nil); code != 400 {
		t.Fatalf("unknown field accepted: %d", code)
	}
}

func valkeyCache(t *testing.T, addr string) *cache.Cache {
	c := cache.New(cache.Options{Addr: addr, Singleflight: true}, slog.New(slog.DiscardHandler))
	t.Cleanup(c.Close)
	return c
}

// With Valkey: profile reads are cached, a purchase invalidates them, and
// rate limits and presence work. Needs LOCALARENA_TEST_VALKEY_ADDR.
func TestAPICacheInvalidation(t *testing.T) {
	addr := os.Getenv("LOCALARENA_TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("LOCALARENA_TEST_VALKEY_ADDR not set")
	}
	c := valkeyCache(t, addr)
	for c.Touch(t.Context(), "ping", "1", time.Second) != nil {
		time.Sleep(10 * time.Millisecond)
	}
	cl, _ := newServer(t, "localarena_apicache", api.Options{Cache: c, PurchaseRL: ratelimit.Rule{Rate: 0.1, Burst: 2}})
	name := fmt.Sprintf("cache_%d", time.Now().UnixNano()%1e9)
	_, m, _ := cl.do("POST", "/v1/players", `{"username":"`+name+`"}`, nil)
	id := int64(m["id"].(float64))
	path := fmt.Sprintf("/v1/players/%d", id)
	for i, want := range []string{"primary", "cache"} {
		if _, _, h := cl.do("GET", path, "", nil); h.Get("X-Served-By") != want {
			t.Fatalf("read %d served by %q, want %q", i, h.Get("X-Served-By"), want)
		}
	}
	buy := fmt.Sprintf(`{"player_id":%d,"cosmetic_id":1}`, id)
	if code, _, _ := cl.do("POST", "/v1/purchases", buy, map[string]string{"Idempotency-Key": "a"}); code != 201 {
		t.Fatalf("purchase: %d", code)
	}
	code, m, h := cl.do("GET", path, "", nil)
	if code != 200 || h.Get("X-Served-By") != "primary" || m["credits"].(float64) != 900 {
		t.Fatalf("after purchase: %d %s %v (cache not invalidated?)", code, h.Get("X-Served-By"), m)
	}
	// Burst 2: the replayed request and one more pass, the next is limited.
	_, _, _ = cl.do("POST", "/v1/purchases", buy, map[string]string{"Idempotency-Key": "a"})
	code, _, h = cl.do("POST", "/v1/purchases", buy, map[string]string{"Idempotency-Key": "a"})
	if code != 429 || h.Get("Retry-After") == "" || h.Get("X-RateLimit-Backend") != "valkey" {
		t.Fatalf("rate limit: %d retry-after=%q backend=%q", code, h.Get("Retry-After"), h.Get("X-RateLimit-Backend"))
	}
	if code, _, _ := cl.do("PUT", path+"/presence", "", nil); code != 204 {
		t.Fatalf("heartbeat: %d", code)
	}
	if _, m, _ := cl.do("GET", fmt.Sprintf("/v1/presence?ids=%d,999999", id), "", nil); m[fmt.Sprint(id)] != "online" || m["999999"] != "offline" {
		t.Fatalf("presence: %v", m)
	}
}

// Valkey unreachable: reads and writes still work from Postgres, rate limits
// fall back to per-replica, presence degrades honestly.
func TestAPIWithValkeyDown(t *testing.T) {
	cl, srv := newServer(t, "localarena_apinocache", api.Options{Cache: valkeyCache(t, "127.0.0.1:1")})
	code, m, h := cl.do("POST", "/v1/players", `{"username":"nocache"}`, nil)
	if code != 201 || h.Get("X-RateLimit-Backend") != "local" {
		t.Fatalf("create: %d backend=%q", code, h.Get("X-RateLimit-Backend"))
	}
	id := int64(m["id"].(float64))
	if code, _, h := cl.do("GET", fmt.Sprintf("/v1/players/%d", id), "", nil); code != 200 || h.Get("X-Served-By") != "primary" {
		t.Fatalf("profile: %d %s", code, h.Get("X-Served-By"))
	}
	if code, _, h := cl.do("PUT", fmt.Sprintf("/v1/players/%d/presence", id), "", nil); code != 503 || h.Get("X-Envoy-Ratelimited") == "" {
		t.Fatalf("heartbeat with cache down: %d (must carry x-envoy-ratelimited so the proxy does not retry)", code)
	}
	if _, m, _ := cl.do("GET", fmt.Sprintf("/v1/presence?ids=%d", id), "", nil); m[fmt.Sprint(id)] != "unknown" {
		t.Fatalf("presence: %v", m)
	}
	if code, _, _ := cl.do("GET", "/readyz", "", nil); code != 200 {
		t.Fatalf("readyz: %d", code)
	}
	srv.Drain()
	if code, _, _ := cl.do("GET", "/readyz", "", nil); code != 503 {
		t.Fatalf("readyz while draining: %d", code)
	}
}

// A database outage must not make the replica unready (that would remove every
// replica from the load balancer at once); only draining does.
func TestReadyzIgnoresSharedDependencies(t *testing.T) {
	pool, err := store.NewPool(t.Context(), "postgres://x:y@127.0.0.1:1/none?connect_timeout=1", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	srv := api.New(store.New(pool, nil), slog.New(slog.DiscardHandler))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	cl := client{t, ts.URL}
	if code, _, _ := cl.do("GET", "/readyz", "", nil); code != 200 {
		t.Fatalf("readyz with DB down = %d, want 200 (degraded)", code)
	}
	srv.Drain()
	if code, _, _ := cl.do("GET", "/readyz", "", nil); code != 503 {
		t.Fatalf("readyz while draining = %d", code)
	}
}
