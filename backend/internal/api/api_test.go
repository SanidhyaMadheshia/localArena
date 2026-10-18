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
	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
	"github.com/SanidhyaMadheshia/localArena/backend/migrations"
)

// End-to-end over HTTP against real Postgres. Needs LOCALARENA_TEST_DATABASE_URL.
func TestAPIPurchaseFlow(t *testing.T) {
	admin := os.Getenv("LOCALARENA_TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("LOCALARENA_TEST_DATABASE_URL not set")
	}
	ctx := t.Context()
	c, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`DROP DATABASE IF EXISTS localarena_apitest WITH (FORCE)`, `CREATE DATABASE localarena_apitest`} {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	_ = c.Close(ctx)
	cfg, _ := pgx.ParseConfig(admin)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/localarena_apitest", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	if err := migrations.Up(ctx, dsn); err != nil {
		t.Fatal(err)
	}
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
