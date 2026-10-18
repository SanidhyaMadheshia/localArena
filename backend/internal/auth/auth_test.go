package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const iss, aud = "http://idp.test/realms/localarena", "localarena-api"

type idp struct {
	mu   sync.Mutex
	keys map[string]*rsa.PrivateKey
	srv  *httptest.Server
}

func newIDP(t *testing.T, kids ...string) *idp {
	i := &idp{keys: map[string]*rsa.PrivateKey{}}
	for _, k := range kids {
		i.add(t, k)
	}
	i.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i.mu.Lock()
		defer i.mu.Unlock()
		var set jose.JSONWebKeySet
		for kid, k := range i.keys {
			set.Keys = append(set.Keys, jose.JSONWebKey{Key: &k.PublicKey, KeyID: kid, Algorithm: "RS256", Use: "sig"})
		}
		_ = json.NewEncoder(w).Encode(set)
	}))
	t.Cleanup(i.srv.Close)
	return i
}

func (i *idp) add(t *testing.T, kid string) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	i.mu.Lock()
	i.keys[kid] = k
	i.mu.Unlock()
}

func (i *idp) remove(kid string) { i.mu.Lock(); delete(i.keys, kid); i.mu.Unlock() }

func (i *idp) token(t *testing.T, kid string, mut func(*jwt.Claims, map[string]any)) string {
	i.mu.Lock()
	k := i.keys[kid]
	i.mu.Unlock()
	sig, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: k}, (&jose.SignerOptions{}).WithHeader("kid", kid))
	if err != nil {
		t.Fatal(err)
	}
	c := jwt.Claims{Issuer: iss, Subject: "sub-1", Audience: jwt.Audience{aud}, Expiry: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		IssuedAt: jwt.NewNumericDate(time.Now())}
	extra := map[string]any{"preferred_username": "alice", "realm_access": map[string]any{"roles": []string{"player"}}}
	if mut != nil {
		mut(&c, extra)
	}
	s, err := jwt.Signed(sig).Claims(c).Claims(extra).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestVerifyClaimsAndRejections(t *testing.T) {
	i := newIDP(t, "k1")
	v := NewVerifier(context.Background(), iss, i.srv.URL, aud)
	p, err := v.Verify(t.Context(), i.token(t, "k1", nil))
	if err != nil || p.Subject != "sub-1" || p.Username != "alice" || !p.HasRole("player") || p.HasRole("admin") {
		t.Fatalf("p=%+v err=%v", p, err)
	}
	for name, mut := range map[string]func(*jwt.Claims, map[string]any){
		"expired":      func(c *jwt.Claims, _ map[string]any) { c.Expiry = jwt.NewNumericDate(time.Now().Add(-time.Minute)) },
		"wrong issuer": func(c *jwt.Claims, _ map[string]any) { c.Issuer = "http://evil.test" },
		"wrong aud":    func(c *jwt.Claims, _ map[string]any) { c.Audience = jwt.Audience{"other-api"} },
	} {
		if _, err := v.Verify(t.Context(), i.token(t, "k1", mut)); err == nil {
			t.Errorf("%s token accepted", name)
		}
	}
	if _, err := v.Verify(t.Context(), "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4In0."); err == nil {
		t.Error("alg=none accepted")
	}
	other := newIDP(t, "k1") // same kid, attacker's key
	if _, err := v.Verify(t.Context(), other.token(t, "k1", nil)); err == nil {
		t.Error("token signed by a foreign key accepted")
	}
}

// Rotation: the IdP adds k2, signs with it, later retires k1. The verifier
// picks up k2 on the first unknown kid without a restart, and k1 tokens stop
// verifying once k1 is gone from the JWKS (after the cache refreshes).
func TestKeyRotation(t *testing.T) {
	i := newIDP(t, "k1")
	v := NewVerifier(context.Background(), iss, i.srv.URL, aud)
	old := i.token(t, "k1", nil)
	if _, err := v.Verify(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	i.add(t, "k2")
	if _, err := v.Verify(t.Context(), i.token(t, "k2", nil)); err != nil {
		t.Fatalf("token with new kid rejected: %v", err)
	}
	if _, err := v.Verify(t.Context(), old); err != nil {
		t.Fatalf("old key still published, must verify: %v", err)
	}
	i.remove("k1")
	i.add(t, "k3")
	_, _ = v.Verify(t.Context(), i.token(t, "k3", nil)) // forces a JWKS refresh
	if _, err := v.Verify(t.Context(), old); err == nil {
		t.Fatal("token signed with a retired key still accepted")
	}
}

func TestMiddleware(t *testing.T) {
	i := newIDP(t, "k1")
	v := NewVerifier(context.Background(), iss, i.srv.URL, aud)
	h := v.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := FromContext(r.Context())
		_, _ = w.Write([]byte(p.Subject))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 401 || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("no token: %d", rec.Code)
	}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+i.token(t, "k1", nil))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "sub-1" {
		t.Fatalf("valid token: %d %s", rec.Code, rec.Body)
	}
}

func TestTickets(t *testing.T) {
	if _, err := NewTickets([]byte("short")); err == nil {
		t.Fatal("short secret accepted")
	}
	tk, _ := NewTickets([]byte("0123456789abcdef0123456789abcdef"))
	now := time.Now()
	s := tk.Issue(42, now)
	if id, err := tk.Verify(s, now.Add(30*time.Second)); err != nil || id != 42 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	if _, err := tk.Verify(s, now.Add(TicketTTL+2*time.Second)); err == nil {
		t.Fatal("expired ticket accepted")
	}
	b := []byte(s)
	b[3] ^= 1
	if _, err := tk.Verify(string(b), now); err == nil {
		t.Fatal("tampered ticket accepted")
	}
	other, _ := NewTickets([]byte("ffffffffffffffffffffffffffffffff"))
	if _, err := other.Verify(s, now); err == nil {
		t.Fatal("ticket accepted with another secret")
	}
}
