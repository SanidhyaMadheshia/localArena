// Package authtest is a fake OIDC identity provider for tests: it serves a
// JWKS and mints RS256 access tokens shaped like Keycloak's.
package authtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Issuer and Audience used by tokens from IDP.
const (
	Issuer   = "http://idp.test/realms/localarena"
	Audience = "localarena-api"
)

// IDP serves one signing key.
type IDP struct {
	key *rsa.PrivateKey
	URL string // JWKS URL
}

// New starts an IDP for the test's lifetime.
func New(t testing.TB) *IDP {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	i := &IDP{key: k}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &k.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	}))
	t.Cleanup(srv.Close)
	i.URL = srv.URL
	return i
}

// Token mints an access token for sub with the given realm roles.
func (i *IDP) Token(t testing.TB, sub, username string, roles ...string) string {
	sig, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: i.key}, (&jose.SignerOptions{}).WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := jwt.Signed(sig).Claims(jwt.Claims{Issuer: Issuer, Subject: sub, Audience: jwt.Audience{Audience},
		Expiry: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)), IssuedAt: jwt.NewNumericDate(time.Now())}).
		Claims(map[string]any{"preferred_username": username, "realm_access": map[string]any{"roles": roles}}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return s
}
