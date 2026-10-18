// Package auth verifies Keycloak-issued access tokens and signs/verifies short-lived
// game tickets (adr/0013).
//
// Access tokens: RS256 JWTs checked against the realm JWKS. go-oidc's
// RemoteKeySet caches keys and refetches when a token carries an unknown
// `kid`, so signing-key rotation needs no restart. Issuer, audience
// ("localarena-api"), expiry and algorithm are all enforced.
//
// Game tickets: the gameserver has no database, so the API (which maps the
// token subject to a player and checks bans) mints an HMAC-SHA256 ticket
// {player id, expiry} valid for 60 s; the gameserver only checks the MAC.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Principal is the authenticated caller.
type Principal struct {
	Subject  string
	Username string
	Roles    []string
}

// HasRole reports whether the principal has a realm role.
func (p Principal) HasRole(r string) bool { return slices.Contains(p.Roles, r) }

// ErrUnauthenticated means no valid token was presented.
var ErrUnauthenticated = errors.New("unauthenticated")

// Verifier checks bearer tokens.
type Verifier struct{ v *oidc.IDTokenVerifier }

// NewVerifier verifies tokens from issuer, fetching keys from jwksURL (which
// may differ from the public issuer host inside a container network).
func NewVerifier(ctx context.Context, issuer, jwksURL, audience string) *Verifier {
	ks := oidc.NewRemoteKeySet(ctx, jwksURL)
	return NewVerifierWithKeySet(issuer, audience, ks)
}

// NewVerifierWithKeySet is NewVerifier with an explicit key set (tests).
func NewVerifierWithKeySet(issuer, audience string, ks oidc.KeySet) *Verifier {
	return &Verifier{v: oidc.NewVerifier(issuer, ks, &oidc.Config{ClientID: audience, SupportedSigningAlgs: []string{oidc.RS256}})}
}

// Verify parses and verifies a raw access token.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.v.Verify(ctx, raw)
	if err != nil {
		return Principal{}, errors.Join(ErrUnauthenticated, err)
	}
	var c struct {
		Username string `json:"preferred_username"`
		Realm    struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err := tok.Claims(&c); err != nil {
		return Principal{}, errors.Join(ErrUnauthenticated, err)
	}
	return Principal{Subject: tok.Subject, Username: c.Username, Roles: c.Realm.Roles}, nil
}

// BearerToken extracts "Authorization: Bearer <token>".
func BearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

type ctxKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the principal set by Middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// Middleware rejects requests without a valid token (401 + WWW-Authenticate).
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := v.Verify(r.Context(), BearerToken(r))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="localarena", error="invalid_token"`)
			http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
	})
}

// ---- game tickets ----

// TicketTTL is how long a game ticket is accepted.
const TicketTTL = 60 * time.Second

// ErrBadTicket covers malformed, forged and expired tickets.
var ErrBadTicket = errors.New("invalid game ticket")

// Tickets signs and verifies game tickets with a shared secret (from SOPS).
type Tickets struct{ key []byte }

// NewTickets requires a secret of at least 32 bytes.
func NewTickets(secret []byte) (*Tickets, error) {
	if len(secret) < 32 {
		return nil, errors.New("game ticket secret must be at least 32 bytes")
	}
	return &Tickets{key: secret}, nil
}

// Issue returns base64url(player_id(8) | expiry_unix(8) | hmac(32)).
func (t *Tickets) Issue(playerID int64, now time.Time) string {
	b := make([]byte, 16, 48)
	binary.BigEndian.PutUint64(b[0:], uint64(playerID))                  // #nosec G115 -- ids are positive
	binary.BigEndian.PutUint64(b[8:], uint64(now.Add(TicketTTL).Unix())) // #nosec G115 -- positive unix time
	m := hmac.New(sha256.New, t.key)
	m.Write(b)
	return base64.RawURLEncoding.EncodeToString(m.Sum(b))
}

// Verify returns the player ID if the ticket is authentic and unexpired.
func (t *Tickets) Verify(ticket string, now time.Time) (int64, error) {
	b, err := base64.RawURLEncoding.DecodeString(ticket)
	if err != nil || len(b) != 48 {
		return 0, ErrBadTicket
	}
	m := hmac.New(sha256.New, t.key)
	m.Write(b[:16])
	if !hmac.Equal(m.Sum(nil), b[16:]) { // constant-time compare
		return 0, ErrBadTicket
	}
	if now.Unix() > int64(binary.BigEndian.Uint64(b[8:])) { // #nosec G115 -- written from a positive int64
		return 0, ErrBadTicket
	}
	return int64(binary.BigEndian.Uint64(b[0:])), nil // #nosec G115 -- written from a positive int64
}
