// Package webhook signs and verifies match-result webhooks (adr/0015).
//
// Scheme (same shape as Stripe/GitHub-style signatures):
//
//	X-LocalArena-Delivery:  unique delivery id (receiver dedupes on it)
//	X-LocalArena-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<delivery>.<body>")>
//
// Verification: constant-time MAC compare over the exact raw body, timestamp
// within Tolerance (bounds the replay window), then the receiver must record
// the delivery id once (replays inside the window are rejected as duplicates).
// Several v1 values may be present during secret rotation; any match passes.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names.
const (
	HeaderSignature = "X-LocalArena-Signature"
	HeaderDelivery  = "X-LocalArena-Delivery"
	Tolerance       = 5 * time.Minute
)

// Verification errors.
var (
	ErrBadSignature = errors.New("webhook: bad signature")
	ErrStale        = errors.New("webhook: timestamp outside tolerance")
)

func mac(secret []byte, ts int64, delivery string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	_, _ = fmt.Fprintf(m, "%d.%s.", ts, delivery) // hash.Hash writes never fail
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// Sign returns the signature header value for body at time now.
func Sign(secret []byte, now time.Time, delivery string, body []byte) string {
	ts := now.Unix()
	return fmt.Sprintf("t=%d,v1=%s", ts, mac(secret, ts, delivery, body))
}

// Verify checks the header against body with any of the secrets (current and,
// during rotation, previous).
func Verify(header, delivery string, body []byte, now time.Time, secrets ...[]byte) error {
	var ts int64 = -1
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return ErrBadSignature
			}
			ts = n
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if ts < 0 || len(sigs) == 0 || delivery == "" || len(delivery) > 64 {
		return ErrBadSignature
	}
	if d := now.Sub(time.Unix(ts, 0)); d > Tolerance || d < -Tolerance {
		return ErrStale
	}
	for _, sec := range secrets {
		want := mac(sec, ts, delivery, body)
		for _, s := range sigs {
			if hmac.Equal([]byte(s), []byte(want)) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

// NewDeliveryID returns a random 128-bit id.
func NewDeliveryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Send POSTs a signed JSON body. Retries (by the caller) must reuse delivery,
// so the receiver can dedupe a delivery that succeeded but whose reply was lost.
func Send(ctx context.Context, cl *http.Client, url string, secret []byte, delivery string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderDelivery, delivery)
	req.Header.Set(HeaderSignature, Sign(secret, time.Now(), delivery, body))
	return cl.Do(req)
}
