// Package authz asks Open Policy Agent for authorization decisions (adr/0014).
//
// Fail closed: if OPA is unreachable, slow (>200 ms), or answers with anything
// but a well-formed decision, the request is denied with ErrUnavailable (the
// API answers 503, not 200 or 403). Policy lives in policies/*.rego, tested
// with `opa test`, and is the only place rules are written.
package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Errors.
var (
	ErrForbidden   = errors.New("forbidden")
	ErrUnavailable = errors.New("authorization unavailable")
)

var decisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "localarena", Subsystem: "authz", Name: "decisions_total",
	Help: "Authorization decisions by action and result (allow, deny, error).",
}, []string{"action", "result"})

// Subject is the caller.
type Subject struct {
	PlayerID int64    `json:"player_id"`
	Roles    []string `json:"roles"`
	Banned   bool     `json:"banned"`
}

// Resource is what is acted on.
type Resource struct {
	PlayerID int64 `json:"player_id"`
}

// Client queries OPA's data API.
type Client struct {
	URL  string // e.g. http://opa:8181/v1/data/localarena/authz/decision
	HTTP *http.Client
}

// New returns a client with a 200 ms budget per decision.
func New(base string) *Client {
	return &Client{URL: base + "/v1/data/localarena/authz/decision", HTTP: &http.Client{Timeout: 200 * time.Millisecond}}
}

// Check returns nil if allowed, ErrForbidden (with reason) if denied, or
// ErrUnavailable if no trustworthy decision could be obtained.
func (c *Client) Check(ctx context.Context, action string, sub Subject, res Resource) error {
	body, _ := json.Marshal(map[string]any{"input": map[string]any{"action": action, "subject": sub, "resource": res}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return fail(action, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fail(action, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fail(action, fmt.Errorf("opa status %d", resp.StatusCode))
	}
	var out struct {
		Result *struct {
			Allow  *bool  `json:"allow"`
			Reason string `json:"reason"`
		} `json:"result"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 4096)).Decode(&out); err != nil || out.Result == nil || out.Result.Allow == nil {
		return fail(action, errors.New("malformed or undefined decision")) // e.g. policy not loaded
	}
	if *out.Result.Allow {
		decisions.WithLabelValues(action, "allow").Inc()
		return nil
	}
	decisions.WithLabelValues(action, "deny").Inc()
	return fmt.Errorf("%w: %s", ErrForbidden, out.Result.Reason)
}

func fail(action string, err error) error {
	decisions.WithLabelValues(action, "error").Inc()
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}
