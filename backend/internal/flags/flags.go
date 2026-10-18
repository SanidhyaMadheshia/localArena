// Package flags evaluates feature flags with the OpenFeature Remote Evaluation
// Protocol (OFREP) against flagd (adr/0015). Any error (flagd down, flag
// missing, wrong type, slow) returns the caller's default: a flag outage must
// never change or break gameplay, only freeze it at the safe default.
package flags

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var evals = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "localarena", Subsystem: "flags", Name: "evaluations_total",
	Help: "Flag evaluations by flag, variant served, and whether the default was used because of an error.",
}, []string{"flag", "value", "fallback"})

// Client evaluates flags. A nil *Client always returns defaults.
type Client struct {
	URL  string // flagd base URL, e.g. http://flagd:8016
	HTTP *http.Client
}

// New returns a client with a 100 ms budget per evaluation.
func New(base string) *Client {
	if base == "" {
		return nil
	}
	return &Client{URL: base, HTTP: &http.Client{Timeout: 100 * time.Millisecond}}
}

// String evaluates a string flag for targetingKey (e.g. a match id, so a
// match keeps one variant for its whole life when evaluated at creation).
func (c *Client) String(ctx context.Context, flag, targetingKey, def string) string {
	if c == nil {
		evals.WithLabelValues(flag, def, "true").Inc()
		return def
	}
	body, _ := json.Marshal(map[string]any{"context": map[string]string{"targetingKey": targetingKey}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/ofrep/v1/evaluate/flags/"+flag, bytes.NewReader(body))
	if err != nil {
		return c.fallback(flag, def)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return c.fallback(flag, def)
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Value any `json:"value"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 4096)).Decode(&out) != nil {
		return c.fallback(flag, def)
	}
	v, ok := out.Value.(string)
	if !ok {
		return c.fallback(flag, def)
	}
	evals.WithLabelValues(flag, v, "false").Inc()
	return v
}

func (c *Client) fallback(flag, def string) string {
	evals.WithLabelValues(flag, def, "true").Inc()
	return def
}
