// Package ratelimit is a token-bucket limiter shared by all API replicas via
// Valkey, with an in-process fallback when Valkey is unavailable.
//
// The bucket state (tokens, last refill time) lives in one Valkey hash and is
// updated atomically by a Lua script that reads the clock with TIME on the
// Valkey server, so replicas with skewed clocks still agree (adr/0008).
package ratelimit

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
)

// tokenBucket: KEYS[1]=bucket, ARGV = rate/s, burst, cost.
// Returns {allowed (0/1), tokens left, seconds until enough tokens}.
// Floats are returned as strings because Lua numbers become integers in replies.
// #nosec G101 -- false positive: Lua source; the word "tokens" trips the credential heuristic.
const tokenBucket = `
local rate, burst, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1]) or burst
local ts = tonumber(b[2]) or now
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate)
local allowed, wait = 0, 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  wait = (cost - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(burst / rate) + 1)
return {allowed, tostring(tokens), tostring(wait)}`

var decisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "localarena", Subsystem: "ratelimit", Name: "decisions_total",
	Help: "Rate-limit decisions by result and backend (valkey = global, local = per-replica fallback).",
}, []string{"result", "backend"})

// Rule is a bucket shape.
type Rule struct {
	Rate  float64 // tokens per second
	Burst int
}

// Limiter checks rules against keys.
type Limiter struct {
	c *cache.Cache

	mu    sync.Mutex
	local map[string]*rate.Limiter
}

// maxLocal bounds fallback memory: the map is reset when it grows past this.
// Resetting forgives clients (fails open), which is acceptable for a fallback.
const maxLocal = 10_000

// New returns a limiter using c (which may be disabled).
func New(c *cache.Cache) *Limiter { return &Limiter{c: c, local: map[string]*rate.Limiter{}} }

// Decision is the outcome of Allow.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Backend    string // "valkey" or "local"
}

// Allow takes one token for key under rule.
func (l *Limiter) Allow(ctx context.Context, key string, r Rule) Decision {
	if res, err := l.c.Eval(ctx, "ratelimit", tokenBucket, []string{"rl:" + key},
		[]string{strconv.FormatFloat(r.Rate, 'f', -1, 64), strconv.Itoa(r.Burst), "1"}); err == nil {
		if arr, err := res.ToArray(); err == nil && len(arr) == 3 {
			allowed, _ := arr[0].AsInt64()
			ws, _ := arr[2].ToString()
			wait, _ := strconv.ParseFloat(ws, 64)
			d := Decision{Allowed: allowed == 1, RetryAfter: time.Duration(wait * float64(time.Second)), Backend: "valkey"}
			decisions.WithLabelValues(result(d.Allowed), d.Backend).Inc()
			return d
		}
	}
	// Degraded: each replica enforces the limit on its own, so the effective
	// global limit is N x rule. Better than failing closed (blocking everyone)
	// or open (no protection at all).
	l.mu.Lock()
	lim, ok := l.local[key]
	if !ok {
		if len(l.local) >= maxLocal {
			l.local = map[string]*rate.Limiter{}
		}
		lim = rate.NewLimiter(rate.Limit(r.Rate), r.Burst)
		l.local[key] = lim
	}
	l.mu.Unlock()
	res := lim.Reserve()
	d := Decision{Allowed: res.Delay() == 0, Backend: "local"}
	if !d.Allowed {
		d.RetryAfter = res.Delay()
		res.Cancel() // we reject instead of waiting, so give the token back
	}
	decisions.WithLabelValues(result(d.Allowed), d.Backend).Inc()
	return d
}

func result(ok bool) string {
	if ok {
		return "allowed"
	}
	return "limited"
}
