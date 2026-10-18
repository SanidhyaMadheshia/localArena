// Package cache wraps Valkey for LocalArena: cache-aside with lease-protected
// fills, presence keys, and Lua scripts, all behind a circuit breaker.
//
// Valkey is disposable acceleration (README runtime invariants): every caller
// must have a fallback, and no operation may spend more than OpTimeout of a
// request's budget. When Valkey is slow or down the breaker opens after 5
// consecutive failures and calls fail immediately with ErrUnavailable for 5 s,
// then a single probe decides whether to close again (adr/0008).
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker/v2"
	"github.com/valkey-io/valkey-go"
	"golang.org/x/sync/singleflight"
)

// ErrUnavailable means Valkey could not be used: disabled, not connected,
// breaker open, or the operation failed. Callers must degrade, never fail.
var ErrUnavailable = errors.New("cache unavailable")

// OpTimeout bounds every Valkey call. A healthy local Valkey answers in well
// under 1 ms; anything slower is treated as a failure.
const OpTimeout = 100 * time.Millisecond

var (
	ops = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "localarena", Subsystem: "cache", Name: "ops_total",
		Help: "Cache operations by op and result (hit, miss, ok, error, rejected).",
	}, []string{"op", "result"})
	breakerState = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "localarena", Subsystem: "cache", Name: "breaker_state",
		Help: "Valkey circuit breaker: 0 closed, 1 half-open, 2 open.",
	})
	fills = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "localarena", Subsystem: "cache", Name: "fills_total",
		Help: "Cache-aside loads from the source of truth, by outcome.",
	}, []string{"result"}) // loaded, shared (singleflight), stored, lease_lost
)

// Options configures the cache.
type Options struct {
	Addr         string        // host:port; empty disables the cache
	Singleflight bool          // collapse concurrent misses for one key in-process
	BreakerOpen  time.Duration // how long the breaker stays open (default 5 s)
	TripAfter    uint32        // consecutive failures that open the breaker (default 5)
}

// Cache is safe for concurrent use. A nil or disabled Cache always misses.
type Cache struct {
	opts    Options
	client  atomic.Pointer[valkey.Client]
	breaker *gobreaker.CircuitBreaker[valkey.ValkeyResult]
	group   singleflight.Group
	log     *slog.Logger
	stop    chan struct{}
}

// New returns a cache and connects in the background, retrying until Valkey
// is reachable, so the API starts (degraded) even when Valkey is down.
func New(opts Options, log *slog.Logger) *Cache {
	if opts.BreakerOpen <= 0 {
		opts.BreakerOpen = 5 * time.Second
	}
	if opts.TripAfter == 0 {
		opts.TripAfter = 5
	}
	c := &Cache{opts: opts, log: log, stop: make(chan struct{})}
	c.breaker = gobreaker.NewCircuitBreaker[valkey.ValkeyResult](gobreaker.Settings{
		Name:        "valkey",
		MaxRequests: 1, // half-open: one probe
		Timeout:     opts.BreakerOpen,
		ReadyToTrip: func(n gobreaker.Counts) bool { return n.ConsecutiveFailures >= opts.TripAfter },
		IsSuccessful: func(err error) bool {
			// A miss is a healthy answer; a caller giving up is not Valkey's fault.
			return err == nil || valkey.IsValkeyNil(err) || errors.Is(err, context.Canceled)
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			breakerState.Set(float64(to))
			log.Warn("cache breaker state change", "from", from.String(), "to", to.String())
		},
	})
	if opts.Addr != "" {
		go c.connect()
	}
	return c
}

func (c *Cache) connect() {
	for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, 5*time.Second) {
		cl, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:      []string{c.opts.Addr},
			DisableCache:     true, // explicit invalidation only; no client-side tracking
			Dialer:           net.Dialer{Timeout: 500 * time.Millisecond},
			ConnWriteTimeout: time.Second,
		})
		if err == nil {
			c.client.Store(&cl)
			c.log.Info("cache connected", "addr", c.opts.Addr)
			return
		}
		c.log.Warn("cache connect failed; running degraded", "err", err, "retry_in", backoff)
		select {
		case <-time.After(backoff):
		case <-c.stop:
			return
		}
	}
}

// Close releases the client.
func (c *Cache) Close() {
	if c == nil {
		return
	}
	close(c.stop)
	if cl := c.client.Load(); cl != nil {
		(*cl).Close()
	}
}

// Enabled reports whether a Valkey address is configured.
func (c *Cache) Enabled() bool { return c != nil && c.opts.Addr != "" }

// do runs one command through the breaker with OpTimeout.
func (c *Cache) do(ctx context.Context, op string, build func(valkey.Client) valkey.Completed) (valkey.ValkeyResult, error) {
	if !c.Enabled() {
		return valkey.ValkeyResult{}, ErrUnavailable
	}
	cl := c.client.Load()
	if cl == nil {
		ops.WithLabelValues(op, "rejected").Inc()
		return valkey.ValkeyResult{}, ErrUnavailable
	}
	res, err := c.breaker.Execute(func() (valkey.ValkeyResult, error) {
		ctx, cancel := context.WithTimeout(ctx, OpTimeout)
		defer cancel()
		r := (*cl).Do(ctx, build(*cl))
		return r, r.Error()
	})
	switch {
	case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
		ops.WithLabelValues(op, "rejected").Inc()
		return res, ErrUnavailable
	case valkey.IsValkeyNil(err):
		return res, err
	case err != nil:
		ops.WithLabelValues(op, "error").Inc()
		return res, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return res, nil
}

// Del removes keys (used for invalidation after a committed write).
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	_, err := c.do(ctx, "del", func(cl valkey.Client) valkey.Completed { return cl.B().Del().Key(keys...).Build() })
	if err == nil {
		ops.WithLabelValues("del", "ok").Inc()
	}
	return err
}

// Invalidate deletes a cache-aside key and its fill lease, so an in-flight
// fill that read the old value cannot store it (see GetOrLoad).
func (c *Cache) Invalidate(ctx context.Context, key string) error {
	return c.Del(ctx, key, leaseKey(key))
}

func leaseKey(key string) string { return key + ":lease" }

// fillScript stores the value only if this caller still holds the lease.
// A writer's Invalidate deletes the lease, which makes a racing stale fill a no-op.
const fillScript = `
if redis.call('GET', KEYS[2]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  redis.call('DEL', KEYS[2])
  return 1
end
return 0`

// GetOrLoad implements cache-aside for JSON values.
//
//  1. GET key: hit -> decode into dst.
//  2. Miss: take a lease (SET key:lease token NX PX 2000) and load from the
//     source of truth. With Singleflight, concurrent misses in this process
//     share one load (stampede guard).
//  3. Store via fillScript only if the lease survived; a concurrent
//     Invalidate means our value may be stale, so we return it but don't cache it.
//
// Any cache failure falls through to load(): the cache can make requests
// faster, never fail them. Returns whether the value came from the cache.
func GetOrLoad[T any](ctx context.Context, c *Cache, key string, ttl time.Duration, load func(context.Context) (T, error)) (T, bool, error) {
	var zero T
	if !c.Enabled() {
		v, err := load(ctx)
		return v, false, err
	}
	r, err := c.do(ctx, "get", func(cl valkey.Client) valkey.Completed { return cl.B().Get().Key(key).Build() })
	if err == nil {
		if b, berr := r.AsBytes(); berr == nil {
			var v T
			if json.Unmarshal(b, &v) == nil {
				ops.WithLabelValues("get", "hit").Inc()
				return v, true, nil
			}
		}
	}
	if valkey.IsValkeyNil(err) {
		ops.WithLabelValues("get", "miss").Inc()
	}
	cacheUsable := err == nil || valkey.IsValkeyNil(err)

	fill := func() (any, error) {
		token := ""
		if cacheUsable {
			token = newToken()
			if _, lerr := c.do(ctx, "lease", func(cl valkey.Client) valkey.Completed {
				return cl.B().Set().Key(leaseKey(key)).Value(token).Nx().PxMilliseconds(2000).Build()
			}); lerr != nil {
				token = "" // someone else holds the lease (nil reply) or cache failed: load without storing
			}
		}
		v, lerr := load(ctx)
		fills.WithLabelValues("loaded").Inc()
		if lerr != nil || token == "" {
			return v, lerr
		}
		b, _ := json.Marshal(v)
		res, serr := c.do(ctx, "fill", func(cl valkey.Client) valkey.Completed {
			return cl.B().Eval().Script(fillScript).Numkeys(2).Key(key, leaseKey(key)).
				Arg(token, string(b), strconv.FormatInt(ttl.Milliseconds(), 10)).Build()
		})
		if serr == nil {
			if n, _ := res.AsInt64(); n == 1 {
				fills.WithLabelValues("stored").Inc()
			} else {
				fills.WithLabelValues("lease_lost").Inc()
			}
		}
		return v, nil
	}
	var v any
	if c.opts.Singleflight {
		var shared bool
		v, err, shared = c.group.Do(key, fill)
		if shared {
			fills.WithLabelValues("shared").Inc()
		}
	} else {
		v, err = fill()
	}
	if err != nil {
		return zero, false, err
	}
	return v.(T), false, nil
}

func newToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Eval runs a Lua script through the breaker.
func (c *Cache) Eval(ctx context.Context, op, script string, keys, args []string) (valkey.ValkeyResult, error) {
	r, err := c.do(ctx, op, func(cl valkey.Client) valkey.Completed {
		return cl.B().Eval().Script(script).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
	})
	if err == nil {
		ops.WithLabelValues(op, "ok").Inc()
	}
	return r, err
}

// Touch sets key with a TTL (presence heartbeats).
func (c *Cache) Touch(ctx context.Context, key, value string, ttl time.Duration) error {
	_, err := c.do(ctx, "touch", func(cl valkey.Client) valkey.Completed {
		return cl.B().Set().Key(key).Value(value).Px(ttl).Build()
	})
	if err == nil {
		ops.WithLabelValues("touch", "ok").Inc()
	}
	return err
}

// Exists returns, for each key, whether it currently exists (presence lookup).
func (c *Cache) Exists(ctx context.Context, keys []string) ([]bool, error) {
	r, err := c.do(ctx, "mget", func(cl valkey.Client) valkey.Completed { return cl.B().Mget().Key(keys...).Build() })
	if err != nil {
		return nil, err
	}
	vals, err := r.ToArray()
	if err != nil {
		return nil, errors.Join(ErrUnavailable, err)
	}
	out := make([]bool, len(vals))
	for i := range vals {
		out[i] = !vals[i].IsNil()
	}
	ops.WithLabelValues("mget", "ok").Inc()
	return out, nil
}
