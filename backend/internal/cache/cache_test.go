package cache_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/cache"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/ratelimit"
)

// Integration tests need a real Valkey: LOCALARENA_TEST_VALKEY_ADDR=localhost:6379.
func newCache(t *testing.T, opts cache.Options) *cache.Cache {
	t.Helper()
	opts.Addr = os.Getenv("LOCALARENA_TEST_VALKEY_ADDR")
	if opts.Addr == "" {
		t.Skip("LOCALARENA_TEST_VALKEY_ADDR not set")
	}
	c := cache.New(opts, slog.New(slog.DiscardHandler))
	t.Cleanup(c.Close)
	deadline := time.Now().Add(5 * time.Second)
	for c.Touch(t.Context(), "ping", "1", time.Second) != nil {
		if time.Now().After(deadline) {
			t.Fatal("valkey not reachable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c
}

var (
	seq atomic.Int64
	run = time.Now().UnixNano() // keys must be unique across test processes, not just within one
)

func key(t *testing.T) string { return fmt.Sprintf("test:%d:%s:%d", run, t.Name(), seq.Add(1)) }

func TestCacheAsideHitMiss(t *testing.T) {
	c := newCache(t, cache.Options{Singleflight: true})
	ctx, k := t.Context(), key(t)
	loads := 0
	load := func(context.Context) (int, error) { loads++; return 42, nil }
	for i, wantHit := range []bool{false, true, true} {
		v, hit, err := cache.GetOrLoad(ctx, c, k, time.Minute, load)
		if err != nil || v != 42 || hit != wantHit {
			t.Fatalf("call %d: v=%d hit=%v err=%v", i, v, hit, err)
		}
	}
	if loads != 1 {
		t.Fatalf("loads = %d", loads)
	}
	if err := c.Invalidate(ctx, k); err != nil {
		t.Fatal(err)
	}
	if _, hit, _ := cache.GetOrLoad(ctx, c, k, time.Minute, load); hit || loads != 2 {
		t.Fatalf("after invalidate: hit=%v loads=%d", hit, loads)
	}
}

// A fill that read the old value before a write committed must not overwrite
// the invalidation: the lease is gone, so the stale value is not stored.
func TestInvalidationBeatsInFlightFill(t *testing.T) {
	c := newCache(t, cache.Options{})
	ctx, k := t.Context(), key(t)
	inLoad, release := make(chan struct{}), make(chan struct{})
	done := make(chan int)
	go func() {
		v, _, _ := cache.GetOrLoad(ctx, c, k, time.Minute, func(context.Context) (int, error) {
			close(inLoad)
			<-release
			return 1, nil // the value as of before the write
		})
		done <- v
	}()
	<-inLoad
	if err := c.Invalidate(ctx, k); err != nil { // writer commits value 2, then invalidates
		t.Fatal(err)
	}
	close(release)
	if v := <-done; v != 1 {
		t.Fatalf("reader got %d", v)
	}
	v, hit, _ := cache.GetOrLoad(ctx, c, k, time.Minute, func(context.Context) (int, error) { return 2, nil })
	if hit || v != 2 {
		t.Fatalf("stale fill was cached: v=%d hit=%v", v, hit)
	}
}

// 100 concurrent misses on one hot key: singleflight collapses them to 1 load.
func TestStampedeGuard(t *testing.T) {
	for _, sf := range []bool{false, true} {
		t.Run(fmt.Sprintf("singleflight=%v", sf), func(t *testing.T) {
			c := newCache(t, cache.Options{Singleflight: sf})
			ctx, k := t.Context(), key(t)
			var loads atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for range 100 {
				wg.Go(func() {
					<-start
					_, _, _ = cache.GetOrLoad(ctx, c, k, time.Minute, func(context.Context) (int, error) {
						loads.Add(1)
						time.Sleep(50 * time.Millisecond) // a slow DB query
						return 1, nil
					})
				})
			}
			close(start)
			wg.Wait()
			t.Logf("singleflight=%v: %d loads for 100 concurrent misses", sf, loads.Load())
			if sf && loads.Load() != 1 {
				t.Fatalf("loads = %d, want 1", loads.Load())
			}
			if !sf && loads.Load() < 10 {
				t.Fatalf("loads = %d; expected a stampede without singleflight", loads.Load())
			}
		})
	}
}

// Slow Valkey (a busy Lua script beyond OpTimeout) trips the breaker; while
// open, calls fail in microseconds; after the open period one probe closes it.
func TestBreakerOpensAndRecovers(t *testing.T) {
	c := newCache(t, cache.Options{TripAfter: 3, BreakerOpen: 300 * time.Millisecond})
	ctx := t.Context()
	// TIME is frozen inside a script, so burn CPU with a counted loop (~150-300 ms).
	slow := `local x = 0 for i = 1, 30000000 do x = x + i end return 1`
	for range 3 {
		if _, err := c.Eval(ctx, "slow", slow, nil, nil); err == nil {
			t.Fatal("slow script should exceed OpTimeout")
		}
	}
	time.Sleep(200 * time.Millisecond) // let the last busy script finish
	start := time.Now()
	_, _, err := cache.GetOrLoad(ctx, c, key(t), time.Minute, func(context.Context) (int, error) { return 7, nil })
	if err != nil || time.Since(start) > 20*time.Millisecond {
		t.Fatalf("open breaker should fall through to load fast: %v in %v", err, time.Since(start))
	}
	if err := c.Touch(ctx, key(t), "x", time.Second); !errors.Is(err, cache.ErrUnavailable) {
		t.Fatalf("breaker should be open: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	if err := c.Touch(ctx, key(t), "x", time.Second); err != nil {
		t.Fatalf("half-open probe should close the breaker: %+v", errors.Unwrap(err))
	}
}

func TestCacheDownStillServes(t *testing.T) {
	c := cache.New(cache.Options{Addr: "127.0.0.1:1"}, slog.New(slog.DiscardHandler)) // nothing listens
	defer c.Close()
	v, hit, err := cache.GetOrLoad(t.Context(), c, "k", time.Minute, func(context.Context) (string, error) { return "db", nil })
	if err != nil || hit || v != "db" {
		t.Fatalf("v=%q hit=%v err=%v", v, hit, err)
	}
}

func TestTokenBucket(t *testing.T) {
	rule := ratelimit.Rule{Rate: 10, Burst: 5}
	check := func(t *testing.T, l *ratelimit.Limiter, backend string) {
		k := key(t)
		allowed := 0
		var last ratelimit.Decision
		for range 20 {
			if last = l.Allow(t.Context(), k, rule); last.Allowed {
				allowed++
			}
			if last.Backend != backend {
				t.Fatalf("backend = %s, want %s", last.Backend, backend)
			}
		}
		if allowed != 5 || last.RetryAfter <= 0 || last.RetryAfter > 150*time.Millisecond {
			t.Fatalf("allowed=%d retryAfter=%v", allowed, last.RetryAfter)
		}
		time.Sleep(220 * time.Millisecond) // refills ~2 tokens
		if !l.Allow(t.Context(), k, rule).Allowed {
			t.Fatal("bucket did not refill")
		}
	}
	t.Run("valkey", func(t *testing.T) { check(t, ratelimit.New(newCache(t, cache.Options{})), "valkey") })
	t.Run("local-fallback", func(t *testing.T) {
		c := cache.New(cache.Options{}, slog.New(slog.DiscardHandler)) // disabled
		check(t, ratelimit.New(c), "local")
	})
}

// Two limiters (two API replicas) share one Valkey bucket.
func TestTokenBucketIsGlobal(t *testing.T) {
	c := newCache(t, cache.Options{})
	a, b := ratelimit.New(c), ratelimit.New(c)
	k, allowed := key(t), 0
	for i := range 20 {
		l := a
		if i%2 == 1 {
			l = b
		}
		if l.Allow(t.Context(), k, ratelimit.Rule{Rate: 1, Burst: 6}).Allowed {
			allowed++
		}
	}
	if allowed != 6 {
		t.Fatalf("allowed %d across replicas, want 6", allowed)
	}
}
