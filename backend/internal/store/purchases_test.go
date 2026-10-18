package store_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

var safeStrategies = []store.Strategy{store.Pessimistic, store.Optimistic, store.Conditional}

// run executes fn n times concurrently, released together by a barrier.
func run(n int, fn func(i int) error) []error {
	errs := make([]error, n)
	var ready, wg sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	for i := range n {
		wg.Go(func() {
			ready.Done()
			<-start
			errs[i] = fn(i)
		})
	}
	ready.Wait()
	close(start)
	wg.Wait()
	return errs
}

func tally(errs []error) map[string]int {
	m := map[string]int{}
	for _, e := range errs {
		if e == nil {
			m["ok"]++
		} else {
			m[e.Error()]++
		}
	}
	return m
}

// 50 buyers, 5 in stock: exactly 5 sales, stock 0, no extra inventory/outbox rows.
func TestLimitedStockNeverOversells(t *testing.T) {
	for _, s := range safeStrategies {
		t.Run(s.String(), func(t *testing.T) {
			ctx := ctxT(t)
			item := newCosmetic(t, 100, ptr[int32](5))
			buyers := make([]int64, 50)
			for i := range buyers {
				buyers[i] = newPlayer(t, 1000)
			}
			errs := run(len(buyers), func(i int) error {
				_, err := st.Purchase(ctx, buyers[i], item, fmt.Sprintf("k-%d", i), s)
				return err
			})
			got := tally(errs)
			if got["ok"] != 5 || got[store.ErrOutOfStock.Error()] != 45 {
				t.Fatalf("results = %v, want 5 ok / 45 out of stock", got)
			}
			if v := scalar[int32](t, st.Primary, `SELECT stock FROM cosmetics WHERE id = $1`, item); v != 0 {
				t.Fatalf("stock = %d", v)
			}
			if v := scalar[int64](t, st.Primary, `SELECT count(*) FROM inventory WHERE cosmetic_id = $1`, item); v != 5 {
				t.Fatalf("inventory rows = %d", v)
			}
			if v := scalar[int64](t, st.Primary, `SELECT count(*) FROM outbox WHERE (payload->>'cosmetic_id')::bigint = $1`, item); v != 5 {
				t.Fatalf("outbox rows = %d", v)
			}
			if v := scalar[int64](t, st.Primary, `SELECT sum(1000 - credits) FROM profiles WHERE player_id = ANY($1)`, buyers); v != 500 {
				t.Fatalf("credits debited = %d, want 500", v)
			}
		})
	}
}

// The naive strategy is expected to be wrong; this test records how wrong.
func TestNaiveStrategyLostUpdate(t *testing.T) {
	ctx := ctxT(t)
	item := newCosmetic(t, 100, ptr[int32](5))
	buyers := make([]int64, 50)
	for i := range buyers {
		buyers[i] = newPlayer(t, 1000)
	}
	errs := run(len(buyers), func(i int) error {
		_, err := st.Purchase(ctx, buyers[i], item, "k", store.Naive)
		return err
	})
	sold := tally(errs)["ok"]
	t.Logf("naive: %d sales for 5 units (stock now %d)", sold,
		scalar[int32](t, st.Primary, `SELECT stock FROM cosmetics WHERE id = $1`, item))
}

// One player, 250 credits, 10 concurrent different purchases at 100: exactly 2 succeed.
func TestCreditsNeverOverspent(t *testing.T) {
	for _, s := range safeStrategies {
		t.Run(s.String(), func(t *testing.T) {
			ctx := ctxT(t)
			player := newPlayer(t, 250)
			items := make([]int64, 10)
			for i := range items {
				items[i] = newCosmetic(t, 100, nil)
			}
			errs := run(len(items), func(i int) error {
				_, err := st.Purchase(ctx, player, items[i], fmt.Sprintf("k-%d", i), s)
				return err
			})
			if got := tally(errs); got["ok"] != 2 || got[store.ErrInsufficientCredits.Error()] != 8 {
				t.Fatalf("results = %v", got)
			}
			if c := scalar[int64](t, st.Primary, `SELECT credits FROM profiles WHERE player_id = $1`, player); c != 50 {
				t.Fatalf("credits = %d, want 50", c)
			}
		})
	}
}

// A retried request (same idempotency key) is applied exactly once.
func TestPurchaseIdempotency(t *testing.T) {
	ctx := ctxT(t)
	player := newPlayer(t, 1000)
	item := newCosmetic(t, 100, nil)
	receipts := make([]store.Receipt, 20)
	errs := run(20, func(i int) (err error) {
		receipts[i], err = st.Purchase(ctx, player, item, "retry-me", store.Conditional)
		return err
	})
	if got := tally(errs); got["ok"] != 20 {
		t.Fatalf("results = %v", got)
	}
	fresh := 0
	for _, r := range receipts {
		if !r.Replayed {
			fresh++
		}
		if r.PurchaseID != receipts[0].PurchaseID || r.Price != 100 {
			t.Fatalf("receipt mismatch: %+v vs %+v", r, receipts[0])
		}
	}
	if fresh != 1 {
		t.Fatalf("%d non-replayed receipts, want 1", fresh)
	}
	if c := scalar[int64](t, st.Primary, `SELECT credits FROM profiles WHERE player_id = $1`, player); c != 900 {
		t.Fatalf("credits = %d, want 900", c)
	}
	other := newCosmetic(t, 100, nil)
	if _, err := st.Purchase(ctx, player, other, "retry-me", store.Conditional); !errors.Is(err, store.ErrIdempotencyMismatch) {
		t.Fatalf("key reuse for another item: %v", err)
	}
	if _, err := st.Purchase(ctx, player, item, "second-key", store.Conditional); !errors.Is(err, store.ErrAlreadyOwned) {
		t.Fatalf("second purchase of owned item: %v", err)
	}
}
