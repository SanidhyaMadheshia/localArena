package store_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/store"
)

// TestExperimentPurchaseContention prints the strategy comparison used in
// Experiment 002. Run with LOCALARENA_EXPERIMENT=1 (it is slow on purpose).
func TestExperimentPurchaseContention(t *testing.T) {
	if os.Getenv("LOCALARENA_EXPERIMENT") == "" {
		t.Skip("set LOCALARENA_EXPERIMENT=1")
	}
	const buyers, stock = 200, 20
	fmt.Printf("| strategy | buyers | stock | sold | out_of_stock | other errors | final stock | wall ms | deadlock retries | optimistic retries |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range []store.Strategy{store.Naive, store.PessimisticForUpdate, store.Pessimistic, store.Optimistic, store.Conditional} {
		ctx := ctxT(t)
		item := newCosmetic(t, 10, ptr[int32](stock))
		ids := make([]int64, buyers)
		for i := range ids {
			ids[i] = newPlayer(t, 1000)
		}
		dl0 := testutil.ToFloat64(store.TxRetries.WithLabelValues("deadlock"))
		oc0 := testutil.ToFloat64(store.TxRetries.WithLabelValues("optimistic_conflict"))
		start := time.Now()
		errs := run(buyers, func(i int) error {
			_, err := st.Purchase(ctx, ids[i], item, "exp", s)
			return err
		})
		wall := time.Since(start)
		var sold, oos, other int
		for _, err := range errs {
			switch {
			case err == nil:
				sold++
			case errors.Is(err, store.ErrOutOfStock):
				oos++
			default:
				other++
			}
		}
		fmt.Printf("| %s | %d | %d | %d | %d | %d | %d | %d | %.0f | %.0f |\n", s, buyers, stock, sold, oos, other,
			scalar[int32](t, st.Primary, `SELECT stock FROM cosmetics WHERE id = $1`, item), wall.Milliseconds(),
			testutil.ToFloat64(store.TxRetries.WithLabelValues("deadlock"))-dl0,
			testutil.ToFloat64(store.TxRetries.WithLabelValues("optimistic_conflict"))-oc0)
	}
}
