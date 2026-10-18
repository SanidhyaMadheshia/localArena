package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// Strategy selects how a purchase protects stock and credits (Experiment 002).
type Strategy int

const (
	// Naive reads, checks in Go, and writes back computed values. It has a
	// lost-update race and exists only to demonstrate the bug.
	Naive Strategy = iota
	// Pessimistic locks the cosmetic then the profile with SELECT ... FOR NO KEY UPDATE.
	Pessimistic
	// Optimistic writes with "WHERE version = $read" and re-runs on conflict.
	Optimistic
	// Conditional uses single guarded UPDATEs ("stock > 0", "credits >= price").
	Conditional
	// PessimisticForUpdate is Pessimistic with plain FOR UPDATE. Kept to
	// reproduce the foreign-key lock-upgrade deadlock in Experiment 002.
	PessimisticForUpdate
)

var strategyNames = []string{"naive", "pessimistic", "optimistic", "conditional", "pessimistic-forupdate"}

func (s Strategy) String() string { return strategyNames[s] }

// ParseStrategy parses a strategy name.
func ParseStrategy(name string) (Strategy, error) {
	if i := slices.Index(strategyNames, name); i >= 0 {
		return Strategy(i), nil
	}
	return 0, fmt.Errorf("unknown strategy %q", name)
}

// Receipt is the result of a purchase. Replayed is true when the idempotency
// key had already been used and the original result is returned unchanged.
type Receipt struct {
	PurchaseID int64 `json:"purchase_id"`
	CosmeticID int64 `json:"cosmetic_id"`
	Price      int64 `json:"price"`
	Replayed   bool  `json:"replayed"`
}

// Purchase buys one cosmetic with fictional credits. Atomically, in one
// transaction: claim the idempotency key, decrement stock, debit credits,
// grant the item, and write an outbox event. Business failures roll the whole
// transaction back, so the key is not consumed and a retry may succeed later.
func (s *Store) Purchase(ctx context.Context, playerID, cosmeticID int64, key string, st Strategy) (Receipt, error) {
	var r Receipt
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r = Receipt{CosmeticID: cosmeticID}
		// 1. Claim the key. A concurrent request with the same key blocks on the
		// unique index until we commit, then takes the replay path below.
		err := tx.QueryRow(ctx, `
			INSERT INTO purchases (player_id, cosmetic_id, price, idempotency_key) VALUES ($1, $2, 0, $3)
			ON CONFLICT (player_id, idempotency_key) DO NOTHING RETURNING id`, playerID, cosmeticID, key).Scan(&r.PurchaseID)
		if errors.Is(err, pgx.ErrNoRows) {
			r.Replayed = true
			var orig int64
			if err := tx.QueryRow(ctx, `SELECT id, cosmetic_id, price FROM purchases WHERE player_id = $1 AND idempotency_key = $2`,
				playerID, key).Scan(&r.PurchaseID, &orig, &r.Price); err != nil {
				return err
			}
			if orig != cosmeticID {
				return ErrIdempotencyMismatch
			}
			return nil
		}
		if err != nil {
			return err
		}
		// 2. Stock and credits.
		if r.Price, err = debit(ctx, tx, playerID, cosmeticID, st); err != nil {
			return err
		}
		// 3. Grant. The (player_id, cosmetic_id) primary key rejects duplicates.
		if _, err := tx.Exec(ctx, `INSERT INTO inventory (player_id, cosmetic_id) VALUES ($1, $2)`, playerID, cosmeticID); err != nil {
			if pgCode(err) == codeUniqueViolation {
				return ErrAlreadyOwned
			}
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE purchases SET price = $2 WHERE id = $1`, r.PurchaseID, r.Price); err != nil {
			return err
		}
		// 4. Outbox: committed atomically with the state change (published by Debezium in week 4).
		payload, _ := json.Marshal(map[string]any{"purchase_id": r.PurchaseID, "player_id": playerID,
			"cosmetic_id": cosmeticID, "price": r.Price, "idempotency_key": key})
		_, err = tx.Exec(ctx, `INSERT INTO outbox (aggregatetype, aggregateid, type, payload) VALUES ('player', $1, 'CosmeticPurchased', $2)`,
			strconv.FormatInt(playerID, 10), payload)
		return err
	})
	if pgCode(err) == codeCheckViolation {
		// A CHECK constraint caught what the strategy missed (only possible for Naive).
		if constraintOf(err) == "cosmetics_stock_check" {
			return r, ErrOutOfStock
		}
		return r, ErrInsufficientCredits
	}
	return r, err
}

func debit(ctx context.Context, tx pgx.Tx, playerID, cosmeticID int64, st Strategy) (int64, error) {
	var (
		price, credits, cver, pver int64
		stock                      *int32
	)
	readCosmetic := func(sql string) error {
		err := tx.QueryRow(ctx, sql, cosmeticID).Scan(&price, &stock, &cver)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	readProfile := func(sql string) error {
		err := tx.QueryRow(ctx, sql, playerID).Scan(&credits, &pver)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	check := func() error {
		if stock != nil && *stock <= 0 {
			return ErrOutOfStock
		}
		if credits < price {
			return ErrInsufficientCredits
		}
		return nil
	}

	switch st {
	case Naive, Pessimistic, PessimisticForUpdate:
		lock := ""
		if st == PessimisticForUpdate {
			lock = " FOR UPDATE"
		}
		if st == Pessimistic {
			// Fixed order (cosmetic, then profile) avoids lock-order deadlocks.
			// NO KEY UPDATE, not UPDATE: the earlier INSERT INTO purchases already
			// holds FOR KEY SHARE on the cosmetic via its foreign key, and so does
			// every concurrent buyer. FOR UPDATE conflicts with KEY SHARE, turning
			// every buyer into a lock-upgrade deadlock (Experiment 002).
			lock = " FOR NO KEY UPDATE"
		}
		if err := readCosmetic(`SELECT price, stock, version FROM cosmetics WHERE id = $1` + lock); err != nil {
			return 0, err
		}
		if err := readProfile(`SELECT credits, version FROM profiles WHERE player_id = $1` + lock); err != nil {
			return 0, err
		}
		if err := check(); err != nil {
			return 0, err
		}
		if stock != nil {
			// Naive: writes a value computed from a possibly stale read (lost update).
			if _, err := tx.Exec(ctx, `UPDATE cosmetics SET stock = $2, version = version + 1 WHERE id = $1`, cosmeticID, *stock-1); err != nil {
				return 0, err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE profiles SET credits = $2, version = version + 1, updated_at = now() WHERE player_id = $1`,
			playerID, credits-price)
		return price, err

	case Optimistic:
		if err := readCosmetic(`SELECT price, stock, version FROM cosmetics WHERE id = $1`); err != nil {
			return 0, err
		}
		if err := readProfile(`SELECT credits, version FROM profiles WHERE player_id = $1`); err != nil {
			return 0, err
		}
		if err := check(); err != nil {
			return 0, err
		}
		if stock != nil {
			tag, err := tx.Exec(ctx, `UPDATE cosmetics SET stock = stock - 1, version = version + 1 WHERE id = $1 AND version = $2`, cosmeticID, cver)
			if err != nil {
				return 0, err
			}
			if tag.RowsAffected() == 0 {
				return 0, errRetry
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE profiles SET credits = credits - $2, version = version + 1, updated_at = now()
			WHERE player_id = $1 AND version = $3`, playerID, price, pver)
		if err != nil {
			return 0, err
		}
		if tag.RowsAffected() == 0 {
			return 0, errRetry
		}
		return price, nil

	case Conditional:
		err := tx.QueryRow(ctx, `UPDATE cosmetics SET stock = stock - 1, version = version + 1
			WHERE id = $1 AND (stock IS NULL OR stock > 0) RETURNING price`, cosmeticID).Scan(&price)
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cosmetics WHERE id = $1)`, cosmeticID).Scan(&exists); err != nil {
				return 0, err
			}
			if !exists {
				return 0, ErrNotFound
			}
			return 0, ErrOutOfStock
		}
		if err != nil {
			return 0, err
		}
		err = tx.QueryRow(ctx, `UPDATE profiles SET credits = credits - $2, version = version + 1, updated_at = now()
			WHERE player_id = $1 AND credits >= $2 RETURNING credits`, playerID, price).Scan(&credits)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrInsufficientCredits
		}
		return price, err
	}
	return 0, fmt.Errorf("unknown strategy %d", st)
}

// Gift moves credits between players. With ordered=false the rows are locked
// in argument order, so concurrent A->B and B->A gifts can deadlock; Postgres
// aborts one (40P01) and inTx re-runs it. ordered=true locks in ascending ID
// order, which makes the deadlock impossible.
func (s *Store) Gift(ctx context.Context, from, to, amount int64, ordered bool) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		ids := []int64{from, to}
		if ordered {
			slices.Sort(ids)
		}
		for _, id := range ids {
			if _, err := tx.Exec(ctx, `SELECT 1 FROM profiles WHERE player_id = $1 FOR NO KEY UPDATE`, id); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE profiles SET credits = credits - $2, version = version + 1 WHERE player_id = $1 AND credits >= $2`, from, amount)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrInsufficientCredits
		}
		_, err = tx.Exec(ctx, `UPDATE profiles SET credits = credits + $2, version = version + 1 WHERE player_id = $1`, to, amount)
		return err
	})
}

// Cosmetic is a catalog item.
type Cosmetic struct {
	ID    int64  `json:"id"`
	SKU   string `json:"sku"`
	Name  string `json:"name"`
	Price int64  `json:"price"`
	Stock *int32 `json:"stock"`
}

// Cosmetics lists the catalog.
func (s *Store) Cosmetics(ctx context.Context) ([]Cosmetic, error) {
	rows, err := s.Replica.Query(ctx, `SELECT id, sku, name, price, stock FROM cosmetics ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Cosmetic])
}
