package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	eventsv1 "github.com/SanidhyaMadheshia/localArena/backend/gen/events/v1"
)

// Reward saga steps (adr/0012). Each step is one local transaction, guarded by
// the saga's current state so that redelivered commands are no-ops:
//
//	(none) --reserve--> reserved --grant--> granted
//	   |                   |
//	   +--> rejected       +--compensate--> compensated   (grant failed permanently)
//
// The pool decrement (reserve) and the player credit (grant) are deliberately
// in different transactions: in a split system they live in different services.

// SagaState values.
const (
	SagaReserved    = "reserved"
	SagaGranted     = "granted"
	SagaCompensated = "compensated"
	SagaRejected    = "rejected"
)

// ErrPlayerGone is a permanent grant failure: the account does not exist.
var ErrPlayerGone = errors.New("player does not exist")

// Reward is one saga instance.
type Reward struct {
	ID, Season, MatchID string
	PlayerID, Credits   int64
}

// SagaStatus returns the current state ("" if the saga has not started).
func (s *Store) SagaStatus(ctx context.Context, id string) (state, reason string, err error) {
	var r *string
	err = s.Primary.QueryRow(ctx, `SELECT state, reason FROM reward_sagas WHERE reward_id = $1`, id).Scan(&state, &r)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if r != nil {
		reason = *r
	}
	return state, reason, err
}

// Reserve takes credits from the season pool. Returns the resulting state:
// reserved, or rejected if the pool cannot cover it. Idempotent.
func (s *Store) Reserve(ctx context.Context, r Reward) (string, error) {
	var state string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT state FROM reward_sagas WHERE reward_id = $1`, r.ID).Scan(&state)
		if err == nil {
			return nil // already past this step
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE reward_pool SET remaining = remaining - $2 WHERE season = $1 AND remaining >= $2`, r.Season, r.Credits)
		if err != nil {
			return err
		}
		state = SagaReserved
		var reason *string
		if tag.RowsAffected() == 0 {
			state, reason = SagaRejected, ptr("reward pool exhausted")
		}
		_, err = tx.Exec(ctx, `INSERT INTO reward_sagas (reward_id, season, player_id, match_id, credits, state, reason)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, r.ID, r.Season, r.PlayerID, r.MatchID, r.Credits, state, reason)
		return err
	})
	return state, err
}

// Grant credits the player and publishes RewardGranted through the outbox, in
// one transaction. Returns ErrPlayerGone (permanent) if the account is missing.
func (s *Store) Grant(ctx context.Context, r Reward) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM reward_sagas WHERE reward_id = $1 FOR UPDATE`, r.ID).Scan(&state); err != nil {
			return err
		}
		if state != SagaReserved {
			return nil // granted, compensated or rejected already: nothing to do
		}
		tag, err := tx.Exec(ctx, `UPDATE profiles SET credits = credits + $2, version = version + 1, updated_at = now() WHERE player_id = $1`,
			r.PlayerID, r.Credits)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrPlayerGone
		}
		if _, err := tx.Exec(ctx, `UPDATE reward_sagas SET state = 'granted', updated_at = now() WHERE reward_id = $1`, r.ID); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, "RewardGranted", r.PlayerID, r.ID, r.MatchID,
			&eventsv1.Envelope_RewardGranted{RewardGranted: &eventsv1.RewardGranted{RewardId: r.ID, PlayerId: r.PlayerID, Credits: r.Credits}})
	})
}

// Compensate undoes Reserve after a permanent Grant failure: credits go back
// to the pool. Guarded by state = reserved, so it runs at most once and never
// after a successful grant.
func (s *Store) Compensate(ctx context.Context, id, reason string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var season string
		var credits int64
		err := tx.QueryRow(ctx, `UPDATE reward_sagas SET state = 'compensated', reason = $2, updated_at = now()
			WHERE reward_id = $1 AND state = 'reserved' RETURNING season, credits`, id, reason).Scan(&season, &credits)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE reward_pool SET remaining = remaining + $2 WHERE season = $1`, season, credits)
		return err
	})
}

// PoolRemaining reports the season pool (for tests and experiments).
func (s *Store) PoolRemaining(ctx context.Context, season string) (int64, error) {
	var n int64
	err := s.Primary.QueryRow(ctx, `SELECT remaining FROM reward_pool WHERE season = $1`, season).Scan(&n)
	return n, err
}

func ptr[T any](v T) *T { return &v }
