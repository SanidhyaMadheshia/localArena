package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Profile is a player's public profile.
type Profile struct {
	PlayerID    int64    `json:"player_id"`
	Username    string   `json:"username"`
	DisplayName string   `json:"display_name"`
	Credits     int64    `json:"credits"`
	Version     int64    `json:"version"`
	AvatarURL   *string  `json:"avatar_url,omitempty"`
	Items       []string `json:"items"`
}

// CreatePlayer creates a player and profile with starting credits.
func (s *Store) CreatePlayer(ctx context.Context, username string, credits int64) (int64, error) {
	id, _, err := s.CreatePlayerIdempotent(ctx, "", username, credits)
	return id, err
}

// CreatePlayerIdempotent is CreatePlayer with an optional idempotency key.
// The key is claimed in the same transaction that creates the player, so a
// retry (same key, same username) returns the original ID with replayed=true;
// the same key with a different username is ErrIdempotencyMismatch.
func (s *Store) CreatePlayerIdempotent(ctx context.Context, key, username string, credits int64) (id int64, replayed bool, err error) {
	hash := sha256.Sum256([]byte(username))
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		replayed = false
		if key != "" {
			tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (scope, key, request_hash) VALUES ('create_player', $1, $2)
				ON CONFLICT (scope, key) DO NOTHING`, key, hash[:])
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 { // key already used: replay
				var stored []byte
				if err := tx.QueryRow(ctx, `SELECT request_hash, result_id FROM idempotency_keys WHERE scope = 'create_player' AND key = $1`,
					key).Scan(&stored, &id); err != nil {
					return err
				}
				if string(stored) != string(hash[:]) {
					return ErrIdempotencyMismatch
				}
				replayed = true
				return nil
			}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO players (username) VALUES ($1) RETURNING id`, username).Scan(&id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO profiles (player_id, display_name, credits) VALUES ($1, $2, $3)`, id, username, credits); err != nil {
			return err
		}
		if key != "" {
			_, err := tx.Exec(ctx, `UPDATE idempotency_keys SET result_id = $2 WHERE scope = 'create_player' AND key = $1`, key, id)
			return err
		}
		return nil
	})
	if pgCode(err) == codeUniqueViolation {
		return 0, false, ErrUsernameTaken
	}
	return id, replayed, err
}

// Profile reads a profile from q (primary or replica pool).
func (s *Store) Profile(ctx context.Context, q *pgxpool.Pool, playerID int64) (Profile, error) {
	var p Profile
	err := q.QueryRow(ctx, `
		SELECT p.id, p.username, pr.display_name, pr.credits, pr.version, pr.avatar_url,
		       coalesce(array(SELECT c.sku FROM inventory i JOIN cosmetics c ON c.id = i.cosmetic_id
		                      WHERE i.player_id = p.id ORDER BY c.sku), '{}')
		FROM players p JOIN profiles pr ON pr.player_id = p.id
		WHERE p.id = $1`, playerID).
		Scan(&p.PlayerID, &p.Username, &p.DisplayName, &p.Credits, &p.Version, &p.AvatarURL, &p.Items)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	return p, err
}

// WriteLSN returns the primary's current WAL position: a read-your-writes token.
func (s *Store) WriteLSN(ctx context.Context) (string, error) {
	var lsn string
	err := s.Primary.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn)
	return lsn, err
}

// ReaderFor returns the replica if it has replayed lsn within wait, otherwise
// the primary. An empty lsn means "any staleness is fine".
//
// This is read-your-writes on asynchronous replication: the client carries the
// LSN of its last write, and reads never go backwards in time for that client.
func (s *Store) ReaderFor(ctx context.Context, lsn string, wait time.Duration) (*pgxpool.Pool, bool) {
	if s.Replica == s.Primary || lsn == "" {
		return s.Replica, s.Replica != s.Primary
	}
	deadline := time.Now().Add(wait)
	for {
		var ok bool
		err := s.Replica.QueryRow(ctx, `SELECT coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, false)`, lsn).Scan(&ok)
		if err == nil && ok {
			return s.Replica, true
		}
		if err != nil || time.Now().After(deadline) {
			return s.Primary, false
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return s.Primary, false
		}
	}
}

// ReplicaLag returns the replica's replay delay as seen by the replica.
func (s *Store) ReplicaLag(ctx context.Context) (time.Duration, error) {
	var secs float64
	err := s.Replica.QueryRow(ctx,
		`SELECT coalesce(extract(epoch FROM now() - pg_last_xact_replay_timestamp()), 0)`).Scan(&secs)
	return time.Duration(secs * float64(time.Second)), err
}

// DisplayNames returns display names for the given players (missing ids are
// absent from the map). Read from the replica: names are decoration.
func (s *Store) DisplayNames(ctx context.Context, ids []int64) (map[int64]string, error) {
	out := make(map[int64]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.Replica.Query(ctx, `SELECT player_id, display_name FROM profiles WHERE player_id = ANY($1)`, ids)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return out, err
		}
		out[id] = name
	}
	return out, rows.Err()
}
