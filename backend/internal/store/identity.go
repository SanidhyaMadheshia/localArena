package store

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrBanned is returned for a banned account.
var ErrBanned = errors.New("account banned")

var usernameSafe = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// PlayerForSubject returns the player linked to an IdP subject, creating it on
// first login (just-in-time provisioning) with starting credits. Concurrent
// first logins for one subject converge on one row (unique idp_sub).
func (s *Store) PlayerForSubject(ctx context.Context, sub, username string, credits int64) (int64, error) {
	var id int64
	err := s.Primary.QueryRow(ctx, `SELECT id FROM players WHERE idp_sub = $1`, sub).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	name := usernameSafe.ReplaceAllString(username, "_")
	if len(name) < 3 {
		name = "player"
	}
	name = name[:min(len(name), 20)] + "_" + sub[:min(len(sub), 8)] // unique even if usernames collide
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `INSERT INTO players (username, idp_sub) VALUES ($1, $2)
			ON CONFLICT (idp_sub) DO NOTHING RETURNING id`, name, sub).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) { // lost the race: the other login created it
			return tx.QueryRow(ctx, `SELECT id FROM players WHERE idp_sub = $1`, sub).Scan(&id)
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO profiles (player_id, display_name, credits) VALUES ($1, $2, $3)`, id, username, credits)
		return err
	})
	return id, err
}

// Banned reports whether a player is currently banned (and why).
func (s *Store) Banned(ctx context.Context, playerID int64) (bool, string, error) {
	var reason string
	err := s.Primary.QueryRow(ctx, `SELECT reason FROM bans WHERE player_id = $1 AND (expires_at IS NULL OR expires_at > now())`,
		playerID).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", nil
	}
	return err == nil, reason, err
}

// Ban bans (or re-bans) a player. Idempotent per player.
func (s *Store) Ban(ctx context.Context, playerID int64, reason, by string, until *time.Time) error {
	tag, err := s.Primary.Exec(ctx, `INSERT INTO bans (player_id, reason, banned_by, expires_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (player_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by,
		expires_at = EXCLUDED.expires_at, created_at = now()`, playerID, reason, by, until)
	if pgCode(err) == "23503" {
		return ErrNotFound
	}
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// SetDisplayName updates the display name (validated by the caller). Parameterised
// like every query in this package: user input never becomes SQL text.
func (s *Store) SetDisplayName(ctx context.Context, playerID int64, name string) error {
	tag, err := s.Primary.Exec(ctx, `UPDATE profiles SET display_name = $2, version = version + 1, updated_at = now() WHERE player_id = $1`, playerID, name)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// SetAvatar stores validated avatar bytes.
func (s *Store) SetAvatar(ctx context.Context, playerID int64, contentType string, data []byte) error {
	_, err := s.Primary.Exec(ctx, `INSERT INTO avatars (player_id, content_type, data) VALUES ($1, $2, $3)
		ON CONFLICT (player_id) DO UPDATE SET content_type = EXCLUDED.content_type, data = EXCLUDED.data, updated_at = now()`,
		playerID, contentType, data)
	return err
}

// Avatar returns a player's stored avatar.
func (s *Store) Avatar(ctx context.Context, playerID int64) (string, []byte, error) {
	var ct string
	var b []byte
	err := s.Replica.QueryRow(ctx, `SELECT content_type, data FROM avatars WHERE player_id = $1`, playerID).Scan(&ct, &b)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, ErrNotFound
	}
	return ct, b, err
}
