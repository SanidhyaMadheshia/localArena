package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// CreateLobby creates an open lobby with the owner as its first member.
func (s *Store) CreateLobby(ctx context.Context, ownerID int64, capacity int) (int64, error) {
	var id int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO lobbies (owner_id, capacity, member_count) VALUES ($1, $2, 1) RETURNING id`,
			ownerID, capacity).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO lobby_members (player_id, lobby_id) VALUES ($1, $2)`, ownerID, id)
		return err
	})
	return id, mapLobbyErr(err)
}

// JoinLobby adds a player using optimistic concurrency on lobbies.version.
//
// Two joins racing for the last seat both read version v; the first UPDATE
// wins, the second waits on the row lock, re-checks "version = v" against the
// committed row, matches 0 rows, and the whole transaction re-runs (seeing the
// lobby full). The membership PRIMARY KEY independently guarantees a player is
// in at most one lobby, even across different lobbies.
func (s *Store) JoinLobby(ctx context.Context, lobbyID, playerID int64) error {
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var capacity, count int
		var status string
		var version int64
		err := tx.QueryRow(ctx, `SELECT capacity, member_count, status, version FROM lobbies WHERE id = $1`, lobbyID).
			Scan(&capacity, &count, &status, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if status != "open" {
			return ErrLobbyClosed
		}
		if count >= capacity {
			return ErrLobbyFull
		}
		if _, err := tx.Exec(ctx, `INSERT INTO lobby_members (player_id, lobby_id) VALUES ($1, $2)`, playerID, lobbyID); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE lobbies SET member_count = member_count + 1, version = version + 1
			WHERE id = $1 AND version = $2`, lobbyID, version)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return errRetry
		}
		return nil
	})
	return mapLobbyErr(err)
}

// LeaveLobby removes the player from whichever lobby they are in and returns its ID.
func (s *Store) LeaveLobby(ctx context.Context, playerID int64) (int64, error) {
	var lobbyID int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `DELETE FROM lobby_members WHERE player_id = $1 RETURNING lobby_id`, playerID).Scan(&lobbyID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE lobbies SET member_count = member_count - 1, version = version + 1 WHERE id = $1`, lobbyID)
		return err
	})
	return lobbyID, err
}

// Lobby is a lobby with its members.
type Lobby struct {
	ID       int64   `json:"id"`
	OwnerID  int64   `json:"owner_id"`
	Capacity int     `json:"capacity"`
	Status   string  `json:"status"`
	Members  []int64 `json:"members"`
}

// GetLobby reads a lobby and its members from the primary in one query.
func (s *Store) GetLobby(ctx context.Context, id int64) (Lobby, error) {
	var l Lobby
	err := s.Primary.QueryRow(ctx, `
		SELECT l.id, l.owner_id, l.capacity, l.status,
		       coalesce(array(SELECT player_id FROM lobby_members WHERE lobby_id = l.id ORDER BY player_id), '{}')
		FROM lobbies l WHERE l.id = $1`, id).Scan(&l.ID, &l.OwnerID, &l.Capacity, &l.Status, &l.Members)
	if errors.Is(err, pgx.ErrNoRows) {
		return l, ErrNotFound
	}
	return l, err
}

// LobbyMembers returns member IDs (for tests and the API).
func (s *Store) LobbyMembers(ctx context.Context, lobbyID int64) ([]int64, error) {
	rows, err := s.Primary.Query(ctx, `SELECT player_id FROM lobby_members WHERE lobby_id = $1 ORDER BY player_id`, lobbyID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func mapLobbyErr(err error) error {
	switch {
	case pgCode(err) == codeUniqueViolation && constraintOf(err) == "lobby_members_pkey":
		return ErrAlreadyInLobby
	case pgCode(err) == codeCheckViolation:
		return ErrLobbyFull // member_count CHECK backstop
	}
	return err
}
