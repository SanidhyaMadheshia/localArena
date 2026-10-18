package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MatchPlayer is one participant's result.
type MatchPlayer struct {
	PlayerID int64 `json:"player_id"`
	Score    int   `json:"score"`
}

// MatchSummary is one entry in a player's history.
type MatchSummary struct {
	ID        int64         `json:"id"`
	StartedAt time.Time     `json:"started_at"`
	Map       string        `json:"map"`
	Score     int           `json:"score"` // the requesting player's score
	Players   []MatchPlayer `json:"players"`
}

// RecordMatch stores a finished match and its scores in one transaction.
func (s *Store) RecordMatch(ctx context.Context, started, ended time.Time, scores []MatchPlayer) (int64, error) {
	var id int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO matches (started_at, ended_at) VALUES ($1, $2) RETURNING id`,
			started, ended).Scan(&id); err != nil {
			return err
		}
		rows := make([][]any, len(scores))
		for i, sc := range scores {
			rows[i] = []any{id, started, sc.PlayerID, sc.Score}
		}
		_, err := tx.CopyFrom(ctx, pgx.Identifier{"match_players"},
			[]string{"match_id", "started_at", "player_id", "score"}, pgx.CopyFromRows(rows))
		return err
	})
	return id, err
}

// HistoryNPlusOne is the deliberately naive version: 1 query for the list,
// then 1 query per match for its participants (N+1 round trips).
func (s *Store) HistoryNPlusOne(ctx context.Context, q *pgxpool.Pool, playerID int64, limit int) ([]MatchSummary, error) {
	rows, err := q.Query(ctx, `
		SELECT m.id, m.started_at, m.map, me.score
		FROM match_players me JOIN matches m ON m.id = me.match_id AND m.started_at = me.started_at
		WHERE me.player_id = $1 ORDER BY me.started_at DESC LIMIT $2`, playerID, limit)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (MatchSummary, error) {
		var m MatchSummary
		return m, r.Scan(&m.ID, &m.StartedAt, &m.Map, &m.Score)
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		rows, err := q.Query(ctx, `SELECT player_id, score FROM match_players WHERE match_id = $1 ORDER BY score DESC, player_id`, out[i].ID)
		if err != nil {
			return nil, err
		}
		if out[i].Players, err = pgx.CollectRows(rows, pgx.RowToStructByPos[MatchPlayer]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// History returns the same result in one round trip: participants are
// aggregated per match with a LATERAL subquery that uses the primary key index.
func (s *Store) History(ctx context.Context, q *pgxpool.Pool, playerID int64, limit int) ([]MatchSummary, error) {
	rows, err := q.Query(ctx, HistorySQL, playerID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (MatchSummary, error) {
		var m MatchSummary
		return m, r.Scan(&m.ID, &m.StartedAt, &m.Map, &m.Score, &m.Players)
	})
}

// HistorySQL is exported so experiments can EXPLAIN exactly what runs.
//
// "Limit first": the CTE takes the player's newest N rows straight from the
// (player_id, started_at DESC) index, and only those N are joined and
// aggregated. Applying LIMIT after the join (HistorySQLLimitLast) makes the
// planner join every match the player ever played (Experiment 002).
const HistorySQL = `
	WITH mine AS (
		SELECT match_id, started_at, score FROM match_players
		WHERE player_id = $1 ORDER BY started_at DESC LIMIT $2
	)
	SELECT m.id, m.started_at, m.map, mine.score, ps.players
	FROM mine
	JOIN matches m ON m.id = mine.match_id AND m.started_at = mine.started_at
	CROSS JOIN LATERAL (
		SELECT json_agg(json_build_object('player_id', mp.player_id, 'score', mp.score)
		                ORDER BY mp.score DESC, mp.player_id) AS players
		FROM match_players mp WHERE mp.match_id = m.id
	) ps
	ORDER BY mine.started_at DESC`

// HistorySQLLimitLast is the first version of HistorySQL, kept for comparison.
const HistorySQLLimitLast = `
	SELECT m.id, m.started_at, m.map, me.score, ps.players
	FROM match_players me
	JOIN matches m ON m.id = me.match_id AND m.started_at = me.started_at
	CROSS JOIN LATERAL (
		SELECT json_agg(json_build_object('player_id', mp.player_id, 'score', mp.score)
		                ORDER BY mp.score DESC, mp.player_id) AS players
		FROM match_players mp WHERE mp.match_id = m.id
	) ps
	WHERE me.player_id = $1
	ORDER BY me.started_at DESC
	LIMIT $2`
