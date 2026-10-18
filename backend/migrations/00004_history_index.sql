-- +goose NO TRANSACTION
-- +goose Up
-- Expand-only, online migration: CONCURRENTLY builds without blocking writes,
-- but cannot run inside a transaction (hence NO TRANSACTION) and must go
-- directly to Postgres, not through PgBouncer transaction pooling.
CREATE INDEX CONCURRENTLY IF NOT EXISTS match_players_player_started_idx
    ON match_players (player_id, started_at DESC);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS match_players_player_started_idx;
