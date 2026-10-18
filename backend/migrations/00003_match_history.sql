-- +goose Up
-- Match history, range-partitioned by start time: old months can be detached
-- or dropped cheaply, and time-bounded queries prune partitions.
-- The partition key must be part of every unique constraint, hence (id, started_at).
CREATE TABLE matches (
    id         bigint GENERATED ALWAYS AS IDENTITY,
    started_at timestamptz NOT NULL,
    ended_at   timestamptz,
    map        text NOT NULL DEFAULT 'arena-1',
    PRIMARY KEY (id, started_at)
) PARTITION BY RANGE (started_at);

CREATE TABLE matches_2026_09 PARTITION OF matches FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE matches_2026_10 PARTITION OF matches FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE matches_2026_11 PARTITION OF matches FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE matches_2026_12 PARTITION OF matches FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
-- Catch-all so an insert outside the planned range never fails; monitor it and split.
CREATE TABLE matches_default PARTITION OF matches DEFAULT;

CREATE TABLE match_players (
    match_id   bigint      NOT NULL,
    started_at timestamptz NOT NULL,
    player_id  bigint      NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    score      integer     NOT NULL DEFAULT 0,
    PRIMARY KEY (match_id, player_id),
    FOREIGN KEY (match_id, started_at) REFERENCES matches (id, started_at) ON DELETE CASCADE
);
-- Deliberately NO index on match_players(player_id): migration 00004 adds it
-- so Experiment 002 can measure the missing-index plan first.

-- +goose Down
DROP TABLE match_players, matches;
