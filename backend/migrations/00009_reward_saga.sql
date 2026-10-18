-- +goose Up
-- Match-reward saga (adr/0012). Steps run in separate local transactions, as
-- if owned by different services; reward_sagas is the orchestrator's state,
-- keyed by the deterministic reward_id, which makes every step idempotent.
CREATE TABLE reward_pool (
    season    text   PRIMARY KEY,
    remaining bigint NOT NULL CHECK (remaining >= 0)
);
INSERT INTO reward_pool VALUES ('s1', 10000000);

CREATE TABLE reward_sagas (
    reward_id  text        PRIMARY KEY,
    season     text        NOT NULL REFERENCES reward_pool (season),
    player_id  bigint      NOT NULL, -- no FK: the player may be gone; that is a saga failure, not a constraint error
    match_id   text        NOT NULL,
    credits    bigint      NOT NULL CHECK (credits > 0),
    state      text        NOT NULL CHECK (state IN ('reserved', 'granted', 'compensated', 'rejected')),
    reason     text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reward_sagas_state_idx ON reward_sagas (state) WHERE state = 'reserved';

-- +goose Down
DROP TABLE reward_sagas, reward_pool;
