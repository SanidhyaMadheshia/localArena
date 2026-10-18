-- +goose Up
-- Generic idempotency records for writes that have no natural dedupe key
-- (e.g. POST /v1/players). A client retry with the same key returns the
-- original result instead of creating a second resource. request_hash detects
-- a key reused for a different request. Old rows are safe to delete after the
-- retry window (index on created_at for that sweep).
CREATE TABLE idempotency_keys (
    scope        text        NOT NULL,
    key          text        NOT NULL CHECK (length(key) BETWEEN 1 AND 64),
    request_hash bytea       NOT NULL,
    result_id    bigint,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, key)
);
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);

-- +goose Down
DROP TABLE idempotency_keys;
