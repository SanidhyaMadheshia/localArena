-- +goose Up
-- Outbox rows carry the protobuf Envelope that Debezium's EventRouter publishes
-- verbatim to game.economy-events.v1 (adr/0010). payload (jsonb) stays for
-- humans and SQL debugging. Expand-only: old writers leave envelope NULL and
-- the router skips them (snapshot.mode=no_data publishes only new rows).
ALTER TABLE outbox ADD COLUMN envelope bytea;

-- Debezium heartbeat target: lets the replication slot advance even when the
-- outbox is idle, so the primary does not retain WAL without bound.
CREATE TABLE debezium_heartbeat (id int PRIMARY KEY, beat_at timestamptz NOT NULL);
INSERT INTO debezium_heartbeat VALUES (1, now());

-- +goose Down
DROP TABLE debezium_heartbeat;
ALTER TABLE outbox DROP COLUMN envelope;
