-- +goose Up
-- Webhook replay protection (adr/0015): a delivery id is accepted once. Rows
-- older than the signature tolerance can be purged; a replay that old already
-- fails the timestamp check.
CREATE TABLE webhook_deliveries (
    delivery_id text        PRIMARY KEY CHECK (length(delivery_id) BETWEEN 1 AND 64),
    source      text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE webhook_deliveries;
