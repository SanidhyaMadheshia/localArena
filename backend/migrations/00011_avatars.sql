-- +goose Up
-- Imported avatars are stored as validated bytes (sniffed image type, <= 512 KiB)
-- and served by the API with that type and nosniff; the original URL is never
-- given to other clients (no third-party tracking, no later content swap).
CREATE TABLE avatars (
    player_id    bigint PRIMARY KEY REFERENCES players (id) ON DELETE CASCADE,
    content_type text   NOT NULL CHECK (content_type IN ('image/png', 'image/jpeg', 'image/webp')),
    data         bytea  NOT NULL CHECK (octet_length(data) <= 524288),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE avatars;
