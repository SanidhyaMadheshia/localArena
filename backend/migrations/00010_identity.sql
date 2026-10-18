-- +goose Up
-- Identity (adr/0013): players are linked to the IdP by the token's `sub`
-- (stable, never reused), not by username (mutable). Bans live in the
-- database so they apply immediately, even to tokens issued before the ban.
ALTER TABLE players ADD COLUMN idp_sub text UNIQUE;
CREATE TABLE bans (
    player_id  bigint PRIMARY KEY REFERENCES players (id) ON DELETE CASCADE,
    reason     text        NOT NULL CHECK (length(reason) BETWEEN 1 AND 200),
    banned_by  text        NOT NULL, -- admin's idp sub
    expires_at timestamptz,          -- NULL = permanent
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE bans;
ALTER TABLE players DROP COLUMN idp_sub;
