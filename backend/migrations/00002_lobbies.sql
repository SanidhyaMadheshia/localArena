-- +goose Up
-- Lobbies. "One active lobby per player" is a PRIMARY KEY on lobby_members.player_id:
-- membership rows exist only while active, so a second join cannot commit.
CREATE TABLE lobbies (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner_id     bigint  NOT NULL REFERENCES players (id),
    capacity     integer NOT NULL CHECK (capacity BETWEEN 2 AND 20),
    member_count integer NOT NULL DEFAULT 0,
    status       text    NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    version      bigint  NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK (member_count BETWEEN 0 AND capacity) -- backstop if application logic is wrong
);

CREATE TABLE lobby_members (
    player_id bigint PRIMARY KEY REFERENCES players (id) ON DELETE CASCADE,
    lobby_id  bigint NOT NULL REFERENCES lobbies (id) ON DELETE CASCADE,
    joined_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX lobby_members_lobby_idx ON lobby_members (lobby_id);

-- +goose Down
DROP TABLE lobby_members, lobbies;
