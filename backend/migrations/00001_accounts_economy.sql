-- +goose Up
-- Accounts and economy. Invariants enforced by the database, not only by code:
--   credits never negative, stock never negative, an item is owned at most once,
--   a purchase request (player, idempotency key) is applied at most once.

CREATE TABLE players (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username   text NOT NULL UNIQUE CHECK (length(username) BETWEEN 3 AND 32),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE profiles (
    player_id    bigint PRIMARY KEY REFERENCES players (id) ON DELETE CASCADE,
    display_name text   NOT NULL,
    credits      bigint NOT NULL DEFAULT 0 CHECK (credits >= 0),
    version      bigint NOT NULL DEFAULT 0, -- optimistic concurrency token
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cosmetics (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku     text   NOT NULL UNIQUE,
    name    text   NOT NULL,
    price   bigint NOT NULL CHECK (price > 0),
    stock   integer CHECK (stock >= 0), -- NULL = unlimited
    version bigint NOT NULL DEFAULT 0
);

CREATE TABLE inventory (
    player_id   bigint NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    cosmetic_id bigint NOT NULL REFERENCES cosmetics (id),
    acquired_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, cosmetic_id)
);

CREATE TABLE purchases (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id       bigint NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    cosmetic_id     bigint NOT NULL REFERENCES cosmetics (id),
    price           bigint NOT NULL,
    idempotency_key text   NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 64),
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (player_id, idempotency_key)
);

-- Transactional outbox in the shape Debezium's EventRouter expects (week 4).
CREATE TABLE outbox (
    id            uuid PRIMARY KEY DEFAULT uuidv7(),
    aggregatetype text  NOT NULL,
    aggregateid   text  NOT NULL,
    type          text  NOT NULL,
    payload       jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

INSERT INTO cosmetics (sku, name, price, stock) VALUES
    ('hat-basic',   'Basic Hat',      100, NULL),
    ('trail-gold',  'Golden Trail',   500, 10),
    ('skin-legend', 'Legendary Skin', 900, 1);

-- +goose Down
DROP TABLE outbox, purchases, inventory, cosmetics, profiles, players;
