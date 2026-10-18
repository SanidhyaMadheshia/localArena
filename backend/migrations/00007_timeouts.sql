-- +goose Up
-- Timeout budget, innermost layer (adr/0007): Envoy route 4 s > API request
-- 3 s > Postgres statement 2 s. A statement that cannot finish in 2 s is
-- cancelled by the server, so a request never holds a connection after the
-- API has already given up. Abandoned open transactions are killed after 5 s.
-- Set per database so it reaches every session, including through PgBouncer
-- (which rejects unknown startup parameters). Migrations override it (0).
-- +goose StatementBegin
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I SET statement_timeout = %L', current_database(), '2s');
  EXECUTE format('ALTER DATABASE %I SET idle_in_transaction_session_timeout = %L', current_database(), '5s');
END $$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
  EXECUTE format('ALTER DATABASE %I RESET statement_timeout', current_database());
  EXECUTE format('ALTER DATABASE %I RESET idle_in_transaction_session_timeout', current_database());
END $$;
-- +goose StatementEnd
