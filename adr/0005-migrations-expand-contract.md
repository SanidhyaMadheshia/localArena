# ADR-0005: Schema migrations: goose, expand/contract, forward-fix rollback policy

- Status: accepted
- Date: 2026-10-18

## Context

During a rolling deploy, old and new API versions run against the same
database at the same time. A migration that breaks the old code causes an
outage even though the new code is correct. Migrations also run from several
places (Compose service, Kubernetes Job, developer shell) and must not race
each other.

## Decision

- **Tool:** pressly/goose v3, SQL files embedded in the binary
  (`backend/migrations`, `cmd/migrate`). Versioned files, numbered
  sequentially, each with `-- +goose Up` and `-- +goose Down`.
- **Concurrency:** the goose provider uses a Postgres advisory-lock session
  locker, so concurrent migrators serialize. Migrations connect **directly**
  to Postgres (`arena-db-rw` / `postgres:5432`), never through PgBouncer,
  because session locks and `CREATE INDEX CONCURRENTLY` need a session.
- **Expand/contract** for every change that touches existing columns:
  1. *Expand:* add nullable columns, new tables, and indexes (`CONCURRENTLY`,
     `-- +goose NO TRANSACTION`). Old code keeps working.
  2. Deploy code that writes both shapes and reads the new one.
  3. Backfill in batches.
  4. *Contract:* drop the old column or constraint in a later release, after
     no running version reads it.
- **Order:** migrate (expand) → roll out code. Never the reverse.
  `TestMigrationsRoundTripAndCompatibility` proves it for 00005: old queries
  work on schema v5, while the new `Profile` query fails on v4 with
  `42703 undefined_column`.
- **Rules:** queries name their columns (no `SELECT *`); no `NOT NULL`
  without a default on large tables; no long `ACCESS EXCLUSIVE` locks.
  Set `lock_timeout` when a migration needs a strong lock.
- **Rollback policy:** every migration has a tested `Down`, and the test
  runs `up → down-to 0 → up`. In production, a bad release rolls back the
  **code**, which is safe because expand migrations are backward compatible.
  Schema problems are fixed forward with a new migration. `Down` is for
  development and for the failed-migration drill in week 7, not for routine
  production use, because dropping a column that has received writes loses
  data.

## Consequences

- Each breaking change takes two or three releases. That cost is accepted in
  exchange for zero-downtime deploys.
- Partitioned `matches` needs new monthly partitions created ahead of time.
  A `DEFAULT` partition catches strays, but rows that pile up there make the
  later split expensive. Partition creation becomes a scheduled job (week 6).

## Alternatives considered

- golang-migrate: similar, but its locking and embedding are less integrated.
- Atlas (declarative): computes diffs automatically, which is powerful but
  hides the exact SQL and locks that this project is meant to learn about.
- ORM auto-migration: no review, no expand/contract, no locking control.
