# ADR-0004: PostgreSQL as source of truth, PgBouncer transaction pooling, replica reads with read-your-writes

- Status: accepted
- Date: 2026-10-18

## Context

Profiles, credits, inventory, purchases, lobbies and match history must stay
correct under concurrency and survive restarts. Several API replicas will each
hold a connection pool, and Postgres backends are processes that cost about
5–10 MB each, so N replicas × pool size can exhaust `max_connections`. Reads
(profiles, history, catalog) far outnumber writes.

## Decision

- **PostgreSQL 18.6** is authoritative. Compose runs `postgres:18.6-alpine`;
  Kubernetes runs CloudNativePG 1.30 (`Cluster` with 2 instances,
  `ghcr.io/cloudnative-pg/postgresql:18.6-minimal-trixie`).
- **Driver:** pgx v5 with `pgxpool` (default 10 connections per process).
  Every request has a 3 s context, so waiting for a pool connection is
  bounded and surfaces as 503 + `Retry-After`, never as an unbounded queue.
- **PgBouncer in transaction mode** (`edoburu/pgbouncer` in Compose,
  CNPG `Pooler` in Kubernetes). `default_pool_size=10`,
  `query_wait_timeout=5s`, and `max_prepared_statements=200` so pgx's cached
  prepared statements work under transaction pooling. Session features
  (session `SET`, session advisory locks, `LISTEN`) are not used through the
  pooler. Migrations bypass it.
- **Replication:** asynchronous streaming replica with a physical
  replication slot (Compose) or CNPG-managed standby. Replica reads use
  **read-your-writes via LSN**: every write response carries
  `X-Write-LSN: pg_current_wal_lsn()`, and the client echoes it as
  `X-Read-After-LSN`. The API uses the replica only when
  `pg_last_wal_replay_lsn() >= lsn` (waiting up to 200 ms) and otherwise
  reads from the primary. Reads with no token accept bounded staleness.
- **Failover (Kubernetes):** CNPG elects the new primary through a
  Kubernetes lease and moves the `-rw` Service. Two settings proved
  necessary (Experiment 002): `smartShutdownTimeout: 10` and PgBouncer
  `server_login_retry: 1`.
- Connection errors, shutdown, and read-only-transaction SQLSTATEs map to
  **503 + Retry-After**, not 500 (`store.IsUnavailable`).

## Consequences

- Async replication means a failover can lose the last few committed
  transactions (RPO > 0). Synchronous replication (`synchronous_commit` with
  quorum) trades write latency for RPO = 0. Revisit in week 6 (DR drills).
- CAP framing: during a partition between primary and replica, writes stay
  consistent (single primary), replica reads stay available but stale. The
  LSN token is how a client opts into consistency for its own writes.
- Pool sizing is now a capacity knob that has to be documented:
  Postgres `max_connections` ≥ pooler `default_pool_size` × databases + admin
  headroom. App pools can be larger because PgBouncer multiplexes them.

## Alternatives considered

- Session pooling: safe for all session features, but provides little
  multiplexing for short API requests.
- No pooler, only large app pools: works for 2–3 replicas and fails as the
  number of replicas grows (Week 3 runs 3 API replicas; HPA comes later).
- Reading from the replica unconditionally: simplest, but a user who buys an
  item and refreshes may not see it. The stale-read test reproduces this
  deterministically by pausing WAL replay.
