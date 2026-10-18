# Experiment 002: Week 2 persistence, transactions, and concurrency correctness

## Hypothesis

With schema constraints, a correct lock strategy, and idempotency keys,
concurrent requests cannot oversell limited stock, overspend credits,
duplicate a purchase, or place a player in two lobbies. A naive
read-modify-write implementation will lose updates. A missing index and an
N+1 access pattern will show up clearly in `EXPLAIN ANALYZE` and latency.
CNPG failover should cost seconds of write unavailability, not minutes.

## System version and configuration

- Week 2 working tree (2026-10-18). Go 1.27.2, pgx v5.11.0, goose v3.29.0.
- Compose: `postgres:18.6-alpine` primary with an async streaming replica
  (physical slot `replica1`), `edoburu/pgbouncer:v1.26.0-p0` in transaction
  mode (pool 10, `query_wait_timeout=5`), plus two shard instances
  (profile `shards`).
- kind (`kind-localarena`, 1.37): CloudNativePG chart 0.29.1 / operator
  1.30.1, `Cluster arena-db` (2 instances, PG 18.6), `Pooler` (PgBouncer,
  transaction mode), `migrate` Job, `api` Deployment with 2 replicas.
- Host: i5-13450HX, Docker Desktop with 16 CPUs / 8 GiB. Every client,
  database, and pooler shares one machine, so latencies are relative.

Reproduce:

```bash
make db-up && make itest           # all correctness tests (unit + Postgres integration)
make race                          # same under -race, inside the Compose network
make experiment-002                # purchase-strategy table + EXPLAIN ANALYZE (results/)
make k8s-up                        # CNPG + pooler + migrate Job + API on kind
experiments/002-week2-persistence/failover.sh   # primary-kill drill
```

## Results

### 1. Concurrent purchase of a limited cosmetic

200 buyers released together by a barrier, 20 units in stock, price 10,
each buyer has 1000 credits (`results/purchase-strategies.md`).

| strategy | sold | out of stock | other errors | final stock | wall ms | deadlock retries | optimistic retries |
|---|---:|---:|---:|---:|---:|---:|---:|
| naive (read → check in Go → write value) | **200** | 0 | 0 | **18** | 1244 | 0 | 0 |
| pessimistic `FOR UPDATE` | **0** | 0 | **200** | 20 | 59787 | 344 | 0 |
| pessimistic `FOR NO KEY UPDATE` | 20 | 180 | 0 | 0 | 984 | 0 | 0 |
| optimistic (`version` column) | 20 | 180 | 0 | 0 | 832 | 0 | 206 |
| conditional `UPDATE … WHERE stock > 0` | 20 | 180 | 0 | 0 | **129** | 0 | 0 |

- **Naive** demonstrates a lost update. 200 buyers each read stock 20 and
  wrote back 19 (or a nearby value), so 200 items were granted for 2 units
  of stock. `CHECK (stock >= 0)` cannot catch this, because no individual
  write was negative. Only atomic or locked writes prevent it.
- **`FOR UPDATE` deadlocked every buyer**, which was unexpected. The first
  statement in the transaction inserts a `purchases` row, and its foreign
  key takes `FOR KEY SHARE` on the cosmetic row. With 200 transactions each
  holding KEY SHARE, every attempt to upgrade to `FOR UPDATE` waits on all
  the others, which is a deadlock cycle. After 10 retries, all 200 failed
  (60 s wall time). `FOR NO KEY UPDATE` does not conflict with KEY SHARE,
  and with it the strategy is correct (ADR-0006).
- **Conditional updates** were the cheapest: one round trip per row, the
  check happens under the row lock, and nothing is retried.

### 2. Correctness tests (`make itest`, all pass; also under `-race`)

| Test | Fault | Result |
|---|---|---|
| `TestLimitedStockNeverOversells` | 50 buyers / 5 units × 3 strategies | exactly 5 sold, stock 0, 5 inventory rows, 5 outbox rows, 500 credits debited |
| `TestCreditsNeverOverspent` | 10 concurrent purchases at 100 with 250 credits | exactly 2 succeed, 50 credits left |
| `TestPurchaseIdempotency` | same `Idempotency-Key` sent 20× concurrently | 1 applied, 19 replays with the same purchase ID, debited once; reusing the key for another item → 422 |
| `TestLobbyLastSeats` | 20 joiners for 3 seats | exactly 3 joined; `member_count` = rows = 4 |
| `TestDuplicateJoinAcrossLobbies` | 1 player joins 10 lobbies at once | exactly 1 succeeds (PK on `lobby_members.player_id`) |
| `TestDeadlockIsDetected` | two transactions lock rows A→B and B→A | exactly one victim, `40P01` after `deadlock_timeout=200ms` |
| `TestGiftLockOrdering` | 20 concurrent opposite-direction gifts | unordered: deadlocks (34 of 60 failed after 10 retries in an earlier 60-gift run, 560 retries); ordered by ID: 0 deadlocks; credits conserved in both |
| `TestPoolExhaustionTimesOut` | pool of 2, both busy | acquire fails at the 200 ms context deadline; `EmptyAcquireCount > 0` |
| `TestPgBouncerQueueWaitTimeout` | 12 clients × `pg_sleep(6)` against pool 10 | 10 served, 2 rejected with `query_wait_timeout` (08P01) |
| `TestStaleReplicaReadAndReadYourWrites` | WAL replay paused on the replica | replica returns the old value (stale read); `ReaderFor(lsn)` falls back to the primary; after resume it uses the replica with the new value |
| `TestMigrationsRoundTripAndCompatibility` | up → down-to 0 → up; schema v4 vs v5 | old code works on new schema; new code on old schema fails with `42703` |
| `TestRouterAgainstTwoShards` | 100 players on 2 shards | 48/52 split, routed reads, scatter-gather top-5 is correct, duplicate username accepted across shards |
| `TestJumpBalanceAndMinimalMovement` | 100k keys, 2 → 3 shards | 49.96/50.04 % balance; 33.3 % moved, all to the new shard |

### 3. Missing index, query shape, N+1

5,000 players, 60,000 matches (Sep–Dec 2026, monthly partitions), 600,000
`match_players` rows. Player 1234 has 120 matches. Query: last 20 matches
with all participants. Full plans are in `results/query-plans.md`.

| query | schema | runs | p50 ms | p95 ms | p99 ms |
|---|---|---:|---:|---:|---:|
| join, LIMIT applied last | v3, no `match_players(player_id)` index | 50 | 13.67 | 46.80 | 57.92 |
| join, LIMIT first (CTE) | v3, no index | 50 | 12.02 | 42.31 | 47.43 |
| join, LIMIT applied last | v4, indexed | 500 | 5.70 | 7.07 | 9.09 |
| **join, LIMIT first (1 query)** | v4, indexed | 500 | **0.80** | **1.23** | **1.52** |
| N+1 (21 queries) | v4, indexed | 500 | 7.75 | 10.83 | 13.51 |

- **Missing index:** v3 does a `Parallel Seq Scan on match_players`
  (≈ 600k rows, 4,412 buffers) to find 120 rows. Migration 00004
  (`CREATE INDEX CONCURRENTLY`) changes it to an index scan on
  `match_players_player_started_idx`.
- **The index alone was not enough.** With `LIMIT` after the join, the
  planner still hash-joined against a seq scan of all 60,000 matches in
  every partition (12 ms). Rewriting so the 20 newest rows are taken from
  the index first (CTE + `LIMIT`) gives a 0.7 ms execution time.
  `EXPLAIN ANALYZE` was the only reliable way to see this.
- **N+1:** with the same indexes, 21 round trips cost about 10× the single
  query (7.75 vs 0.80 ms p50) on loopback, where a round trip is about
  0.3 ms. Over a real network at about 1 ms per round trip, the gap grows
  linearly with N. `TestHistoryNPlusOneVsJoin` asserts 21 vs 1 statements
  using the pgx query tracer (`localarena_db_queries_total`).
- **Partition pruning:** a one-month predicate scans only `matches_2026_11`
  (111 buffers). The history join does not prune, because it filters on
  `match_players`, not on `matches.started_at`.

### 4. Kubernetes primary failover (CNPG + Pooler)

A busybox pod writes `POST /v1/players` every 100 ms through Service `api`
(2 replicas), and the primary pod is deleted (`results/failover.txt`).

| run | config | failed writes | write outage |
|---|---|---:|---:|
| 0 | defaults (`smartShutdownTimeout` 180 s) | n/a | the old primary kept serving through the pooler for ~3 min while "Terminating"; promotion only after smart shutdown expired |
| 1 | `smartShutdownTimeout: 10` | 48 | 15 s, starting ~12 s after delete |
| 2 | + PgBouncer `server_login_retry: 1` | **3** | **1 s** |

- Postgres smart shutdown waits for existing sessions to end, and a pooler
  keeps its server connections open, so deleting the primary pod caused no
  failover for about 180 s.
- After promotion, PgBouncer kept returning its **cached** login failure
  (`server login has been failing, cached error … (server_login_retry)`)
  for 15 s, even though the new primary was already up.
- During the outage the API returned 500. It now maps connection-class,
  shutdown, and read-only SQLSTATEs to **503 + Retry-After**
  (`store.IsUnavailable`).
- Leader election: the CNPG instance manager logs `Acquiring primary lease
  → Acquired primary lease → Promoting instance → server promoted`
  (about 0.7 s). The Kubernetes lease prevents two primaries.

## Root explanation (summary)

Correctness comes from making the database the arbiter: constraints for
invariants, single-statement conditional writes or correctly chosen row
locks for contended counters, and unique keys for idempotency. The two
surprises, the FK lock-upgrade deadlock and the pooler-amplified failover
outage, both came from interactions between components that each behaved
as documented. Neither showed up until measured.

## Remaining risks / carried forward

- RPO is not measured. Async replication can lose acknowledged writes on
  failover; this goes to the week 6 DR drill, along with synchronous commit.
- Both CNPG instances were scheduled on `localarena-worker`, because the
  control plane is tainted, so anti-affinity is preferred rather than
  required. A node-loss drill needs a second worker.
- Client-side retries after a 503 rely on the `Idempotency-Key` for
  purchases. Other writes (`POST /v1/players`) are not idempotent yet.
- No authentication: `player_id` in request bodies is trusted (week 5).
- The shard router does not rebalance, and cross-shard username uniqueness
  is unsolved (ADR-0006).
- Pool-exhaustion behavior is shown in tests, not under HTTP load.
  k6 against the API comes in week 3.
