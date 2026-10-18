# ADR-0006: Concurrency control for economy and lobbies; sharding prototype

- Status: accepted
- Date: 2026-10-18

## Context

The Week 2 gate: no overspending, no overselling of limited cosmetics, no
duplicate active-lobby membership, and retries must not duplicate purchases.
Several lock strategies were implemented and measured (Experiment 002).

## Decision

1. **Invariants live in the schema first.** `CHECK (credits >= 0)`,
   `CHECK (stock >= 0)`, `inventory PRIMARY KEY (player_id, cosmetic_id)`,
   `lobby_members PRIMARY KEY (player_id)`, `CHECK (member_count <= capacity)`,
   `UNIQUE (player_id, idempotency_key)`. A buggy code path (see `Naive`)
   can still lose updates, but it cannot commit a negative balance or a
   second active lobby.
2. **Purchases use conditional updates by default:**
   `UPDATE cosmetics SET stock = stock - 1 WHERE id = $1 AND (stock IS NULL OR stock > 0)`
   followed by `UPDATE profiles SET credits = credits - $p WHERE player_id = $1 AND credits >= $p`.
   Each statement checks and writes atomically under the row lock, with no
   read-modify-write window. It was the fastest strategy measured (200
   buyers for 20 units in 129 ms, versus 832 ms optimistic and 984 ms
   pessimistic).
3. **Pessimistic locking uses `FOR NO KEY UPDATE`, not `FOR UPDATE`.** The
   transaction first inserts a `purchases` row, whose foreign key takes
   `FOR KEY SHARE` on the cosmetic. Every concurrent buyer holds that share
   lock, so upgrading to `FOR UPDATE` deadlocks all of them: 0 of 200
   succeeded and there were 344 deadlock retries. `NO KEY UPDATE` does not
   conflict with `KEY SHARE`.
4. **Optimistic locking** (`version` column, `WHERE version = $read`, re-run
   on 0 rows) is used for lobbies, where conflicts are rare and the read
   determines several decisions (status, capacity).
5. **Idempotency:** a purchase claims `(player_id, Idempotency-Key)` with
   `INSERT … ON CONFLICT DO NOTHING` inside the same transaction. A
   concurrent duplicate blocks on the unique index, then replays the stored
   result (HTTP 200 instead of 201). A business failure rolls back the claim,
   so the key can be retried.
6. **Deadlocks:** locks are always taken in a fixed global order
   (cosmetic → profile, then ascending player ID for gifts). All transactions
   go through `inTx`, which re-runs the closure on `40P01` and `40001` with
   jittered exponential backoff (at most 10 retries). The closure must have
   no side effects outside the transaction.
7. **Outbox:** the `CosmeticPurchased` event is inserted in the same
   transaction (Debezium EventRouter column layout) and is published in
   week 4.
8. **Sharding (prototype only):** Jump Consistent Hash on a mixed player ID
   over N pools. Single-shard transactions only; cross-shard reads use
   scatter-gather (`TopCredits`).

## Consequences

- With sharding, global `UNIQUE(username)` is lost. The test shows the same
  username accepted on two shards, so a production design needs a username
  directory or a reservation table on one shard. Identity allocation also
  moves out of `GENERATED ALWAYS` (global IDs; snowflake-style later).
- Growing from 2 to 3 shards moves about 1/3 of keys, and only onto the new
  shard (tested: 33.3%). That still requires an online copy-and-cutover
  process. Not implemented.
- Distributed locks: the only cross-process lock used is the Postgres
  advisory lock that serializes migrations. Correctness never depends on a
  lease-based lock without fencing.

## Alternatives considered

- `SERIALIZABLE` isolation everywhere: correct with less reasoning, but
  every conflict becomes a retry. It remains an option for complex
  multi-row rules later.
- Redis or Valkey locks for purchases: they add a second source of truth
  and need fencing tokens to be safe.
- Citus or Vitess instead of an application router: the right answer at
  real scale, but it hides the routing this week is meant to teach.
