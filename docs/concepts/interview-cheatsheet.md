# System design interview cheatsheet

The 60 most important system-design interview questions this project answers,
grouped by topic. Each has a short model answer grounded in a measured result
and a link to the deep dive. Numbers are from [`../../experiments/`](../../experiments)
and [`../../FINAL_REPORT.md`](../../FINAL_REPORT.md).

Jump to: [Real-time & game loop](#real-time-and-game-loop) ·
[Databases & concurrency](#databases-and-concurrency) ·
[Traffic, caching & resilience](#traffic-caching-and-resilience) ·
[Events, Kafka & streaming](#events-kafka-and-streaming) ·
[Security & identity](#security-and-identity) ·
[Observability & SRE](#observability-and-sre) ·
[Delivery & Kubernetes](#delivery-and-kubernetes) ·
[Multi-region & operations](#multi-region-and-operations) ·
[Scale-up design questions](#scale-up-design-questions)

---

## Real-time and game loop

**1. Why make the server authoritative instead of trusting clients?**
If clients report their own position or score, a modified client can teleport or
award points. An authoritative server accepts only intent (clamped `dx,dy`) and
computes all state, so cheating is limited to choosing inputs. See
[ADR-0001](../adr/ADR-0001-authoritative-server.md).

**2. Why a fixed tick instead of updating per frame?**
A fixed tick (20 Hz, 50 ms budget) gives a deterministic, budgeted step
independent of wall-clock jitter, enabling replay and reproducible tests. Measured
tick p99 was 0.76 ms at 1,000 players. See [ADR-0001](../adr/ADR-0001-authoritative-server.md).

**3. How do you make a simulation deterministic, and why?**
Integer coordinates, iterate players in ascending ID order, break ties by lowest
ID, and draw randomness from a seeded PCG. `TestDeterministicReplay` replays 2,000
ticks × 20 players to an identical hash. Determinism enables replay and anti-cheat.
See [ADR-0001](../adr/ADR-0001-authoritative-server.md).

**4. Why WebSocket over UDP/HTTP for a browser game, and the trade-off?**
Browsers cannot do raw UDP; WebSocket gives a bidirectional TCP channel that is
easy to debug. The cost is TCP head-of-line blocking — one lost segment delays
later snapshots. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).

**5. How do you handle a slow client without stalling everyone?**
Each client has a capacity-1 "latest-wins" mailbox that replaces stale snapshots,
so a slow client costs one buffered item. See [ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).

**6. The mailbox was full of nothing yet clients were seconds behind — why?**
Kernel socket buffers (MiBs) absorbed hundreds of snapshots, so `WriteMessage`
never blocked and the capacity-1 mailbox always drained. Bounded app queues do not
bound end-to-end staleness when the kernel buffers underneath. The fix: clients
report their last processed tick and the server throttles laggers to 1 snap/s. See
[Experiment 001](../../experiments/001-week1-baseline/README.md).

**7. Why throttle a lagging client instead of disconnecting it?**
Cutting it off livelocked — a client receiving nothing can never catch up, and 5
bots were dropped. Throttling to 1 snapshot/s lets it recover. See
[ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).

**8. One goroutine per match owning state with no locks — why?**
Locks around shared world state make tick duration depend on contention, and a
blocking send to a slow client could stall the tick for everyone. A single actor
per match with bounded channels avoids both; a match uses at most one core. See
[ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).

**9. What saturates first under load — simulation or networking?**
Networking. Under 2,000 bots / 200 Hz / 1 CPU, simulation stayed 0.07 ms p99 while
tick start lag hit ~99 ms; 40% of CPU was in socket-write syscalls. A new
`tick_start_lag_seconds` metric exposed it. See [Experiment 001](../../experiments/001-week1-baseline/README.md).

**10. What is the bandwidth cost per player and why does it matter?**
~14.5 KB/s/player (≈725 B snapshot × 20 Hz); per-match bandwidth is O(n²) because
every player's snapshot grows with match size. Delta/binary encoding is the lever.
See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).

---

## Databases and concurrency

**11. How do you stop two buyers overspending or overselling the same item?**
Put invariants in the schema first (`CHECK (stock >= 0)`, `CHECK (credits >= 0)`)
and use a conditional update (`UPDATE ... WHERE stock > 0`) that checks and writes
atomically under the row lock. 200 buyers / 20 units sold exactly 20. See
[ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**12. A naive read-check-write path granted 200 items for 2 units — what happened?**
A lost update: every buyer read stock 20 and wrote back ~19, so no single write was
negative and `CHECK` couldn't catch it. Only atomic or locked writes prevent it.
See [Experiment 002](../../experiments/002-week2-persistence/README.md).

**13. Why did `FOR UPDATE` deadlock every buyer?**
Inserting the `purchases` row first takes a `FOR KEY SHARE` lock on the cosmetic
(foreign key). With 200 transactions holding KEY SHARE, upgrading to `FOR UPDATE`
waits on all the others — a deadlock cycle. `FOR NO KEY UPDATE` doesn't conflict
with KEY SHARE and fixed it. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**14. When optimistic vs pessimistic vs conditional locking?**
Conditional updates (fastest, 129 ms) for contended counters; optimistic (version
column) for lobbies where conflicts are rare and the read drives decisions;
pessimistic `FOR NO KEY UPDATE` when you must lock a row for a multi-step
transaction. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**15. How do you make a retried purchase safe (idempotency)?**
Claim `(player_id, Idempotency-Key)` with `INSERT ... ON CONFLICT DO NOTHING` in
the same transaction; a duplicate blocks on the unique index then replays the
stored result (HTTP 200 vs 201). 20 concurrent duplicates applied once. See
[ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**16. How do you prevent deadlocks between transactions?**
Always acquire locks in a fixed global order (cosmetic→profile, ascending player ID
for gifts). `TestGiftLockOrdering`: unordered gifts deadlocked, ordered gave 0. Also
retry `40P01`/`40001` with jittered backoff. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**17. Why PgBouncer in transaction mode, and what breaks?**
Postgres backends cost ~5–10 MB each, so N replicas × pool size can exhaust
`max_connections`. Transaction pooling multiplexes many clients onto few backends.
Session features (session SET, advisory locks, LISTEN) don't work through it, so
migrations bypass it. See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).

**18. How do you let a user read their own write from a replica (read-your-writes)?**
Return the write's LSN (`X-Write-LSN`), have the client echo it (`X-Read-After-LSN`),
and read the replica only once `pg_last_wal_replay_lsn() >= lsn` (wait up to 200 ms),
otherwise read the primary. See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).

**19. How did you find a missing index, and why wasn't the index enough?**
`EXPLAIN ANALYZE` showed a parallel seq scan of ~600k rows. Adding the index helped
(5.7 ms) but the planner still hash-joined all matches until the query was rewritten
to take the 20 newest rows first (CTE + LIMIT): p50 0.80 ms. See
[Experiment 002](../../experiments/002-week2-persistence/README.md).

**20. What is an N+1 query and how costly is it?**
Fetching a list then one query per item: 21 round trips vs 1 join cost ~10× on
loopback (7.75 vs 0.80 ms p50), and the gap grows linearly with latency over a real
network. See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).

**21. How do you do a zero-downtime schema change?**
Expand/contract: add nullable columns/indexes (old code still works), deploy code
writing both and reading new, backfill, then drop the old shape in a later release.
Migrate before rolling out code, never the reverse. See [ADR-0005](../adr/ADR-0005-migrations.md).

**22. Why forward-fix instead of running down-migrations in production?**
Expand migrations are backward compatible, so a bad release rolls back the *code*
safely. Dropping a column that received writes loses data, so schema problems are
fixed forward with a new migration. See [ADR-0005](../adr/ADR-0005-migrations.md).

**23. How do concurrent migrators avoid racing each other?**
goose takes a Postgres session advisory lock, so concurrent migrators serialize.
Migrations connect directly (not via the pooler) because the lock needs a session.
See [ADR-0005](../adr/ADR-0005-migrations.md).

**24. Why did DB failover take 3 minutes, and how was it cut to 1 second?**
Postgres smart shutdown waited for the pooler's open sessions (~180 s), and
PgBouncer then served a cached login error for 15 s. Setting
`smartShutdownTimeout: 10` and `server_login_retry: 1` cut the write outage to 1 s
(3 failed writes). See [Experiment 002](../../experiments/002-week2-persistence/README.md).

**25. How does the shard router minimize data movement on growth?**
Jump Consistent Hash: growing 2→3 shards moved exactly 33.3% of keys, all onto the
new shard. The cost is losing global `UNIQUE(username)`, which needs a directory.
See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**26. What does CAP mean for your database under a partition?**
A single primary keeps writes consistent; the replica stays available but may be
stale. The LSN token is how a client opts into consistency for its own writes. See
[ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).

---

## Traffic, caching and resilience

**27. How does one replica failing not cause an API outage?**
3 stateless replicas behind Envoy with active `/readyz` checks, passive outlier
ejection, and host-local retries. Kill/pause/graceful-stop drills all gave 0 failed
requests out of ~9,000. See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).

**28. Why is a hung replica worse than a crashed one?**
A crashed host refuses connections and is retried in milliseconds; a hung host
accepts then says nothing, so each request waits the full per-try timeout. Round
robin kept sending 1/3 of traffic there (p99 2,518 ms); least request routed away
(p99 12 ms), so it's the default. See [Experiment 003](../../experiments/003-week3-resilience/README.md).

**29. How do you order timeouts across layers?**
Innermost shortest: DB 2 s < Envoy per-try 2.5 s < API 3 s < route 4 s. A slow
shared DB then returns an API 503 instead of an Envoy timeout, so only a genuinely
hung replica is retried elsewhere. The wrong order caused 7,482 proxy retries. See
[ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).

**30. How do you stop retries from amplifying an outage?**
Only retry idempotent requests on another host, cap retries at a budget (20% of
active, min 10), use jittered backoff, and never retry 503s caused by a shared
dependency (tagged `x-envoy-ratelimited`). Backoff+budget cut offered load 58% in a
brownout. See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).

**31. How does a cache outage avoid losing data or taking down the API?**
Valkey is acceleration, not source of truth. On outage, profile/lobby reads fall
back to Postgres, rate limits go per-replica, and presence (Valkey-only) returns
503. Durable paths had 0 failures during a 10 s outage. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**32. Why a circuit breaker around the cache?**
After 5 consecutive failures it opens for 5 s, so calls fail in microseconds
instead of waiting for timeouts — a dead cache adds no latency. A cache miss or
caller cancellation doesn't count as a failure. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**33. How do you prevent a cache stampede?**
In-process `singleflight` collapses concurrent misses per key: a 1,000 rps hot key
caused 62 DB loads with it vs 650 without. A lease (`SET NX PX`) plus a Lua CAS
bounds stores and prevents a stale fill overwriting an invalidation. See
[ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**34. How do you invalidate a cache safely?**
Delete (not update) after the write commits, and delete the fill lease too, so an
in-flight fill that read the old value can't resurrect it. A missed invalidation
serves stale for at most the 30 s TTL, and purchases always re-check Postgres. See
[ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**35. How does edge asset caching avoid purges?**
Immutable content-hashed URLs cached a year; "invalidation" is publishing a new
hash plus a manifest with a short TTL. 200 concurrent cold requests caused 2 origin
fetches (`proxy_cache_lock`); cached assets stay served while the origin is down.
See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**36. How do rate limits hold across replicas?**
A token bucket in one Valkey Lua script, with the clock from Valkey `TIME` so
skewed replicas agree. On Valkey loss it falls back to per-replica limiting
(effective N× the rule) — neither fully fail-open nor fail-closed. See
[ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).

**37. Why must readiness reflect only this replica, not the database?**
When `/readyz` checked the DB, every replica went unready at once during failover,
removing all endpoints even for cache-served reads. Readiness now reflects only the
replica; DB state is reported as degraded. See [Experiment 003](../../experiments/003-week3-resilience/README.md).

---

## Events, Kafka and streaming

**38. How do you publish events without ever stalling the game tick?**
`Publish` is a non-blocking enqueue into a bounded buffer; a background pump calls
franz-go `TryProduce` (idempotent, `acks=all`, zstd). With all 3 brokers down for
60 s, tick p99 stayed 0.44 ms and 883 queued events = 883 distinct IDs in Kafka, 0
lost. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).

**39. Kafka is down — how do different event classes behave?**
Critical economy events commit to the Postgres outbox and publish later; durable
gameplay events spool to disk and replay; best-effort telemetry is dropped (342 of
606 in the outage run). See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).

**40. How do you avoid losing an event on a crash between DB commit and publish?**
The transactional outbox: write the state change and the event row in one
transaction, then let Debezium publish from the WAL. A crash mid-backlog published
200/200 with 0 lost, 0 duplicated. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).

**41. Why key match events by match ID, and what's the downside?**
Same key → same partition → per-match ordering. The downside is a hot partition: a
very active match concentrates all its records on one partition (24/24 in the test).
Player analytics must repartition by player ID. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).

**42. Duplicate delivery is unavoidable — how do you prevent double effects?**
Idempotent consumers, not exactly-once delivery: deterministic event IDs, reward
IDs derived from the causing event, state-guarded saga steps, and a leaderboard
built from "latest score per player per match" not a running sum. Replaying ~1.45 M
duplicates changed nothing. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).

**43. What does Kafka Streams exactly-once (EOS v2) actually cover?**
Kafka input consumption, state-store updates and output production commit
atomically — inside Streams only. The Postgres reward grant is a separate
idempotent consumer, not covered by EOS. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).

**44. How does a stream processor recover its state after loss?**
State lives in RocksDB backed by a Kafka changelog topic. Deleting both instances'
RocksDB volumes, the app returned to RUNNING in 10.9 s rebuilding from changelogs,
with identical totals. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).

**45. Why did Streams failover take 46 s and how was it fixed?**
The default consumer `session.timeout.ms` (45 s) decides how fast a dead instance
is noticed. Cut to 10 s with 1 s heartbeats → 11 s failover. See
[Experiment 004](../../experiments/004-week4-streaming/README.md).

**46. How do you handle a poison record without blocking the partition?**
Skip it and copy to a dead-letter topic (`game.invalid-events.v1`); both Streams
instances stayed RUNNING. For the saga, a command dead-letters after 5 retries to
`game.reward-commands-dlq.v1`. See [ADR-0012](../adr/ADR-0012-saga.md).

**47. How does a saga undo a partially-completed workflow?**
Orchestrated reserve→grant steps, each guarded by state in `reward_sagas`; a
permanent grant failure triggers compensation returning credits to the pool.
Transient failures go to a retry topic with backoff. Credits applied once:
459 → 459 after replay. See [ADR-0012](../adr/ADR-0012-saga.md).

**48. How are breaking schema changes caught before they ship?**
Apicurio enforces BACKWARD_TRANSITIVE compatibility; retyping
`PlayerScored.player_id` int64→string was rejected while an additive field was
accepted. CI runs the check against `main`. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).

**49. An abandoned replication slot retained WAL forever — how do you manage it?**
A slot whose consumer is gone retains WAL without bound (674 kB for 200 purchases
in the test). Alert on retained WAL and cap with `max_slot_wal_keep_size`. See
[ADR-0010](../adr/ADR-0010-outbox-cdc.md).

---

## Security and identity

**50. How did you stop clients acting as other players?**
Keycloak OIDC issues 5-minute RS256 JWTs; the API verifies issuer/audience/expiry
and maps the stable `sub` to a player, ignoring any `player_id` in the body. A
banned player's still-valid token is refused because bans are checked per request.
See [ADR-0013](../adr/ADR-0013-identity-secrets.md).

**51. How does a stateless gameserver authenticate players without a database?**
The API (after full auth + ban check) mints a 60 s HMAC game ticket
`base64url(player | expiry | HMAC)`; the gameserver verifies only the MAC. Short
TTL bounds a leaked secret's blast radius. See [ADR-0013](../adr/ADR-0013-identity-secrets.md).

**52. How does signing-key rotation work without downtime?**
The API fetches JWKS from the IdP; an unknown `kid` triggers a refetch, so a new
signing key is accepted with no restart and a retired key is rejected after refresh
(`TestKeyRotation`). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).

**53. Why put authorization in OPA instead of handlers, and which way does it fail?**
Rego policies are testable and reviewable on their own (default deny); the API
supplies facts, OPA supplies the rule. It fails closed: unreachable/non-200/
undefined/>200 ms → deny writes (503). An OPA outage stops purchases/bans but not
reading or playing. See [ADR-0014](../adr/ADR-0014-authorization-opa.md).

**54. How do you make webhooks safe against replay and tampering?**
HMAC-sign `t.delivery.body`, verify over the raw body with a constant-time compare
and a ±5 min window, then claim the delivery ID once in the recording transaction.
A replay returns 200 `replayed` and records once. See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).

**55. How do you defend the avatar importer against SSRF?**
A dial-time guard refuses http/file/gopher, loopback, `169.254.169.254`, private
ranges and odd ports (11 cases), plus byte-sniffed image types and size limits. See
[ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).

**56. Why isn't the WAF your real defense, and what caught the gap?**
WAF patterns are a noise filter; correctness relies on parameterized SQL,
`textContent` rendering, CSP and nosniff. Envoy's `safe_regex` first matched only
the path, missing `?q=<script>`; matching the `:path` header fixed it. See
[Experiment 005](../../experiments/005-week5-security/README.md).

---

## Observability and SRE

**57. How do you follow one request from a metric spike to the root cause?**
RED metrics (by route pattern) → exemplar/trace in Tempo → SQL child spans
(otelpgx) → logs carrying `trace_id`. In PM-001 a slow trace with *no SQL span*
pointed to a paused PgBouncer, not the database. See [ADR-0016](../adr/ADR-0016-observability.md).

**58. Why keep player IDs and raw paths out of metric/log labels?**
Unbounded label cardinality blows up Prometheus/Loki. High-cardinality data goes
into span attributes and log lines; `TestREDRouteLabels` proves raw paths never
become labels. See [ADR-0016](../adr/ADR-0016-observability.md).

**59. How do you alert so fast outages page but noise doesn't?**
Multi-window burn-rate alerts: page when 1 h **and** 5 m burn exceed 14.4×, ticket
at 6 h **and** 30 m over 6×. The short window resolves the alert soon after a fix.
Every alert has a runbook and a promtool test. See [ADR-0017](../adr/ADR-0017-slos-recovery.md).

**60. What are RPO and RTO for your database, and what sets them?**
RTO ~0.7 s (promotion). RPO is the replication lag at failure: 0 when healthy,
2.78 s of writes after a 5 s partition — a property of replication state, not the
tooling, so it must be monitored. Zero-RPO credits would need synchronous quorum
commit. See [ADR-0017](../adr/ADR-0017-slos-recovery.md).

**61. An SLI paged with no games running — why, and the fix?**
`1 - good/total` with zero ticks evaluates to 100% bad. Guard with a non-zero tick
rate and a promtool regression test; numerators use `or vector(0)`. See
[Experiment 006](../../experiments/006-week6-observability/README.md).

**62. How do you verify a backup is actually restorable?**
A repeatable drill: `pg_dump -Fc` of the live primary (1.8 MB in 0.49 s), restore
into a fresh container (0.61 s), then integrity-check row counts, constraints,
balances, partitions and migration version. See [ADR-0017](../adr/ADR-0017-slos-recovery.md).

---

## Delivery and Kubernetes

**63. How do you deploy the API with zero failed requests?**
Rolling update `maxSurge 1, maxUnavailable 0` with readiness gate and a 5 s drain:
1,672 ok / 0 failed. Canary, blue-green and rollback drills were also 0-failure.
See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).

**64. How does a failing migration not reach production pods?**
A `pre-install,pre-upgrade` hook Job (Argo CD `PreSync`) runs `migrate up`; a
failure fails the release before any new pod starts, and the old revision keeps
serving (836 ok / 0 failed, schema unchanged). See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).

**65. Why not autoscale game servers with an HPA?**
A game server holds live matches in memory, so scaling is allocation, not CPU:
scale-in must pick an *empty* server, scale-out follows free player slots, and
matchmaking must avoid draining servers. The API (stateless) does use HPA (2→5, 0
of 90,000 failed). See [ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).

**66. How does a game server avoid dropping players on deploy?**
On SIGTERM it flips readiness to 503, refuses new joins (close 1013), waits for
matches to finish, then exits — within `terminationGracePeriodSeconds` sized to
round length + margin. Drill: 0 of 20 bots dropped. See [ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).

**67. Why can only one controller deploy the app (Helm vs Argo CD)?**
Helm 4 uses server-side apply; after Argo CD synced, a manual `helm upgrade`
conflicted on field ownership. That's correct — one deployer per app. Drills pause
auto-sync explicitly. See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).

**68. Why did a rollback delete the pooler, and how is stateful data protected?**
`--rollback-on-failure` on a *first install* uninstalls the release. The CNPG
`Cluster` survived via `helm.sh/resource-policy: keep`; the Pooler lacked it.
Stateful resources need `keep`. See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).

**69. What belongs in IaC vs GitOps here?**
OpenTofu owns cluster-level platform (namespace, CNPG operator, metrics-server,
Argo CD) with pinned versions and `import` for existing releases; Argo CD owns the
application from Git. See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).

---

## Multi-region and operations

**70. Where does each kind of data live across regions?**
Live match state in the region's gameserver memory; accounts/credits with a single
home-region writer (async replicas elsewhere); profiles/presence/rate limits in
region-local Valkey; match events in region-local Kafka mirrored to an aggregate
cluster. See [ADR-0021](../adr/ADR-0021-multi-region.md).

**71. Why is a remote write so slow, and what would you change?**
A region B write took 805 ms vs 35 ms local — ~5 sequential round trips ×150 ms WAN
to the single writer. Fix with fewer round trips (one-statement CTEs, pipelining)
or moving write ownership to the player's home region. See [ADR-0021](../adr/ADR-0021-multi-region.md).

**72. What happens to each capability during an inter-region partition?**
Region B reads from its replica (35 ms) and gameplay is unaffected, but writes fail
fast with 503 in 53 ms — availability traded for no double-spend (CP economy, AP
reads/gameplay). See [ADR-0021](../adr/ADR-0021-multi-region.md).

**73. Why is client-side region selection faster failover than DNS?**
Clients sticking to a dead region caused 1,549 failed reconnects; re-running
health-based selection moved all 20 with 0 failures. DNS TTL 30 s left
resolver-cached clients on the old region ~34 s. DNS is a coarse lever. See
[Experiment 008](../../experiments/008-week8-regions/README.md).

**74. A regression went unnoticed for three weeks — how do you catch that class?**
Enabling tickets silently turned bots into guests, so no rewards were granted since
week 5. Component alerts were green. The fix is a business-outcome alert
(`LocalArenaNoRewardsGranted`) asserting the product works end to end, surfacing it
in ~15 min. See [ADR-0022](../adr/ADR-0022-operations.md).

**75. How do you make sure every layer has a health signal?**
Give each pipeline stage at least one cause alert (ticks, burn rate, pool/PgBouncer,
spool/dropped events, consumer lag, no-rewards), each with a runbook and promtool
test. Game days inject sealed overlapping faults to verify detection. See
[ADR-0022](../adr/ADR-0022-operations.md).

---

## Scale-up design questions

**76. "Design this": the snapshot path is O(n²) per match — how do you scale match size 100×?**
Stop sending full state: delta snapshots (only changes since the acked tick),
interest management (send only nearby entities via a spatial grid), and a binary
encoding (protobuf) instead of JSON. Server fan-out is the saturating resource
(Experiment 001), so also move encoding off the tick and batch socket writes; at
extreme scale shard a match across servers with authoritative hand-off at zone
borders. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md) and
[ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).

**77. "What would you change at 100× players" for the economy database?**
Shard accounts by player ID (Jump hash moves ~1/N on growth) and replace
`GENERATED ALWAYS` IDs with snowflake IDs; add a username directory/reservation
service to restore global uniqueness lost by sharding; keep purchases single-shard
with conditional updates; route cross-shard reads via scatter-gather; consider
Citus/Vitess to automate routing. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).

**78. "Design this": make the economy survive a region loss with zero data loss.**
Switch the home-region primary to `synchronous_commit = remote_apply` with a quorum
standby so an acknowledged credit is durable in ≥2 regions (RPO 0), accepting higher
write latency and writes stalling if the standby is down. Keep gameplay/reads AP.
Promotion stays an operator decision; today async gives RPO = replication lag. See
[ADR-0021](../adr/ADR-0021-multi-region.md) and [ADR-0017](../adr/ADR-0017-slos-recovery.md).

**79. "What would you change at 100×" for events and leaderboards?**
Increase partitions and sub-key very large matches (match/shard) to spread hot
partitions while keeping per-shard order; scale Streams instances and standby
replicas; compute per-region boards locally and a global board from
MirrorMaker-2-mirrored topics (eventually consistent); keep consumers idempotent so
replay and mirror duplicates are safe. See [ADR-0011](../adr/ADR-0011-kafka-streams.md)
and [ADR-0021](../adr/ADR-0021-multi-region.md).

**80. "Design this": matchmaking + game-server allocation for 10× concurrent matches.**
Replace HPA with a fleet controller (Agones-style): keep a buffer of ready servers,
allocate a match to a server with free slots, mark draining servers unready so
matchmaking stops sending to them, and scale in only empty servers. Scale on free
player slots, not CPU (CPU stays low until the tick budget is gone). See
[ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).

---

## How to use this

- **Design round:** sketch [`../00-architecture.md`](../00-architecture.md), then
  pick one topic above and go deep via its ADR link.
- **Debugging round:** questions 6, 13, 24, 28, 45, 49, 61, 74 are all real bugs
  with a measured root cause — rehearse the diagnosis path.
- **Rapid review:** read only the bold questions; each answer cites a number you
  can defend.
