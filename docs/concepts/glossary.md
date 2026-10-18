# Concept glossary (A–Z)

Every concept used across the 22 LocalArena ADRs, in one place. Each entry has a
plain explanation, an **In LocalArena** line linking to the deep dive that uses
it, and an **Also used in** line with one real-world example outside games.

Deep dives live in [`../adr/`](../adr). Evidence lives in
[`../../experiments/`](../../experiments). Numbers cited here are measured; where
a concept was not measured, the entry says so.

> Link convention: deep dives are `../adr/ADR-00NN-*.md`; experiments are
> `../../experiments/00N-*/README.md`.

---

## A

### acks=all (producer acknowledgement)
A Kafka producer setting where a write is acknowledged only after the leader and
all in-sync replicas have persisted it. It trades latency for durability: a write
cannot be lost to a single broker failure.
- **In LocalArena:** the non-blocking event producer uses `acks=all` with
  idempotence so match/economy events survive broker loss. See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a payments ledger publishing settlement events that must never silently vanish.

### active-active vs active-passive
Active-active means multiple sites accept writes simultaneously; active-passive
keeps one writer and promotes a standby on failure. Active-active needs conflict
resolution (CRDTs, last-writer-wins); active-passive is simpler but has failover
downtime.
- **In LocalArena:** the economy is active-passive (single home-region writer);
  active-active writes are explicitly out of scope. See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** a bank running a primary data center with a warm DR site.

### advisory lock (PostgreSQL)
An application-defined lock held in Postgres, not tied to any row. Session-level
advisory locks serialize arbitrary critical sections across processes.
- **In LocalArena:** goose takes a session advisory lock so concurrent migrators
  serialize instead of racing. See [ADR-0005](../adr/ADR-0005-migrations.md).
- **Also used in:** a cron job that must run on exactly one of many app instances.

### aggregate cluster (Kafka mirroring)
A central Kafka cluster that receives mirrored topics from regional clusters for
global analytics, keeping producers local and fast.
- **In LocalArena:** regional topics are mirrored by MirrorMaker 2 into an
  aggregate cluster that computes the global leaderboard. See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** a retailer aggregating per-region order streams into one warehouse topic.

### allocation (vs autoscaling)
Placing stateful work on a specific instance with capacity, rather than letting a
generic autoscaler add/remove pods arbitrarily. Needed when a pod holds
long-lived in-memory state.
- **In LocalArena:** game servers are allocation-driven (free slots, empty-server
  scale-in), not HPA-driven, because a pod holds live matches. See [ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).
- **Also used in:** placing a video transcoding job on a worker that already has the source cached.

### Apicurio (schema registry)
A registry that stores versioned event schemas and enforces compatibility rules
on new versions, rejecting breaking changes at publish time.
- **In LocalArena:** the protobuf `Envelope` is registered with
  `BACKWARD_TRANSITIVE` compatibility; a breaking change is rejected in CI. See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a microservice platform validating event-contract changes before deploy.

### Argo CD (GitOps)
A controller that continuously reconciles a Kubernetes cluster toward the desired
state declared in a Git repository, with automated sync, prune and self-heal.
- **In LocalArena:** Argo CD owns the app Deployment and self-heals drift in ~2 s
  (Experiment 007). See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).
- **Also used in:** a SaaS company deploying every service from a mono-repo of manifests.

### asynchronous replication
The primary acknowledges a write before the replica has applied it, so a failover
can lose the most recent committed transactions (RPO > 0).
- **In LocalArena:** Postgres uses async streaming replication; failover lost 0
  writes when healthy and 2.78 s of writes after a 5 s partition (Experiment 006).
  See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a read-replica fleet for an e-commerce catalog where slight lag is tolerable.

### at-least-once delivery
A messaging guarantee where every message is delivered one or more times;
consumers must be idempotent to tolerate duplicates.
- **In LocalArena:** Debezium and Kafka consumers are at-least-once, so consumers
  dedupe by `event_id`. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** an email-sending pipeline that may retry and must not send twice.

### audience (JWT `aud`)
A JWT claim naming the intended recipient service. Verifiers reject tokens whose
audience does not match, preventing a token minted for one service being replayed
at another.
- **In LocalArena:** access tokens carry `aud=localarena-api`; the API rejects
  tokens with any other audience. See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** an API gateway rejecting a token issued for an internal admin console.

### authoritative server
A design where the server owns all game/business state and clients send only
intent; the server validates and decides, so a tampered client cannot cheat.
- **In LocalArena:** the server owns position, score and outcome; clients send
  only `dx,dy` intent, clamped by the engine. See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** a trading system where the matching engine, not the client, decides fills.

### autoscaling (HPA)
Automatically changing the number of replicas based on a metric (usually CPU).
Correct for stateless workloads; wrong for ones holding in-memory state.
- **In LocalArena:** the stateless API scales 2→5 on CPU with 0 failures of
  90,000 requests (Experiment 007); game servers do not. See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** a web storefront adding pods during a flash sale.

---

## B

### backpressure
Signalling or behaviour that keeps a fast producer from overwhelming a slow
consumer — by blocking, dropping, or throttling rather than queueing without
bound.
- **In LocalArena:** inputs use a bounded channel that drops when full; snapshots
  use a capacity-1 mailbox that replaces stale state. See [ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).
- **Also used in:** a video pipeline dropping frames when the encoder falls behind.

### backup and restore
Taking a consistent copy of durable data and proving it can be restored into a
fresh system with verified integrity.
- **In LocalArena:** `pg_dump -Fc` of a live primary (1.8 MB in 0.49 s) restored
  into a fresh container in 0.61 s with all integrity checks passing (Experiment
  006). See [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** a hospital records system running nightly verified restores for compliance.

### BACKWARD_TRANSITIVE compatibility
A schema-evolution rule: a new schema version can read data written by any prior
version. Additive optional fields pass; removing or retyping a field is rejected.
- **In LocalArena:** Apicurio enforces it on the event `Envelope`; changing
  `PlayerScored.player_id` from int64 to string was rejected (Experiment 004). See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** an analytics pipeline that must keep reading a year of historical events.

### ban (authorization fact)
A per-user flag that revokes access immediately, checked on every request rather
than baked into a token so it takes effect before the token expires.
- **In LocalArena:** bans live in Postgres and are checked per request; a banned
  player's still-valid token is refused (Experiment 005). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** a marketplace suspending a seller account mid-session for fraud.

### blue-green deployment
Running two full environments (blue = current, green = next) and switching traffic
by flipping a selector; rollback is flipping back, with near-zero cutover time.
- **In LocalArena:** a green Deployment behind the same Service; the selector
  switch took 0.08 s with 0 failed requests (Experiment 007). See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** a bank releasing a new web front end with instant rollback capability.

### bounded queue
A queue with a fixed capacity, forcing an explicit policy (block, drop, or
replace) when full rather than growing until memory is exhausted.
- **In LocalArena:** the match input channel holds 1,024 entries and drops on
  overflow (`inputs_total{result="queue_full"}`). See [ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).
- **Also used in:** a logging agent with a fixed ring buffer that drops under pressure.

### brute-force protection
Locking or slowing authentication after repeated failures to resist password
guessing.
- **In LocalArena:** Keycloak brute-force protection is enabled on the realm. See
  [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** a bank login throttling after several wrong PIN attempts.

### burn-rate alerting
Paging based on how fast an error budget is being consumed over multiple time
windows, so fast outages page quickly and slow ones resolve soon after a fix.
- **In LocalArena:** pages fire when 1 h **and** 5 m burn rates exceed 14.4×;
  tickets at 6 h **and** 30 m over 6× (Google SRE workbook). See [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** a streaming service alerting on how quickly its availability budget drains.

### business-outcome alert
An alert that asserts the product works end to end ("players who finish matches
get credits"), not merely that components are up.
- **In LocalArena:** `LocalArenaNoRewardsGranted` would have caught the week-5
  regression where bots became guests and no rewards were granted (PM-002). See
  [ADR-0022](../adr/ADR-0022-operations.md).
- **Also used in:** an e-commerce alert on "orders placed per minute" dropping to zero despite healthy services.

---

## C

### cache-aside
A pattern where the app reads the cache first, and on a miss loads from the
database and populates the cache. The cache is disposable acceleration, not the
source of truth.
- **In LocalArena:** profiles (TTL 30 s) and lobbies (TTL 10 s) are cached aside
  in Valkey; fills read the primary. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a product-detail page caching catalog rows in front of a SQL database.

### cache invalidation
Removing or versioning a cached value when the underlying data changes.
Delete-after-commit avoids writing a stale value back into the cache.
- **In LocalArena:** profile/lobby entries are deleted (not updated) after the
  write commits; a lease CAS prevents an in-flight fill overwriting the
  invalidation. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a CMS purging a cached article after an edit.

### cache stampede (thundering herd)
When a hot key expires and many concurrent requests all miss and hit the database
at once. Collapsing the concurrent loads prevents a load spike.
- **In LocalArena:** `singleflight` collapsed a 1,000 rps hot-key stampede to 62
  DB loads vs 650 without (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a news site when a viral article's cache entry expires under peak traffic.

### canary deployment
Releasing a new version to a small slice of traffic first, observing it, then
promoting or rolling back.
- **In LocalArena:** one canary pod plus two stable pods took ~31% of traffic
  (replica-weighted) with 0 failures (Experiment 007). See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** a social app shipping a new feed ranker to 5% of users.

### CAP theorem
Under a network partition, a distributed system must choose between consistency
(reject to stay correct) and availability (serve possibly-stale data).
- **In LocalArena:** during a region partition the economy chooses CP (writes 503
  to avoid double-spend) while reads/gameplay stay AP. See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** a distributed key-value store choosing availability for a shopping cart.

### causation ID / correlation ID
IDs threaded through events and requests. A correlation ID groups all work for one
logical operation; a causation ID points to the event that directly caused this
one.
- **In LocalArena:** every event Envelope carries `correlation_id` and
  `causation_id`; traces propagate W3C `traceparent`. See [ADR-0016](../adr/ADR-0016-observability.md).
- **Also used in:** tracing a customer order across checkout, payment and shipping services.

### CDC (change data capture)
Reading a database's commit log to turn committed row changes into a stream of
events, without the application dual-writing.
- **In LocalArena:** Debezium reads the Postgres WAL via `pgoutput` and publishes
  outbox rows to Kafka. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** replicating an orders table into a search index in near real time.

### CDN / edge cache
Caching static assets close to users at edge nodes, with content-hashed URLs so
updates publish a new URL and never need a purge.
- **In LocalArena:** Garage (S3-compatible) behind two NGINX caching proxies with
  immutable content-hashed URLs; 200 concurrent cold requests caused only 2 origin
  fetches (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** serving a web app's hashed JS/CSS bundles from a CDN.

### circuit breaker
A wrapper that stops calling a failing dependency after a threshold of errors,
failing fast while open, then probing with one request before closing.
- **In LocalArena:** `gobreaker` around Valkey opens after 5 consecutive failures,
  stays open 5 s, then admits one probe; a dead cache adds microseconds, not
  latency. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a checkout service short-circuiting a flaky fraud-scoring API.

### clamping (input validation)
Forcing an input into a legal range before using it, so malformed or malicious
values cannot drive out-of-bounds behaviour.
- **In LocalArena:** the engine clamps `dx,dy` so `dx=100` still moves at normal
  speed. See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** a slider API clamping a requested page size to a maximum.

### clock skew
Clocks on different machines disagree, so wall-clock timestamps cannot be trusted
for ordering. Use monotonic durations, logical ticks, or a single clock source.
- **In LocalArena:** client `ct` timestamps are diagnostic only; ordering uses
  server ticks, and the rate limiter uses Valkey `TIME` so replicas agree. See
  [ADR-0001](../adr/ADR-0001-authoritative-server.md) and [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a distributed log where event order cannot rely on each host's clock.

### compacted topic (Kafka log compaction)
A topic that retains at least the latest value per key forever, acting as a
durable changelog or materialized table.
- **In LocalArena:** profile and rules tables, and `game.leaderboard-updates.v1`,
  are compacted so the latest value per key survives. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a config-distribution topic holding the current value per setting.

### compensation (saga)
An action that semantically undoes a completed saga step when a later step fails,
since there is no distributed transaction to roll back.
- **In LocalArena:** if a reward grant permanently fails, the saga compensates by
  returning reserved credits to the pool. See [ADR-0012](../adr/ADR-0012-saga.md).
- **Also used in:** refunding a charged card when the subsequent inventory reservation fails.

### conditional update (compare-and-set in SQL)
A single SQL statement that checks and writes atomically under the row lock
(`UPDATE ... WHERE stock > 0`), eliminating the read-modify-write race.
- **In LocalArena:** the default purchase path; fastest strategy measured (200
  buyers / 20 units in 129 ms vs 832 ms optimistic, 984 ms pessimistic; Experiment
  002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** decrementing coupon stock in one atomic statement at checkout.

### connection pooling
Reusing a bounded set of database connections across many requests, because each
backend connection costs memory and the database caps `max_connections`.
- **In LocalArena:** pgxpool (10/process) plus PgBouncer in transaction mode;
  pool waits are bounded by a 3 s context and surface as 503. See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a web app multiplexing thousands of HTTP requests over a few dozen DB connections.

### constant-time comparison
Comparing secrets (MACs, tokens) in a way that does not reveal information through
timing, defeating timing side-channel attacks.
- **In LocalArena:** webhook HMAC verification uses a constant-time compare. See
  [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** verifying a password-reset token or API signature.

### constraint-first (invariants in the schema)
Encoding business invariants as database constraints (CHECK, UNIQUE, PK) so no
code path, however buggy, can commit an invalid state.
- **In LocalArena:** `CHECK (credits >= 0)`, `CHECK (stock >= 0)`, unique
  idempotency keys and lobby PKs guarantee no overspend or double-join even if
  code is wrong. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a banking ledger with a non-negative balance constraint.

### CORS (cross-origin resource sharing)
Browser-enforced rules controlling which origins may call an API from script. An
allow-list restricts callers to trusted origins.
- **In LocalArena:** CORS is an explicit allow-list; CSRF is not applicable because
  auth is a bearer header, not cookies. See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** a public JSON API allowing only its own web front end's origin.

### CSP (Content Security Policy)
An HTTP header telling the browser which content sources are allowed, mitigating
XSS by blocking inline/injected scripts.
- **In LocalArena:** responses set CSP `default-src 'none'` plus `nosniff`. See
  [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** a web app locking script execution to its own hashed bundles.

### CTE (common table expression) / LIMIT-first
A `WITH` query that materializes an intermediate result; applying `LIMIT` before a
join lets the planner fetch only the needed rows via an index.
- **In LocalArena:** rewriting the history query to take the 20 newest rows first
  cut p50 from ~12 ms to 0.80 ms (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** paginating a user's recent transactions efficiently.

---

## D

### DLQ (dead-letter queue)
A separate topic for records that cannot be processed after bounded retries or
cannot be decoded, so a poison record does not block the partition.
- **In LocalArena:** reward commands go to `game.reward-commands-dlq.v1` after 5
  attempts; undecodable events go to `game.invalid-events.v1` (Experiment 004).
  See [ADR-0012](../adr/ADR-0012-saga.md).
- **Also used in:** a payment webhook processor parking malformed callbacks for manual review.

### deadlock
Two or more transactions each hold a lock the other needs, so none can proceed;
the database detects the cycle and aborts a victim.
- **In LocalArena:** `FOR UPDATE` deadlocked all 200 buyers (FK `KEY SHARE` vs
  upgrade); `TestDeadlockIsDetected` reproduces A→B / B→A (Experiment 002). See
  [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** two bank transfers locking accounts in opposite order.

### Debezium
A CDC platform (Kafka Connect connectors) that streams database row changes into
Kafka, with a replication slot tracking its position.
- **In LocalArena:** the Postgres connector publishes outbox rows via the
  EventRouter SMT; a crash mid-backlog lost 0 and duplicated 0 (Experiment 004).
  See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** syncing a legacy database into a new microservice's event stream.

### delta compression (snapshots)
Sending only what changed since the last acknowledged state instead of full
state, to cut bandwidth.
- **In LocalArena:** full snapshots make per-match bandwidth O(n²); delta/binary
  encoding is the named optimization but not implemented. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** a collaborative editor sending edit deltas instead of the whole document.

### deprecation / sunset (API)
Signalling via HTTP headers that an old API version is going away and when, so
clients can migrate before it is removed.
- **In LocalArena:** `/v1` is frozen and, when enabled, carries `Deprecation`,
  `Sunset: 2027-01-31` and a successor `Link`. See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** a public REST API announcing a v1 shutdown date.

### determinism (deterministic simulation)
Given the same inputs and seed, the system always produces the same state —
enabling replay, reproducible tests and anti-cheat.
- **In LocalArena:** integer coords, ascending-ID iteration, seeded PCG RNG;
  `TestDeterministicReplay` replays 2,000 ticks × 20 players to an identical hash.
  See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** a risk engine replaying a trading day to reproduce a result exactly.

### deterministic event ID
An event ID computed from stable inputs (hash of run/match/tick/kind) so a
re-emitted event has the same ID and consumers dedupe it.
- **In LocalArena:** event IDs and reward IDs are deterministic; replaying 1.45 M
  duplicates changed nothing (Experiment 004). See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** generating an idempotency key from an order's natural key.

### disruption budget (PDB)
A Kubernetes policy capping how many pods of a workload can be voluntarily
disrupted at once, protecting availability during drains/upgrades.
- **In LocalArena:** the API has a PDB `minAvailable 1`. See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** keeping a quorum of a stateful service up during a node drain.

### distributed lock
A lock held across processes to serialize a critical section. Lease-based locks
need fencing tokens to stay safe when the holder stalls.
- **In LocalArena:** the only cross-process lock is the Postgres advisory lock for
  migrations; correctness never depends on an unfenced lease. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** ensuring only one worker runs a scheduled batch job.

### DNS TTL and failover
DNS caches answers for the record's TTL, so changing where a name points takes
effect only after caches expire — making DNS a coarse, slow failover lever.
- **In LocalArena:** TTL 30 s gave ~34 s before resolver-cached clients moved
  (Experiment 008); fast failover comes from client health-based selection. See
  [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** repointing a public hostname to a DR region during an outage.

### drain (graceful shutdown)
On shutdown, stop accepting new work, finish in-flight work, then exit, so a
deploy or scale-in does not drop active sessions.
- **In LocalArena:** API fails `/readyz` for 5 s then `Shutdown`; game servers
  refuse joins and wait for matches to end (0 of 20 bots dropped, Experiment 007).
  See [ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).
- **Also used in:** a web server finishing open requests before a rolling deploy terminates it.

### dual write (anti-pattern)
Writing to two systems (DB then Kafka) without a shared transaction; a crash
between them loses or duplicates data. The outbox pattern fixes it.
- **In LocalArena:** rejected in favour of the transactional outbox for economy
  events. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** the classic "save order, then publish OrderCreated" bug in microservices.

---

## E

### edge (gateway / reverse proxy)
A single ingress that terminates TLS, routes, load-balances, health-checks and
applies edge security in front of many backend replicas.
- **In LocalArena:** Envoy is the edge: TLS, WAF-style deny route, global rate
  limit, load balancing and retries. See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md)
  and [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** an API gateway fronting dozens of microservices.

### error budget
The allowed amount of unreliability (1 − SLO) over a window; it is spent by
incidents and governs how aggressively to alert and release.
- **In LocalArena:** SLOs define 30-day budgets; burn-rate alerts page when the
  budget drains too fast. See [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** an SRE team pausing feature releases when the budget is exhausted.

### eventual consistency
Replicas converge to the same value over time, so a read right after a write may
return a stale value until replication catches up.
- **In LocalArena:** a match recorded via region B was invisible in B until its
  replica caught up (Experiment 008). See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** a social feed where a new post appears to followers with a short delay.

### exactly-once (Kafka Streams EOS v2)
A guarantee that, within a Kafka-to-Kafka Streams app, input consumption, state
updates and output production commit atomically — no duplicates, no loss. It does
not extend to external side effects.
- **In LocalArena:** Streams use `exactly_once_v2`; the Postgres reward grant is a
  separate idempotent consumer, not covered by EOS. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** a fraud-scoring pipeline aggregating events without double counting.

### expand/contract migration
A zero-downtime schema change done in phases: expand (add nullable
columns/indexes), deploy code, backfill, then contract (drop the old shape) in a
later release.
- **In LocalArena:** every column-touching change is expand/contract; old code
  works on the new schema in between (Experiment 002). See [ADR-0005](../adr/ADR-0005-migrations.md).
- **Also used in:** renaming a column in a live SaaS database without downtime.

### exponential backoff with jitter
Retrying after a delay that grows exponentially and is randomized, to avoid
synchronized retry storms that hammer a recovering dependency.
- **In LocalArena:** `inTx` retries deadlocks/serialization with jittered backoff;
  the saga retry topic uses backoff; backoff+budget cut offered load 58% in a
  brownout (Experiment 003). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a mobile client retrying a flaky API after a timeout.

---

## F

### fail closed / fail open
When a dependency is unavailable, fail closed denies the action (safe for
security); fail open allows it (safe for availability). The direction is a
per-dependency decision.
- **In LocalArena:** OPA unavailable → deny writes (fail closed); flagd
  unavailable → default flag; Valkey down → degrade. See [ADR-0014](../adr/ADR-0014-authorization-opa.md).
- **Also used in:** a door-access system that locks (fail closed) vs a fire door that unlocks (fail open) on power loss.

### failover
Promoting a standby to primary when the primary fails, moving writes to the new
leader.
- **In LocalArena:** CNPG promotes via a Kubernetes lease; DB RTO ~0.7 s, write
  outage reduced from ~3 min to 1 s after tuning (Experiment 002/006). See
  [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a managed database service promoting a replica after an AZ failure.

### feature flag
A runtime switch that enables/disables or gradually rolls out a feature without a
deploy, evaluated with a safe default on error.
- **In LocalArena:** flagd via OFREP; `game-mode` is a 20% sticky fractional
  rollout keyed by round ID; flagd outage freezes at the default (Experiment 005).
  See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** dark-launching a new checkout flow to 1% of users.

### fencing token
A monotonically increasing token handed out with a lock so a stalled old holder's
writes are rejected after a newer holder takes over.
- **In LocalArena:** cited as the reason Redis/Valkey locks are avoided for
  purchases; the design never depends on an unfenced lease. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a leader writing to shared storage after a GC pause that lost its lease.

### fixed-tick loop
Advancing a simulation in fixed time steps (a tick) rather than per-frame, giving
a deterministic, budgeted update independent of wall-clock jitter.
- **In LocalArena:** 20 Hz, 50 ms budget; tick p99 0.76 ms at 1,000 players
  (Experiment 001). See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** a control-loop/PLC updating actuators at a fixed rate.

### FOR NO KEY UPDATE vs FOR UPDATE
Two Postgres row-lock strengths. `FOR UPDATE` is exclusive and conflicts with the
`KEY SHARE` that foreign keys take; `FOR NO KEY UPDATE` is weaker and does not,
avoiding FK-induced deadlocks.
- **In LocalArena:** switching to `FOR NO KEY UPDATE` fixed a deadlock that killed
  all 200 buyers under `FOR UPDATE` (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** locking an inventory row that is also referenced by order foreign keys.

### forward-fix (rollback policy)
Rolling back the *code* (safe because migrations are expand-only) and fixing
schema problems with a new migration, rather than running `Down` in production.
- **In LocalArena:** `Down` is for dev/drills; production fixes forward. See
  [ADR-0005](../adr/ADR-0005-migrations.md).
- **Also used in:** a team that never runs down-migrations against production.

---

## G

### game day
A planned exercise injecting randomized, sealed, overlapping faults under load to
test detection, mitigation and runbooks.
- **In LocalArena:** the final game day drew 3 of 6 faults; all diagnosed, and it
  surfaced the no-rewards regression (PM-002, Experiment 008). See [ADR-0022](../adr/ADR-0022-operations.md).
- **Also used in:** a bank running quarterly resilience drills on its payment rails.

### game ticket (stateless auth handoff)
A short-lived signed token minted by a stateful service (after full auth) so a
stateless service can verify identity with only a MAC, no database.
- **In LocalArena:** `base64url(player | expiry | HMAC-SHA256)`, valid 60 s; the
  gameserver verifies only the MAC (Experiment 005). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** a signed upload URL letting a client talk directly to object storage.

### GitOps
Managing infrastructure and app deployment declaratively from Git, with a
controller continuously reconciling the cluster to the repo.
- **In LocalArena:** Argo CD reconciles the app; manual `helm upgrade` conflicts
  with Argo's field ownership by design (Experiment 007). See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).
- **Also used in:** a platform team deploying every change through pull requests.

### GlobalKTable
A Kafka Streams table fully replicated to every instance, so any key can be joined
locally without repartitioning.
- **In LocalArena:** the rules/anti-cheat table is modelled as a GlobalKTable in
  the architecture. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** joining a stream of events against a small, fully-replicated reference table.

### graceful degradation
Continuing to serve reduced functionality when a dependency fails, instead of
failing entirely.
- **In LocalArena:** Valkey down → profile/lobby reads from Postgres, rate limits
  per-replica, presence 503 (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a shopping site hiding recommendations when the recommender is down but still selling.

---

## H

### head-of-line blocking
In an ordered stream (TCP), one delayed/lost item delays everything behind it.
- **In LocalArena:** TCP HOL blocking delays later snapshots after a lost segment;
  a UDP comparison is a separate experiment. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** HTTP/1.1 pipelining stalling behind one slow response (fixed by HTTP/2 streams).

### health check (readiness / liveness)
Probes the orchestrator and load balancer use to decide whether to send traffic
(readiness) or restart a pod (liveness). Readiness must reflect only this
instance, not shared dependencies.
- **In LocalArena:** `/readyz` reflects only the replica after a bug where a
  DB-dependent readiness removed all endpoints at once (Experiment 003). See
  [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a load balancer ejecting an unhealthy app instance.

### hedging
Issuing a second (speculative) request after a per-try timeout to beat tail
latency, at the cost of extra load.
- **In LocalArena:** named as a possible follow-up for GETs to bound round-robin
  hang latency; not implemented (Experiment 003). See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a search backend sending a backup request to cut p99 latency.

### Helm chart
A templated, versioned package of Kubernetes manifests with values for different
environments.
- **In LocalArena:** one chart `infra/helm/localarena` packages the app plus
  dependencies; stateful resources carry `resource-policy: keep`. See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** distributing a database operator's install as a chart.

### HMAC
A keyed hash used to authenticate a message and detect tampering; only holders of
the secret can produce or verify it.
- **In LocalArena:** game tickets and webhook signatures use HMAC-SHA256,
  constant-time compared. See [ADR-0013](../adr/ADR-0013-identity-secrets.md) and
  [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** Stripe/GitHub webhook signature verification.

### hot partition
A Kafka partition (or shard) that receives disproportionate traffic because of its
key, becoming a bottleneck.
- **In LocalArena:** one very active match keyed by match ID puts all its records
  on one partition — the price of per-match ordering (Experiment 004). See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a celebrity user's ID concentrating writes on one database shard.

---

## I

### idempotency key
A client-supplied key that makes a retried write have the same effect as one
request, so network retries cannot duplicate side effects.
- **In LocalArena:** purchases claim `(player_id, Idempotency-Key)` with
  `INSERT ... ON CONFLICT DO NOTHING`; 20 concurrent duplicates applied once
  (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** Stripe's `Idempotency-Key` header on charge creation.

### immutable / content-hashed URL
A URL whose path includes a hash of the content, so the content never changes for
a given URL and can be cached forever; updates publish a new URL.
- **In LocalArena:** assets like `/assets/skin-gold.8a5d0cbba780.svg` are cached
  one year `immutable`; "invalidation" is publishing a new hash. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** webpack/Vite emitting hashed bundle filenames for long-term caching.

### in-sync replica (ISR) / min.insync.replicas
The set of replicas caught up to the leader; `min.insync.replicas` sets how many
must ack an `acks=all` write, trading availability for durability.
- **In LocalArena:** RF 3, `min.insync.replicas=2`; losing 2 of 3 brokers stops
  writes by design and producers spool (Experiment 004). See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a Kafka cluster tuned to never lose an acknowledged event.

### infrastructure as code (IaC)
Declaring infrastructure in version-controlled files applied by a tool, for
reproducibility and review.
- **In LocalArena:** OpenTofu owns namespace, CNPG operator, metrics-server and
  Argo CD, with pinned versions and `import` for existing releases (Experiment
  007). See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).
- **Also used in:** a team provisioning cloud VPCs and databases with Terraform.

### interactive queries (Kafka Streams)
Querying a stream processor's local state store directly over HTTP, forwarding to
the instance that owns the key or scatter-gathering across all.
- **In LocalArena:** `/leaderboard/{id}` forwards to the key owner; `/top`
  scatter-gathers. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** reading a real-time aggregation (e.g. per-user counters) without a separate database.

### idempotent producer (Kafka)
A producer that dedupes retried sends within a session using sequence numbers, so
a retry does not append a duplicate record.
- **In LocalArena:** franz-go idempotent producer with `acks=all`. See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** any exactly-once Kafka producer configuration.

---

## J

### Jump Consistent Hash
A hashing algorithm that maps a key to one of N buckets so that growing from N to
N+1 moves only ~1/N of keys, all onto the new bucket.
- **In LocalArena:** the shard router uses it; 2→3 shards moved 33.3% of keys, all
  to the new shard (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** sharding a cache or storage layer with minimal reshuffling on growth.

### JWKS (JSON Web Key Set)
The published set of public keys an IdP uses to sign tokens; verifiers fetch it to
validate signatures and refetch on an unknown key ID to support rotation.
- **In LocalArena:** the API fetches JWKS from the internal URL; an unknown `kid`
  triggers a refetch so key rotation needs no restart (Experiment 005). See
  [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** any OIDC resource server validating access tokens.

### JWT (JSON Web Token)
A signed, self-describing token carrying claims (subject, roles, audience,
expiry) that a service verifies without calling the issuer.
- **In LocalArena:** 5-minute RS256 access tokens verified for issuer, audience
  and expiry (Experiment 005). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** stateless session auth across a microservice fleet.

---

## K

### Kafka (KRaft mode)
A durable, partitioned event log; KRaft runs the metadata quorum on Kafka's own
controllers instead of ZooKeeper.
- **In LocalArena:** Kafka 4.3 KRaft, 3 combined broker+controller nodes, RF 3,
  auto-create off. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** an event backbone connecting services at a large retailer.

### KStream / KTable
Kafka Streams abstractions: a KStream is an unbounded record stream; a KTable is a
changelog interpreted as the latest value per key.
- **In LocalArena:** `PlayerScored` is streamed, re-keyed and reduced into a KTable
  of latest-score-per-player-per-match. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** maintaining a live materialized view of account balances from a transaction stream.

### KRaft quorum / controller
The Raft-based metadata consensus among Kafka controllers that elects partition
leaders and tracks ISR without ZooKeeper.
- **In LocalArena:** combined broker+controller nodes run the KRaft quorum. See
  [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** modern Kafka clusters removing the ZooKeeper dependency.

---

## L

### latest-wins mailbox
A capacity-1 slot per client that always replaces an unread value with the newest,
so a slow consumer costs one buffered item and never delays producers.
- **In LocalArena:** each client has a capacity-1 snapshot mailbox
  (`snapshots_total{result="replaced"}`). See [ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).
- **Also used in:** a dashboard that only ever shows the most recent sensor reading.

### leader election
Choosing one node to act as the single writer/coordinator, using a lease or
consensus so there are never two leaders (split brain).
- **In LocalArena:** CNPG elects the new primary via a Kubernetes lease; two
  operator pods use leader election (Experiment 003). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a distributed scheduler picking one active coordinator.

### least-request load balancing
Routing each request to the backend with the fewest outstanding requests (power of
two choices), which routes away from a stuck host quickly.
- **In LocalArena:** least request is the API default; against a hung replica p99
  was 12 ms vs 2,518 ms for round robin (Experiment 003). See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** balancing requests across backends with uneven per-request cost.

### lease (cache fill)
A short-lived marker a cache filler takes so only one filler populates a key, and
a compare-and-set ensures a stale fill cannot overwrite a newer invalidation.
- **In LocalArena:** `SET key:lease token NX PX 2000` plus a Lua CAS;
  `TestInvalidationBeatsInFlightFill` proves the ordering. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** preventing a cache fill from resurrecting just-deleted data.

### LSN (log sequence number) / read-your-writes
A Postgres WAL position. A client echoes the LSN of its write and reads a replica
only once that replica has replayed past it, otherwise reads the primary — so a
user always sees their own write.
- **In LocalArena:** writes return `X-Write-LSN`; reads send `X-Read-After-LSN`;
  the API waits up to 200 ms or falls back to the primary (Experiment 002). See
  [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a social app ensuring you see your own comment immediately after posting.

### lock ordering
Always acquiring multiple locks in a fixed global order so cyclic waits — and thus
deadlocks — cannot form.
- **In LocalArena:** locks taken cosmetic→profile, then ascending player ID for
  gifts; `TestGiftLockOrdering` shows ordered = 0 deadlocks (Experiment 002). See
  [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** transferring between two bank accounts by locking the lower ID first.

### long polling
A client holds an HTTP request open until there is new data or a timeout, then
immediately reconnects — a simple near-real-time pattern over plain HTTP.
- **In LocalArena:** status is available via long poll (`?since=V&wait=`), compared
  with SSE and WebSocket. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** a chat app's fallback when WebSockets are blocked.

### lost update
Two read-modify-write operations overwrite each other, so one update is silently
lost; constraints alone cannot catch it.
- **In LocalArena:** the naive purchase path granted 200 items for 2 units of
  stock (Experiment 002); conditional/locked writes prevent it. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** two admins editing the same record and one change vanishing.

---

## M

### materialized view (state store / changelog)
A precomputed, queryable result kept up to date by a stream processor, backed by a
local state store and a Kafka changelog topic for recovery.
- **In LocalArena:** the `leaderboard-totals` store is materialized and backed by a
  changelog; a full state wipe restored in 10.9 s (Experiment 004). See
  [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** a real-time analytics dashboard reading a continuously-updated aggregate.

### migration (schema)
A versioned, ordered change to the database schema, applied forward and (in dev)
reversible, serialized so concurrent migrators do not race.
- **In LocalArena:** goose SQL migrations embedded in the binary, serialized by an
  advisory lock, run before code rollout. See [ADR-0005](../adr/ADR-0005-migrations.md).
- **Also used in:** evolving a production schema across releases with Flyway/Liquibase.

### MirrorMaker 2
A Kafka tool that replicates topics between clusters, prefixing with the source
name and translating offsets for consumer failover.
- **In LocalArena:** the design mirrors `region-a.*` / `region-b.*` into an
  aggregate cluster; prefixes stop mirror loops (not deployed). See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** replicating events from regional Kafka clusters into a central analytics cluster.

### monotonic time
A clock that never goes backward, used for measuring durations safely even when
the wall clock is adjusted.
- **In LocalArena:** simulation ordering uses ticks and monotonic durations, not
  wall-clock reads. See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** measuring request latency without NTP adjustments corrupting it.

### multi-window / multi-burn-rate alert
Combining a long and a short window so an alert fires only on sustained burn and
resolves quickly once fixed, cutting both missed incidents and flapping.
- **In LocalArena:** page requires 1 h **and** 5 m windows over 14.4×. See
  [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** the standard Google SRE availability alerting setup.

---

## N

### N+1 query
Fetching a list, then issuing one query per item, producing N+1 round trips where
a single join would do.
- **In LocalArena:** 21 round trips cost ~10× a single join (7.75 vs 0.80 ms p50);
  `TestHistoryNPlusOneVsJoin` asserts 21 vs 1 statement (Experiment 002). See
  [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** an ORM lazily loading each order's line items in a loop.

### non-blocking producer
Publishing by a non-blocking enqueue into a bounded buffer that a background pump
drains, so the hot path never waits on the broker.
- **In LocalArena:** the game tick does only a non-blocking channel send; tick p99
  stayed 0.44 ms with all brokers down (Experiment 004). See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** an app emitting audit events without blocking user requests.

### nosniff / X-Content-Type-Options
A header that stops browsers from MIME-sniffing a response into a different,
dangerous content type (e.g. treating data as HTML/script).
- **In LocalArena:** responses set `nosniff` alongside CSP; avatar bytes are
  re-sniffed server-side. See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** serving user uploads so they cannot execute as HTML.

---

## O

### OIDC (OpenID Connect)
An identity layer over OAuth2 that issues verifiable ID/access tokens; the
authorization-code + PKCE flow is the browser-safe variant.
- **In LocalArena:** Keycloak realm with authorization code + PKCE for the web
  client (Experiment 005). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** "Sign in with ..." across consumer and enterprise apps.

### OPA (Open Policy Agent) / Rego
A policy engine evaluating declarative Rego rules against a decision input,
decoupling authorization logic from application code.
- **In LocalArena:** `policies/localarena.rego` returns `{allow, reason}`, default
  deny; the API supplies facts, OPA supplies the rule (Experiment 005). See
  [ADR-0014](../adr/ADR-0014-authorization-opa.md).
- **Also used in:** admission control and API authorization in a Kubernetes platform.

### optimistic locking
Reading a version, writing with `WHERE version = read`, and retrying on zero rows.
Cheap when conflicts are rare and the read informs several decisions.
- **In LocalArena:** used for lobbies; 206 optimistic retries in the 200-buyer
  contention test (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a wiki saving an edit only if the page hasn't changed since load.

### outbox pattern (transactional outbox)
Writing the state change and an event row in one database transaction, then
publishing the event from the outbox via CDC/poller — eliminating dual-write loss.
- **In LocalArena:** economy writes insert an outbox row in the same transaction;
  Debezium publishes it; a crash between commit and publish lost nothing
  (Experiment 004). See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** reliably emitting `OrderPlaced` exactly with the order row.

### outlier detection
A proxy automatically ejecting a backend that returns too many consecutive errors
or gateway failures, with a cap so it never drops all hosts.
- **In LocalArena:** Envoy ejects after 5 consecutive 5xx / 3 gateway failures, at
  most 34% of hosts, with a panic threshold. See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a service mesh removing a degraded instance from rotation.

---

## P

### parameterized query
Sending SQL and user values separately so input is data, never code — the
definitive defense against SQL injection.
- **In LocalArena:** parameterized SQL everywhere; `1 OR 1=1` is rejected as data
  (Experiment 005). See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** every well-built login form and search box.

### partition (database) / partition pruning
Splitting a large table by a key (e.g. month) so queries touch only relevant
partitions, and maintenance targets one partition at a time.
- **In LocalArena:** `matches` is monthly-partitioned; a one-month predicate
  scanned only `matches_2026_11` (111 buffers, Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** time-series tables keeping recent partitions hot and archiving old ones.

### partition (network)
A failure where parts of a system cannot communicate, forcing a CAP choice between
consistency and availability.
- **In LocalArena:** a region B↔A partition makes B writes 503 in 53 ms (CP) while
  reads/gameplay continue (AP) (Experiment 008). See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** a cross-AZ link failure in a distributed database.

### partition (Kafka) / key
The unit of parallelism and ordering in a topic; records with the same key go to
the same partition, preserving per-key order.
- **In LocalArena:** match events keyed by match ID for per-match order; player
  events keyed by player ID. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** keying payment events by account to keep per-account order.

### PCG (seeded RNG)
A pseudo-random generator seeded from a known value so sequences are reproducible;
not for security.
- **In LocalArena:** the world owns a seeded PCG; `math/rand/v2` is deliberate (not
  `crypto/rand`) for reproducibility. See [ADR-0001](../adr/ADR-0001-authoritative-server.md).
- **Also used in:** reproducible Monte-Carlo simulations and test fixtures.

### PgBouncer (transaction pooling)
A lightweight connection pooler; in transaction mode a server connection is
assigned only for the duration of a transaction, multiplexing many clients onto
few backends.
- **In LocalArena:** `default_pool_size=10`, `query_wait_timeout=5s`; migrations
  bypass it because they need a session (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** fronting Postgres for a serverless app with many short-lived connections.

### PKCE
A proof-key extension to the OAuth authorization-code flow that stops an
intercepted code being exchanged by an attacker — required for public clients.
- **In LocalArena:** the web client uses authorization code + PKCE S256
  (Experiment 005). See [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** mobile and SPA logins with any OIDC provider.

### poison record
A message that cannot be processed (undecodable or always-failing); it must be
routed aside so it does not block the partition forever.
- **In LocalArena:** garbage bytes are skipped and copied to
  `game.invalid-events.v1`; both Streams instances stayed RUNNING (Experiment 004).
  See [ADR-0012](../adr/ADR-0012-saga.md).
- **Also used in:** a Kafka consumer parking a malformed order event in a DLQ.

### pool exhaustion
All connections in a pool are busy, so new requests wait; with a bounded wait they
fail fast instead of queueing without limit.
- **In LocalArena:** a 3 s context bounds pool waits to a 503; `TestPoolExhaustion`
  and `TestPgBouncerQueueWaitTimeout` cover both layers (Experiment 002). See
  [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a web service under a traffic spike where DB connections run out.

### presence (ephemeral state)
Short-lived "who is online" data kept only in a cache; it is intentionally not
persisted, and on cache loss it reads "unknown" rather than loading the database.
- **In LocalArena:** presence lives only in Valkey; heartbeats 503 when Valkey is
  down to avoid a load spike on Postgres (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a chat app's online-status indicators.

### Prometheus (pull metrics) / RED
A metrics system that scrapes targets; RED = Rate, Errors, Duration per endpoint,
with bounded-cardinality labels (route pattern, not raw path).
- **In LocalArena:** Prometheus scrapes services directly; RED labelled by route
  pattern; `TestREDRouteLabels` ensures raw paths never become labels. See
  [ADR-0016](../adr/ADR-0016-observability.md).
- **Also used in:** monitoring a microservice fleet's latency and error rates.

### protobuf (Protocol Buffers)
A compact, typed, versioned binary serialization format with an IDL, used for
event contracts and RPC.
- **In LocalArena:** one protobuf `Envelope` with a `oneof` payload; chosen to
  match the Go/gRPC stack. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** gRPC service definitions across a polyglot backend.

---

## Q

### query optimization (EXPLAIN ANALYZE)
Reading the planner's actual execution plan to find seq scans, bad join orders and
missing indexes, then rewriting the query or adding indexes.
- **In LocalArena:** `EXPLAIN ANALYZE` revealed a seq scan of 600k rows and that an
  index alone wasn't enough without LIMIT-first (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** diagnosing a slow report query in any SQL database.

### QUIC / WebTransport
A UDP-based transport with independent streams that avoids TCP head-of-line
blocking; browser/library maturity is still limited.
- **In LocalArena:** considered and deferred as too large for Week 1. See
  [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** HTTP/3 and low-latency media transport.

---

## R

### rate limiting (token bucket)
Capping request rate per identity using a bucket that refills at a fixed rate and
allows short bursts; a shared store makes the limit hold across replicas.
- **In LocalArena:** a Valkey Lua token bucket (purchases 2/s burst 5 per player;
  account creation 5/s burst 20 per IP), with a per-replica fallback when Valkey is
  down (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** an API capping requests per API key per second.

### read replica
A copy of the database that serves reads to offload the primary; reads may be
stale by the replication lag.
- **In LocalArena:** an async streaming replica serves reads, gated by LSN
  read-your-writes (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** scaling read-heavy catalog or reporting workloads.

### rebalancing (consumer group)
Reassigning partitions among consumers when membership changes; it briefly pauses
processing, so faster failure detection shortens it.
- **In LocalArena:** cutting `session.timeout.ms` to 10 s took Streams failover
  from 46 s to 11 s (Experiment 004). See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** a Kafka consumer fleet redistributing partitions when a node dies.

### replication factor (RF)
How many copies of each partition/shard exist across brokers/nodes, setting how
many failures can be tolerated without data loss.
- **In LocalArena:** Kafka RF 3; Garage ran RF 1 (no durability) because it holds
  only re-publishable artifacts. See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** a storage system keeping 3 replicas for durability.

### replication slot (WAL retention)
A Postgres object that tracks a consumer's WAL position and retains WAL until
consumed; an abandoned slot retains WAL without bound.
- **In LocalArena:** Debezium's slot retained 674 kB during an outage; an orphaned
  slot needs alerting and `max_slot_wal_keep_size` (Experiment 004). See
  [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** logical replication to a data warehouse that must not miss changes.

### replay (event sourcing)
Reprocessing historical events to rebuild state or a projection, relying on
idempotent consumers so replay is safe alongside live traffic.
- **In LocalArena:** leaderboard state is rebuilt from changelogs; replaying 1.45 M
  duplicate events changed nothing (Experiment 004). See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** rebuilding a search index or read model from an event log.

### replay protection (webhooks)
Rejecting a resent signed request using a timestamp window plus a one-time
delivery ID claimed in the same transaction as the effect.
- **In LocalArena:** `±5 min` window plus a `webhook_deliveries` unique claim; a
  replayed delivery returns 200 `replayed` and records once (Experiment 005). See
  [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** Stripe/GitHub webhook receivers deduping redeliveries.

### resource policy keep (Helm)
A Helm annotation that prevents a resource being deleted on uninstall/rollback,
protecting stateful data.
- **In LocalArena:** the CNPG `Cluster` has `resource-policy: keep`; the `Pooler`
  lacked it and was deleted on a failed first install (Experiment 007). See
  [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** protecting a PersistentVolumeClaim across chart operations.

### retry budget
A cap on the fraction of in-flight requests that may be retries, so a dependency
failure cannot be amplified into a retry storm.
- **In LocalArena:** Envoy caps retries at 20% of active requests (min 10); raising
  the floor to 10 fixed 27 failed requests on replica kill (Experiment 003). See
  [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a resilient RPC client limiting retry amplification.

### retry storm
A cascade where retries multiply load on an already-struggling dependency, making
the outage worse; backoff + jitter + budget contain it.
- **In LocalArena:** naive retries offered 446 attempts/s (4.04× amplification) vs
  188/s with backoff+budget during a brownout (Experiment 003). See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a mobile fleet hammering a recovering backend after an outage.

### reverse proxy
A server that sits in front of backends, forwarding client requests and handling
TLS, routing and caching.
- **In LocalArena:** Envoy (API edge) and NGINX (asset edges) are reverse proxies.
  See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** NGINX in front of application servers at nearly every web company.

### RocksDB (state store)
An embedded key-value store Kafka Streams uses for local state, backed by a Kafka
changelog so it can be rebuilt after loss.
- **In LocalArena:** Streams state in RocksDB; needs a glibc runtime image; a full
  wipe restored in 10.9 s (Experiment 004). See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** stateful stream processors and embedded databases.

### rolling update
Replacing pods a few at a time with surge/unavailable limits and readiness gates
so there is no capacity dip or outage during a deploy.
- **In LocalArena:** `maxSurge 1, maxUnavailable 0` plus a 5 s drain gave 0 failed
  requests over 1,672 (Experiment 007). See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** zero-downtime deploys of a stateless web service.

### row lock
A lock on a specific database row that serializes writers to it; combined with a
conditional predicate it prevents lost updates on a contended counter.
- **In LocalArena:** conditional updates do the check under the row lock in one
  statement (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** decrementing a shared inventory counter correctly.

### RPO / RTO
Recovery Point Objective (how much recent data you may lose) and Recovery Time
Objective (how long to restore service). Async replication gives RPO > 0.
- **In LocalArena:** DB RTO ~0.7 s; RPO 0 when healthy, 2.78 s of writes after a
  5 s partition (Experiment 006). See [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** defining DR targets for a payments database.

### round-robin load balancing
Distributing requests cyclically across backends regardless of load; simple but
keeps sending to a stuck host until health checks eject it.
- **In LocalArena:** kept on `:10001` for comparison; against a hung host p99 hit
  the per-try timeout (2,518 ms) vs 12 ms for least request (Experiment 003). See
  [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a basic DNS or L4 balancer across stateless web servers.

---

## S

### saga
A sequence of local transactions across services with compensations, used instead
of a distributed transaction; an orchestrated saga drives the steps centrally.
- **In LocalArena:** the reward saga runs reserve → grant, compensating on
  permanent failure; credits applied once under duplicates (Experiment 004). See
  [ADR-0012](../adr/ADR-0012-saga.md).
- **Also used in:** an order → payment → shipping flow that must undo on partial failure.

### sampling (tracing)
Recording only a fraction of traces to bound overhead and storage; parent-based
sampling keeps a trace whole.
- **In LocalArena:** parent-based sampling at ratio 1.0 in the lab; tracing is a
  no-op when the OTLP endpoint is unset. See [ADR-0016](../adr/ADR-0016-observability.md).
- **Also used in:** a high-traffic service sampling 1% of requests for tracing.

### scatter-gather
Querying all shards/instances in parallel and merging results, used for queries
that cannot be answered from a single partition.
- **In LocalArena:** cross-shard `TopCredits` and the Streams `/top` endpoint
  scatter-gather (Experiment 002/004). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a federated search fanning a query across index shards.

### schema evolution / versioning
Changing data/API contracts over time while keeping old and new consumers working;
additive changes are safe, breaking changes get a new version.
- **In LocalArena:** additive-only within a version; events gated by Apicurio,
  protocol by a version field, API by `/v1` vs `/v2`. See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** evolving a public API and its event contracts without breaking clients.

### secrets management (SOPS + age)
Encrypting secrets in the repository so only holders of a private key can decrypt,
keeping plaintext secrets out of Git.
- **In LocalArena:** SOPS + age; `.sops.yaml` holds the public recipient, the
  private key stays local; secrets decrypt to a mode-600 file (Experiment 005). See
  [ADR-0013](../adr/ADR-0013-identity-secrets.md).
- **Also used in:** storing environment secrets alongside GitOps manifests.

### self-heal (GitOps)
A controller detecting manual drift from the declared state and reverting it
automatically.
- **In LocalArena:** Argo CD restored a scaled-to-zero flagd in 2 s (Experiment
  007). See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).
- **Also used in:** reverting an unauthorized production change back to the Git-declared config.

### server-side apply (field ownership)
Kubernetes tracking which controller owns which fields, so two controllers editing
the same object conflict — enforcing one deployer per resource.
- **In LocalArena:** a manual `helm upgrade` conflicted with Argo CD's ownership
  (Experiment 007 F1). See [ADR-0020](../adr/ADR-0020-iac-ci-gitops.md).
- **Also used in:** preventing two CI pipelines from fighting over the same Deployment.

### session window
A stream-processing window that groups events separated by less than a gap into
one session, closing after the gap plus a grace period.
- **In LocalArena:** player sessions use a 30 s gap with 10 s grace on event time,
  suppressed until the window closes. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** grouping a user's web clicks into browsing sessions.

### SSE (server-sent events)
A one-way server→client stream over a long-lived HTTP response; simpler than
WebSocket when the client does not need to push.
- **In LocalArena:** status is offered via SSE (`/status/stream`), compared with
  long poll and WebSocket. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** a live stock-ticker or notification feed to the browser.

### sharding
Splitting data across independent databases by a key to scale writes; it trades
away cross-shard constraints and joins.
- **In LocalArena:** a Jump-hash router prototype; global `UNIQUE(username)` is
  lost across shards and needs a directory (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** partitioning user data by user ID at large scale.

### singleflight
Collapsing concurrent identical in-process calls into one, so duplicate work (like
a cache fill) happens once per process.
- **In LocalArena:** per-key singleflight cut hot-key DB loads from 650 to 62
  (Experiment 003). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** deduping concurrent identical downstream calls in a gateway.

### SLI / SLO
A Service Level Indicator is a measured quality (e.g. % fast requests); an SLO is
the target for it. They drive error budgets and alerting.
- **In LocalArena:** API availability 99.5%, latency 99% < 300 ms, ticks 99.9% <
  50 ms, events 99% delivered. See [ADR-0017](../adr/ADR-0017-slos-recovery.md).
- **Also used in:** a cloud provider's published uptime commitments.

### SMT (single message transform)
A per-record transformation in Kafka Connect, such as EventRouter which reshapes
outbox rows into routed event records.
- **In LocalArena:** Debezium's EventRouter SMT publishes the envelope bytes keyed
  by `aggregateid`. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** renaming/routing fields as CDC records flow into Kafka.

### snowflake ID
A sortable, globally-unique ID generated without a central coordinator (time +
machine + sequence), useful once identity leaves `GENERATED ALWAYS`.
- **In LocalArena:** named as the replacement for global sequence IDs once sharding
  removes `GENERATED ALWAYS`. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** generating tweet/message IDs across many servers.

### split brain
Two nodes both believing they are the writer during a partition, risking divergent
state; prevented by leader election with a lease/quorum.
- **In LocalArena:** a Kubernetes lease prevents two Postgres primaries during
  failover (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a clustered database guarding against two active masters.

### SQL injection
Injecting SQL through unsanitized input; prevented by parameterized queries that
keep input as data.
- **In LocalArena:** parameterized SQL everywhere; `UNION SELECT` is also blocked
  at the WAF as defense in depth (Experiment 005). See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** any app accepting search terms or login fields.

### SSRF (server-side request forgery)
Tricking a server into making requests to internal addresses; blocked by
validating/denying the resolved destination at dial time.
- **In LocalArena:** the avatar importer refuses http/file/gopher, loopback,
  link-local `169.254.169.254`, private ranges and odd ports (11 cases, Experiment
  005). See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** blocking a URL-preview feature from hitting cloud metadata endpoints.

### startup / readiness / liveness probes
Kubernetes probes: startup gates the others while a slow boot finishes, readiness
controls traffic, liveness restarts a wedged pod.
- **In LocalArena:** the API defines all three plus a readiness-off drain. See
  [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** any production Kubernetes Deployment.

### stale read
Reading an out-of-date value from a replica or cache that has not yet caught up to
the latest write.
- **In LocalArena:** `TestStaleReplicaReadAndReadYourWrites` pauses WAL replay to
  reproduce it deterministically (Experiment 002). See [ADR-0004](../adr/ADR-0004-postgres-pooling-replicas.md).
- **Also used in:** a dashboard reading a lagging analytics replica.

### STRICT_DNS service discovery
Resolving a service name to its set of backend IPs via DNS and load-balancing
across them, re-resolving as membership changes.
- **In LocalArena:** Envoy finds API replicas via `STRICT_DNS` on the Compose
  service name. See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a proxy discovering backend pods behind a headless Service.

### suppression (Kafka Streams)
Holding back a windowed result until the window closes so only one final value is
emitted, rather than every intermediate update.
- **In LocalArena:** session windows are suppressed until close before emitting.
  See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** emitting one finalized hourly aggregate instead of a stream of partials.

---

## T

### tail latency (p95 / p99)
The slow end of the latency distribution; averages hide it, and it dominates user
experience, so SLOs target percentiles.
- **In LocalArena:** tick p50/p95/p99 0.07/0.21/0.76 ms at 1,000 players; a hung
  replica pushed API p99 to 2,518 ms under round robin (Experiments 001/003). See
  [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** an SLA promising p99 latency for an API.

### TargetDown
A monitoring alert that fires when a scrape target stops responding — a basic
per-component liveness signal.
- **In LocalArena:** `TargetDown` detected a killed gameserver in 46 s on game day
  (Experiment 008). See [ADR-0022](../adr/ADR-0022-operations.md).
- **Also used in:** alerting when any monitored service's metrics endpoint goes silent.

### termination grace period
The time Kubernetes waits after SIGTERM before SIGKILL, sized so stateful work
(like a game round) can finish draining.
- **In LocalArena:** `terminationGracePeriodSeconds` = round length + margin (45 s
  dev / 150 s default); drain timeout is grace − 5 s. See [ADR-0018](../adr/ADR-0018-gameserver-lifecycle.md).
- **Also used in:** letting a worker finish a long task before the pod is killed.

### throttling (application flow control)
Deliberately slowing output to a client that cannot keep up, instead of cutting it
off, so it can still catch up.
- **In LocalArena:** a client >1 s behind is throttled to 1 snapshot/s (not cut
  off); cutting off livelocked in run 1 (Experiment 001). See [ADR-0003](../adr/ADR-0003-match-actor-backpressure.md).
- **Also used in:** an API slowing a client that exceeds its quota rather than dropping it.

### timeout budget (innermost shortest)
Nesting timeouts so each inner layer's timeout is shorter than its caller's,
preventing a slow dependency from being wrongly retried as a hung host.
- **In LocalArena:** DB 2 s < Envoy per-try 2.5 s < API 3 s < route 4 s; the wrong
  order caused 7,482 proxy retries (Experiment 003). See [ADR-0007](../adr/ADR-0007-edge-timeouts-retries.md).
- **Also used in:** a request chain across microservices with coordinated deadlines.

### TLS (termination, HSTS)
Encrypting traffic in transit; terminating TLS at the edge and setting HSTS to
force HTTPS.
- **In LocalArena:** Envoy terminates TLS 1.2+ on `:8443` with HSTS; TLS 1.1 is
  refused (Experiment 005). See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** HTTPS termination at any public load balancer.

### token bucket
See **rate limiting**. A bucket of tokens refilling at a steady rate; each request
consumes one, and a burst is allowed up to the bucket size.
- **In LocalArena:** Valkey Lua token bucket plus an Envoy global local bucket
  (1000/s, burst 2000). See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** API quota enforcement and traffic shaping.

### Toxiproxy / tc netem
Fault-injection tools that add latency, loss, bandwidth limits and partitions to
network paths for resilience testing.
- **In LocalArena:** Toxiproxy simulates inter-region WAN latency (A↔B 150 ms) and
  partitions (Experiment 008). See [ADR-0021](../adr/ADR-0021-multi-region.md).
- **Also used in:** chaos testing a service's behaviour under packet loss.

### trace / span (distributed tracing)
A trace is the end-to-end record of one request as a tree of spans across
services; it lets you follow a request from metric to SQL to logs.
- **In LocalArena:** OTel traces with otelhttp/otelpgx spans; a purchase trace
  shows BEGIN→INSERT→SELECT→COMMIT SQL spans (Experiment 006). See [ADR-0016](../adr/ADR-0016-observability.md).
- **Also used in:** diagnosing which downstream service made a checkout slow.

### transaction isolation (SERIALIZABLE)
Isolation levels control what concurrent transactions can see; SERIALIZABLE makes
them behave as if run one at a time, at the cost of retries on conflict.
- **In LocalArena:** considered as an option for complex multi-row rules; the
  default is targeted locks/constraints. See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** a booking system preventing double-booking under concurrency.

---

## U

### UDP vs TCP
UDP is connectionless and unordered (no head-of-line blocking) but unreliable; TCP
is reliable and ordered. Games often prefer UDP for latency.
- **In LocalArena:** WebSocket (TCP) is chosen for Week 1 debuggability; a native
  UDP bot comparison is a separate experiment. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** VoIP/video (UDP) vs file transfer (TCP).

### unique constraint (idempotency / membership)
A database uniqueness rule that makes a second conflicting insert fail, enforcing
"at most one" invariants like one active lobby or one applied idempotency key.
- **In LocalArena:** `lobby_members PRIMARY KEY (player_id)` and
  `UNIQUE (player_id, idempotency_key)`; one player joining 10 lobbies succeeds once
  (Experiment 002). See [ADR-0006](../adr/ADR-0006-concurrency-sharding.md).
- **Also used in:** enforcing one vote per user per poll.

---

## V

### Valkey
An open-source Redis fork used as a cache, presence store, rate-limit backend and
leaderboard store — disposable acceleration, never the source of truth.
- **In LocalArena:** Valkey backs cache-aside, token-bucket limits and presence; an
  outage degrades features but loses no durable data (Experiment 003). See
  [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** a session/cache layer in front of a relational database.

### version segment (cache key)
Embedding a schema version in the cache key (`profile:v1:<id>`) so a shape change
can roll out without flushing the whole cache.
- **In LocalArena:** profile/lobby cache keys carry a `v1` segment. See
  [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** versioning cached serialized objects during a rollout.

---

## W

### WAF (web application firewall)
Edge rules that reject obviously malicious request patterns (traversal, injection,
JNDI); a noise filter, not a substitute for application controls.
- **In LocalArena:** an Envoy deny route on the `:path` header; it first missed
  query-string attacks because `safe_regex` matched only the path (Experiment 005).
  See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** a cloud WAF filtering common exploit traffic before the app.

### WAL (write-ahead log)
Postgres's durable log of changes, replayed for crash recovery, streamed for
replication and read by logical decoding for CDC.
- **In LocalArena:** `wal_level=logical`; replicas stream WAL; Debezium decodes it;
  LSN positions gate read-your-writes. See [ADR-0010](../adr/ADR-0010-outbox-cdc.md).
- **Also used in:** point-in-time recovery and logical replication in Postgres.

### WebSocket
A persistent, bidirectional TCP channel over an HTTP upgrade, suited to real-time
browser communication.
- **In LocalArena:** gameplay uses WebSocket with a readPump/writePump per
  connection and ping/pong deadlines. See [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** a collaborative editor or live chat in the browser.

### webhook
An HTTP callback a system sends to notify another of an event; secured with a
signature, timestamp window and delivery-ID dedupe.
- **In LocalArena:** `POST /v2/webhooks/match-results` is HMAC-signed,
  replay-protected and claimed once in the recording transaction (Experiment 005).
  See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** payment providers notifying merchants of completed charges.

### window / grace period / late events (stream time)
Event-time windows group records by when they happened; a grace period admits
late-arriving events before the window finalizes.
- **In LocalArena:** session windows use event time with a 10 s grace; the
  timestamp extractor reads event time from the Envelope. See [ADR-0011](../adr/ADR-0011-kafka-streams.md).
- **Also used in:** computing hourly metrics correctly when events arrive out of order.

### cross-site WebSocket hijacking / Origin check
A WebSocket attack where another site opens a socket using the victim's cookies;
checking the `Origin` header against an allow-list prevents it.
- **In LocalArena:** the `/ws` upgrade requires a matching `Origin`. See
  [ADR-0002](../adr/ADR-0002-protocol-websocket.md).
- **Also used in:** any cookie-authenticated WebSocket endpoint.

---

## X

### XSS (cross-site scripting)
Injecting script that runs in another user's browser; prevented by treating user
text as data (escaping/`textContent`), CSP and `nosniff`.
- **In LocalArena:** display names are Unicode-validated, JSON-encoded and rendered
  with `textContent`; CSP `default-src 'none'` (Experiment 005). See [ADR-0015](../adr/ADR-0015-api-evolution-edge-security.md).
- **Also used in:** safely rendering user comments on any web page.

### X-Forwarded-For
A header chain of client IPs added by proxies; only the proxy-appended rightmost
hop is trustworthy for per-IP decisions.
- **In LocalArena:** the rate limiter uses the rightmost `X-Forwarded-For` appended
  by Envoy for per-IP account-creation limits. See [ADR-0008](../adr/ADR-0008-caching-ratelimit-breaker.md).
- **Also used in:** per-client rate limiting behind a load balancer.

---

## Z

### zstd compression (producer)
A fast compression codec applied to Kafka producer batches to cut network and
storage at low CPU cost.
- **In LocalArena:** the event producer uses zstd with 20 ms linger for batching.
  See [ADR-0009](../adr/ADR-0009-kafka-contracts-producer.md).
- **Also used in:** compressing log/event streams into Kafka or object storage.

### zero-downtime deploy
Releasing without an outage, combining rolling/canary/blue-green strategies,
readiness gates, drains and expand/contract migrations.
- **In LocalArena:** every delivery drill recorded 0 failed requests (Experiment
  007). See [ADR-0019](../adr/ADR-0019-delivery-strategies.md).
- **Also used in:** continuous deployment at any always-on web service.
