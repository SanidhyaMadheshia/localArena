# LocalArena: 8-Week Multiplayer Backend and System Design Plan

> **Run it:** [GETTING_STARTED.md](GETTING_STARTED.md) (`make play`, then http://localhost:8080). **Study it:** [docs/](docs/README.md). **Results:** [FINAL_REPORT.md](FINAL_REPORT.md).

## 1. Mission

Build **LocalArena**, a locally hosted, server-authoritative 2D multiplayer arena, while learning the 111 backend/system-design topics in the accompanying curriculum. Everything runs on one Linux computer using open-source software; no cloud account is required.

The game remains mechanically simple so the backend stays central:

- 10–20 players per match
- Movement, collectible items, scoring, and optional tagging/shooting
- Public matchmaking and private lobbies
- Lobby/match chat and presence
- Profiles, fictional credits, cosmetic inventory, and leaderboards
- Match history and replay events
- Automated bot clients for load generation
- Two simulated geographic regions during the final week

This plan provides broad practical exposure to all 111 topics, not mastery of each. The proof of learning is a working implementation plus repeatable experiments, metrics, design decisions, runbooks, and postmortems.

## 2. Final success criteria

By the end of week 8, LocalArena should:

1. Run locally through Docker Compose and through a local Kubernetes cluster.
2. Use an authoritative fixed-tick game server; clients send input, never authoritative positions or scores.
3. Support at least 200 local bot connections and 10 concurrent matches, then document the machine's actual saturation point.
4. Keep game ticks within a 50 ms budget at the selected baseline (20 Hz), with p50/p95/p99 evidence.
5. Prevent duplicate matchmaking, rewards, and cosmetic purchases.
6. Continue serving API traffic after one stateless replica fails.
7. Degrade safely when cache, queue, or a downstream service is unavailable.
8. Expose useful logs, metrics, traces, dashboards, and alerts.
9. Deploy with health probes, resource limits, rolling updates, a canary, and rollback.
10. Demonstrate backup/restore, failure injection, regional failover simulation, and a written postmortem.

Do not compare localhost throughput directly with internet-scale systems. Measure relative behavior, resource cost per player/match, failure propagation, and correctness.

## 3. Technology choices

| Concern | Default choice | Purpose |
|---|---|---|
| Game/API backend | Go | Networking, concurrency, profiling, race detection |
| Stream processors | Java 21 or Kotlin + Kafka Streams | Stateful event processing on the JVM |
| Browser client | Minimal TypeScript + Canvas | Visualization only; avoid frontend scope creep |
| Real-time transport | WebSocket | Main client/game-server connection |
| Protocol experiment | UDP native bots | Compare reliability and latency with TCP |
| Internal RPC | gRPC + Protocol Buffers | Typed service-to-service APIs |
| Durable data | PostgreSQL + PgBouncer | Profiles, inventory, purchases, match metadata |
| Cache/presence | Valkey | Presence, rate limits, leaderboards, ephemeral state |
| Event backbone | Apache Kafka in KRaft mode | Durable events, partitions, replay, consumer groups |
| Event processing | Kafka Streams | Windows, joins, aggregations, state stores, exactly-once v2 |
| Event contracts | Protobuf/Avro + Apicurio Registry | Versioned schemas and compatibility checks |
| CDC/integration | Debezium + Kafka Connect | PostgreSQL outbox and change-data capture |
| Kafka inspection | Kafka UI | Topics, partitions, offsets, records, and consumer lag |
| Proxy/gateway | Envoy | Routing, balancing, health checking, resilience |
| Assets | MinIO + NGINX cache | Maps, skins, replay files, edge-cache simulation |
| Load tests | k6 + custom Go bots | HTTP tests plus realistic game sessions |
| Fault injection | Toxiproxy + `tc netem` | Latency, loss, disconnections, bandwidth limits |
| Observability | OpenTelemetry, Prometheus, Grafana, Loki, Tempo, Alertmanager | Metrics, logs, traces, alerts |
| Identity/policy | Keycloak + Open Policy Agent | OAuth/OIDC, JWT, IAM policies |
| Secrets | SOPS + `age` | Encrypted configuration in the repository |
| Feature flags | OpenFeature + flagd | Controlled feature rollout |
| Local orchestration | kind or k3d | Local Kubernetes |
| Delivery | Helm + Argo CD | Packaging and GitOps deployment |
| Infrastructure code | OpenTofu | Open-source Terraform-compatible HCL workflow |
| Security tests | OWASP ZAP, Trivy, Semgrep/gosec | Application, image, and source scanning |

Start only the components required by the current week. Do not run the entire final stack on day one. On an 8 GB machine, run observability components selectively and prefer k3d. Sixteen GB is comfortable; 32 GB helps with two local clusters.

## 4. Architecture

```text
Browser clients / Go bot swarm
              |
        WebSocket / UDP
              |
           Envoy
              |
   +----------+-----------+----------------+
   |                      |                |
API/Auth              Lobby/Matchmaker  Game servers
   |                      |          In-memory match actors
   |                      |                |
PostgreSQL ---------- Valkey/etcd          | asynchronous events
   |                                       v
   +-- transactional outbox -> Debezium -> Apache Kafka
                                             |
                              +--------------+----------------+
                              |              |                |
                        Leaderboard     Anti-cheat       Session/replay
                        Kafka Streams   Kafka Streams    Kafka Streams
                              |              |                |
                           Valkey        Alert topic     PostgreSQL/MinIO

All components -> OpenTelemetry Collector
                   +-> Prometheus/Grafana/Alertmanager
                   +-> Loki
                   +-> Tempo
```

### Kafka and event-streaming boundary

Kafka is not part of the frame-by-frame gameplay hot path. Player input and snapshots continue over WebSocket/UDP directly to the authoritative game server. Once the game server decides what happened, it emits durable asynchronous events without blocking the tick loop.

Event classes have different durability policies:

| Class | Examples | Policy when Kafka is unavailable |
|---|---|---|
| Critical | Purchases, rewards, inventory mutations | Commit to PostgreSQL with an outbox; Debezium publishes later |
| Durable gameplay | Match started/completed, score, elimination | Bounded disk spool or durable retry with deterministic event IDs |
| Best-effort telemetry | Position samples, tick diagnostics | Batch/sample and drop under pressure rather than freeze a match |

Initial versioned topics and keys:

| Topic | Key | Reason |
|---|---|---|
| `game.match-events.v1` | `match_id` | Preserve per-match event order |
| `game.player-events.v1` | `player_id` | Sessions, progression, and player analytics |
| `game.economy-events.v1` | `player_id` | Preserve per-player credit/inventory order |
| `game.matchmaking-events.v1` | `region:skill_bucket` | Queue and allocation analytics |
| `game.server-telemetry.v1` | `server_id` | Per-server health windows |
| `game.profile-table.v1` | `player_id` | Compacted profile `KTable` |
| `game.rules-table.v1` | `rule_id` | Compacted anti-cheat/rules `GlobalKTable` |
| `game.invalid-events.v1` | original key | Validation and poison-event DLQ |

Planned Kafka Streams applications:

1. **Live leaderboard:** re-key match events by player, aggregate daily/seasonal scores, materialize state, and publish updates.
2. **Player sessionizer:** use session windows, event time, grace periods, and late-event handling.
3. **Anti-cheat analytics:** sliding windows plus joins against profile/rule tables to flag impossible activity.
4. **Matchmaking analytics:** calculate wait time, queue depth, cancellation rate, and allocation quality by region/skill bucket.
5. **Progression/economy projection:** process rewards and purchases idempotently; demonstrate Kafka-to-Kafka exactly-once boundaries.
6. **Replay processor:** validate match sequence continuity, reconstruct summaries, and archive completed replays to MinIO.
7. **Server-health processor:** aggregate tick latency, disconnects, dropped snapshots, and capacity signals.

Kafka guarantees ordering only within a partition. A match-keyed event must be repartitioned by `player_id` for player analytics. `exactly_once_v2` covers Kafka input, state stores, output records, and offsets; it does not make PostgreSQL writes, HTTP calls, or notifications exactly once.

### Runtime invariants

- The server owns position, collisions, score, inventory, and match outcome.
- Each match initially owns its state in one actor/event loop; messages enter through bounded channels.
- Inputs contain player ID/session, sequence number, action, and client timestamp.
- Snapshots contain server tick, acknowledged input, and relevant world state.
- A slow client receives the newest useful snapshot; snapshots never accumulate without a bound.
- Wall-clock time is not trusted for simulation ordering; use server ticks and monotonic durations.
- PostgreSQL is authoritative for durable account/inventory data.
- Valkey is disposable acceleration/ephemeral state, not the only copy of important data.
- The game tick never waits synchronously for Kafka acknowledgements; a bounded background producer batches events.
- Every event has `event_id`, `event_time`, `produced_at`, `schema_version`, `correlation_id`, and `causation_id`.
- Event consumers are idempotent because at-least-once delivery can duplicate events.
- Critical database changes and their outbox rows commit in one PostgreSQL transaction.
- Topic keys, partition counts, retention, compaction, and schema compatibility are explicit design decisions.

## 5. Repository layout

```text
localarena/
├── cmd/                    # API, matchmaker, game server, workers, bots
├── internal/
│   ├── engine/             # Deterministic simulation
│   ├── protocol/           # WebSocket/protobuf messages
│   ├── identity/
│   ├── matchmaking/
│   ├── inventory/
│   └── telemetry/
├── streams/                # Java/Kotlin Kafka Streams applications
├── schemas/                # Protobuf/Avro event contracts
├── connect/                # Debezium and Kafka Connect configuration
├── web/                    # Minimal Canvas client
├── migrations/
├── infra/
│   ├── compose/
│   ├── kubernetes/
│   ├── helm/
│   ├── opentofu/
│   └── observability/
├── load-tests/
├── chaos/
├── dashboards/
├── alerts/
├── experiments/
├── adr/                    # Architecture decision records
├── runbooks/
└── postmortems/
```

## 6. Working method

Use six implementation days and one rest/review day per week. A normal day:

1. **Theory (60–90 min):** concepts and primary documentation.
2. **Design (30 min):** invariant, tradeoff, and expected failure.
3. **Build (3–4 hours):** smallest implementation proving the concept.
4. **Experiment (60–90 min):** baseline, inject one fault, inspect evidence.
5. **Journal (30 min):** result, surprise, mitigation, and remaining question.

For every important topic, produce: **explanation -> implementation -> failure experiment -> evidence -> tradeoff**.

## 7. Eight-week execution plan

### Week 1 — Authoritative simulation and networking

**Learn:** Docker fundamentals; WebSockets, long polling, SSE; memory leaks, GC, thread safety, backpressure; latency, throughput, p99/tail latency, network partitions, clock skew, DNS, TCP/UDP, HTTP/2/3, and gRPC.

**Build by day:**

- **Day 1:** Create repository, Go modules, Compose skeleton, architecture diagram, ADR-001 (authoritative server), and message schema.
- **Day 2:** Implement deterministic fixed-tick engine with movement, bounds, pickups, scoring, seeded randomness, and unit tests.
- **Day 3:** Add WebSocket sessions, input sequence numbers, snapshots, disconnect cleanup, and one Canvas client.
- **Day 4:** Build Go bot and bot-swarm controller; add bounded input/output queues and slow-client handling.
- **Day 5:** Add Prometheus counters/histograms and Go `pprof`; compare WebSocket with SSE/long polling for status and gRPC for an internal call.
- **Day 6:** Benchmark 10/50/100/200+ bots; inject latency/loss; run race detection and leak tests; write Experiment 001.

**Required experiments:** slow client, reconnect storm, 100–500 ms latency, packet loss, data race, goroutine leak, GC pressure, and tick overload.

**Gate:** two browsers and at least 100 bots can play; tick p99, memory/player, bytes/s/player, and cleanup behavior are documented.

### Week 2 — Persistence, transactions, and concurrency correctness

**Learn:** indexing, query optimization, N+1, connection pooling, replicas, sharding, partitioning, replication, leader election, CAP, eventual consistency, optimistic/pessimistic locking, distributed locks, races, deadlocks, migrations, and schema versioning.

**Build by day:**

- **Day 1:** PostgreSQL schema for accounts, profiles, inventory, purchases, matches, and outbox; migration tool and rollback policy.
- **Day 2:** Profile/history APIs; indexes; `EXPLAIN ANALYZE`; deliberately create and remove an N+1 query.
- **Day 3:** PgBouncer and pool metrics; test exhaustion and timeout behavior.
- **Day 4:** Lobby creation/joining and unique membership invariants using transactions and optimistic locking.
- **Day 5:** Implement fictional-credit cosmetic purchase; compare row locks, version columns, and constraints; create then resolve a deadlock.
- **Day 6:** Add one read-replica experiment, table partitioning for match history, and a two-shard routing prototype.

**Required experiments:** concurrent purchase of one limited cosmetic, duplicate join, deadlock, missing index, stale replica read, pool exhaustion, old-code/new-schema compatibility.

**Gate:** no overspending or duplicate active-lobby membership under concurrent tests; query plans and migration strategy are documented.

### Week 3 — Traffic, caching, and resilience

**Learn:** rate limiting, caching, balancing, reverse proxies, gateways, service discovery, circuit breakers, timeouts, retries, exponential backoff, idempotency, horizontal/vertical scaling, CDN/edge caching, cache invalidation, health checks, and readiness/liveness concepts.

**Build by day:**

- **Day 1:** Run three API replicas behind Envoy; compare round-robin and least-request behavior.
- **Day 2:** Add Valkey for profile cache, presence, lobby lookup, and token-bucket rate limits.
- **Day 3:** Define timeout budgets; add bounded retries with jitter and idempotency keys.
- **Day 4:** Add circuit breaker and graceful degradation when Valkey or a dependency fails.
- **Day 5:** Put map/skin assets in MinIO behind two NGINX caching proxies; version assets and invalidate safely.
- **Day 6:** Load test, kill/pause replicas, throttle CPU/memory, trigger a cache stampede and retry storm.

**Gate:** one stateless failure causes no API outage; cache failure does not lose durable data; retries cannot duplicate writes.

### Week 4 — Kafka, Kafka Streams, and distributed workflows

**Learn:** Kafka KRaft, brokers/controllers, topics, keys, partitions, leaders/replicas, ISR, producer acknowledgements/idempotence/batching/compression, consumer groups/offsets/rebalancing, retention/compaction, event contracts, pub/sub, backpressure, replay, Kafka Streams `KStream`/`KTable`, state stores, changelogs, repartitioning, delivery semantics, sagas, and DLQs.

**Build by day:**

- **Day 1:** Run Apache Kafka in KRaft mode with Kafka UI; create versioned topics and inspect partition placement, offsets, retention, compaction, and consumer groups.
- **Day 2:** Add Protobuf/Avro event envelopes and Apicurio compatibility checks; publish asynchronous match events from Go using a bounded, batched producer that never blocks the tick loop.
- **Day 3:** Connect the PostgreSQL transactional outbox through Debezium/Kafka Connect; prove that a service crash between database commit and publication loses no critical event.
- **Day 4:** Build Java/Kotlin Kafka Streams leaderboard and match-history processors using re-keying, aggregation, RocksDB state stores, changelog topics, materialized views, and idempotent outputs.
- **Day 5:** Build player-session windows and cosmetic purchase/reward saga; handle event time, out-of-order records, grace periods, compensation, retry topics, and DLQ.
- **Day 6:** Run multi-instance processors; duplicate/reorder/delay events, kill a broker/processor, trigger rebalance and state restoration, create a hot partition, and partition Kafka/etcd.

**Gate:** gameplay remains responsive while Kafka is unavailable; duplicate delivery cannot duplicate credits/rewards; leaderboard state restores from changelogs and can be rebuilt by replay; schema compatibility and saga compensation are proven.

### Week 5 — Security, identity, protocols, and API evolution

**Learn:** feature flags, secrets, IAM, OAuth, JWT rotation, TLS, encryption at rest/in transit, WAF, local overload/DDoS defense, CORS, CSRF, SQL injection, XSS, SSRF, webhooks, API versioning, and semantic versioning.

**Build by day:**

- **Day 1:** Keycloak OIDC login, short-lived JWTs, refresh flow, signing-key rotation, roles, and bans.
- **Day 2:** OPA authorization; SOPS/age secrets; TLS at the edge and selected internal paths.
- **Day 3:** Signed/replay-protected match-result webhook, API v1/v2 policy, and backward/forward-compatible event-schema evolution in Apicurio.
- **Day 4:** Secure chat/profile/avatar-import paths against XSS, SQL injection, CSRF, SSRF, and oversized payloads.
- **Day 5:** WAF rules, connection/login/input rate limits, feature-flagged game mode, and encrypted local storage experiment.
- **Day 6:** Run ZAP and source/image scans against this local lab; perform a basic threat-model review.

**Gate:** clients cannot set authoritative state; replayed requests do not duplicate actions; critical findings are fixed or documented.

### Week 6 — Observability, SRE, recovery, and chaos

**Learn:** monitoring, logging, tracing, metrics, alerts, SLOs, SLIs, error budgets, observability, backups, failover, disaster recovery, chaos engineering, incidents, on-call, and postmortems.

**Build by day:**

- **Day 1:** OpenTelemetry traces and correlation/causation IDs across gateway, matchmaking, database, Debezium, Kafka, Streams processors, and sinks.
- **Day 2:** Prometheus/Grafana dashboards for RED metrics plus tick duration, players, matches, dropped snapshots, broker health, partition distribution, consumer lag, rebalance duration, stream-thread state, RocksDB size, and restoration progress.
- **Day 3:** Loki logs, Tempo traces, Alertmanager alerts, and runbook links.
- **Day 4:** Define SLIs/SLOs/error budget; create availability, matchmaking, tick-budget, and event-lag alerts.
- **Day 5:** PostgreSQL backup/restore and failover drill; verify recovery-point and recovery-time observations.
- **Day 6:** AI-assisted hidden incident; diagnose only through symptoms/evidence, recover, and write a blameless postmortem.

**Initial SLO targets:** 99% login under 200 ms; 99% matchmaking under 5 s at baseline; 99.9% ticks under 50 ms; 99% match events processed within 10 s.

**Gate:** a fresh database is restored from backup; one incident has a timeline, impact, root cause, contributing factors, and corrective actions.

### Week 7 — Kubernetes, CI/CD, and infrastructure automation

**Learn:** CI/CD, Kubernetes, autoscaling, feature/deployment strategies, probes, rollback, IaC, Terraform concepts through OpenTofu, Helm, build caching, and dependency management.

**Build by day:**

- **Day 1:** Deploy to kind/k3d using Deployments, Services, ConfigMaps, Secrets, Ingress, requests, and limits.
- **Day 2:** Startup/readiness/liveness probes, disruption budgets, graceful shutdown, and game-server match draining.
- **Day 3:** HPA driven by API load; document why player/match-aware game-server scaling needs custom allocation logic.
- **Day 4:** Helm chart and OpenTofu-managed local resources; pin dependencies and container bases.
- **Day 5:** CI tests, race checks, Testcontainers Kafka Streams topology tests, schema compatibility checks, scans, cached container builds, and Argo CD deployment.
- **Day 6:** Rolling API update, stateful Kafka Streams rolling upgrade, canary protocol/schema change, blue-green release, failed migration, rollback, and dependency-upgrade drill.

**Gate:** environment is reproducible; API deploys without visible outage; game servers drain before termination; Streams applications rebalance and restore state without duplicate materialized results.

### Week 8 — Multi-region simulation and production game day

**Learn:** multi-region design, regional failover, cost optimization, cold starts, serverless limits, tail latency, partitions, clock skew, DNS behavior, and production operations.

**Build by day:**

- **Day 1:** Two regions using separate clusters or Compose networks; CoreDNS/global proxy routes players.
- **Day 2:** Add 20–40 ms player/region and 100–200 ms inter-region delay; implement region selection and regional matchmaking.
- **Day 3:** Decide ownership of profiles, matches, topics, and leaderboards; test stale reads, cross-region event delay, duplicate replication, and region-local consumer groups.
- **Day 4:** Regional failover, DNS TTL experiment, Kafka replication/mirroring design exercise, backup restore, replay, and partition behavior.
- **Day 5:** Optional Knative replay/thumbnail task for scale-to-zero/cold-start measurement; calculate CPU, memory, Kafka storage/retention, network, and state-store cost per player/match.
- **Day 6:** Final game day: sustained bots and Kafka producers plus several hidden failures; diagnose, mitigate, recover, rebuild a projection, roll back, and write final postmortem.

**Final failures should include:** game-server termination, overloaded matchmaking, regional partition, database latency, full Kafka broker storage, offline/under-replicated partitions, Streams rebalance or restoration delay, schema incompatibility, clock skew, bad JWT rotation, incompatible game protocol, corrupt cache, and failed migration.

**Gate:** final architecture review, benchmark report, regional-failure evidence, runbooks, and postmortem are complete.

## 8. Topic coverage index (1–111)

The week assignments below collectively cover every requested topic. Repeated topics are deliberately revisited at greater depth.

| Week | Topic IDs | Focus |
|---|---|---|
| 1 | 7, 22–24, 41–44, 91–100 | Containers, realtime protocols, runtime behavior, networking, performance |
| 2 | 25–40, 81–82 | Databases, concurrency, distributed-data theory, migrations |
| 3 | 1–5, 9–14, 45–50, 56–57 | Traffic, resilience, scaling, caches, health |
| 4 | 15–21, 29–35, 38, 44, 95 | Kafka, Streams, CDC, schemas, sagas, replay, coordination, partitions |
| 5 | 51, 67–80, 100–103 | Flags, identity, security, RPC, webhooks, API/event versions |
| 6 | 58–66, 83–87, 109–111 | Application plus Kafka observability, recovery, chaos, incidents |
| 7 | 6–8, 45, 52–57, 67, 104–108 | Delivery, Kubernetes, stateful processor rollout, IaC, builds |
| 8 | 48–49, 75, 83–90, 93–99, 109–111 | Edge/regions, stream replication/replay, DR, economics, operations |

## 9. Core experiments backlog

Complete at least 35; aim for 50:

1. Slow WebSocket consumer
2. Reconnect storm
3. Packet loss and latency
4. Tick CPU saturation
5. Goroutine leak
6. Data race
7. GC pressure
8. Concurrent limited-item purchase
9. Database deadlock
10. Missing-index p99 regression
11. N+1 query
12. Pool exhaustion
13. Replica stale read
14. Duplicate matchmaking
15. API replica kill
16. Hung API replica
17. Cache outage
18. Cache stampede
19. Retry storm
20. Circuit-breaker open/half-open behavior
21. Kafka unavailable while matches continue
22. Duplicate Kafka event delivery
23. Out-of-order and late events through window grace periods
24. Kafka Streams instance dies and tasks rebalance
25. Local RocksDB state deleted and restored from changelogs
26. Broker failure, partition-leader election, and ISR changes
27. Hot partition caused by a poor event key
28. Poison record and schema-validation DLQ
29. Breaking schema rejected by compatibility checks
30. Debezium outage and CDC backlog recovery
31. Crash after PostgreSQL/outbox commit but before publication
32. Kafka-to-Kafka exactly-once versus external side effects
33. Replay a season while live traffic continues
34. Saga compensation
35. Split-brain leader experiment
36. JWT rotation
37. Webhook replay
38. Chat XSS attempt
39. SSRF attempt
40. Backup restore
41. Database failover
42. Kafka broker disk full and retention response
43. Consumer lag/backpressure while a sink is paused
44. Bad readiness probe
45. OOMKilled pod
46. Stateful Streams canary regression and rollback
47. Region partition and duplicate cross-region event
48. Clock skew
49. Client/server protocol incompatibility
50. Event-schema incompatibility during mixed-version deployment

### Experiment record template

```markdown
# Experiment NNN — Title

## Hypothesis
## System version and configuration
## Baseline traffic and measurements
## Fault injected
## Expected behavior
## Actual behavior
## Metrics, logs, and traces
## Root explanation
## Mitigation
## Retest result
## Remaining risks
```

## 10. AI-assisted learning protocol

Use AI as a tutor and reviewer, not as a replacement for prediction and debugging.

### Before implementation

```text
Teach me [topic] in the context of LocalArena. Identify the invariant,
tradeoffs, and likely failure modes. Ask me to predict an experiment's
outcome before showing an implementation.
```

### Experiment design

```text
Design a reproducible localhost experiment for [concept]. Specify baseline
load, one injected fault, expected metrics, acceptance criteria, and how to
avoid confusing same-host resource contention with distributed behavior.
```

### Review

```text
Review this change for authoritative-state violations, races, unbounded
queues, timeouts, retries, idempotency, security, and observability. Rank
findings by impact and explain the failure scenario for each.
```

### Incident simulation

```text
Act as incident commander. Choose a hidden failure supported by my local
lab. Reveal only alerts and user symptoms. Let me request logs, metrics,
and traces, then grade my diagnosis, mitigation, and communication.
```

Rules:

- Predict before running.
- Ask AI for tests and counterexamples, not just generated features.
- Personally explain every accepted change.
- Keep architecture decisions in `adr/`.
- Never run attack/load tools against systems you do not own.

## 11. Definition of done

A weekly milestone is complete only when:

- The feature has automated correctness tests.
- The relevant load/failure test is repeatable.
- Metrics, logs, or traces prove the observed behavior.
- At least one tradeoff is written in an ADR or experiment report.
- Known limitations are documented.
- Commands needed to reproduce the result are recorded.
- Broken experiments are fixed or explicitly carried into the next week.

## 12. Scope controls

Do not spend time on polished art, complex physics, mobile clients, voice chat, real payment providers, public cloud, or production-grade anti-cheat. UDP prediction/reconciliation, Knative, active-active database writes, and live match migration are optional stretch goals. Prefer a reliable, observable 20-player game over an unfinished massive architecture.

## 13. Final portfolio artifacts

- Working LocalArena repository and one-command local startup
- Architecture diagram and at least eight ADRs
- 35–50 experiment reports
- Performance report with p50/p95/p99 and saturation point
- Threat model and security scan results
- Dashboards and alert rules
- Backup/restore and regional-failover evidence
- CI/CD, Helm, and OpenTofu configuration
- At least three runbooks and two postmortems
- Final document explaining what failed, why, and which design changes improved it
