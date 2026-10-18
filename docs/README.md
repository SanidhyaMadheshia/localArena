# LocalArena documentation

A study guide to every design decision in LocalArena. Each ADR deep dive explains:

- **Context**: the problem.
- **HLD**: high-level design, with a Mermaid architecture diagram.
- **LLD**: low-level design, with components, data structures, code paths and configuration.
- **Sequence diagrams** (Mermaid) for the key flows, including failure flows.
- **Concepts**: what each one is, why it was used here, and what was measured.
- **Where else it is used**: real-world scenarios outside games.
- **Trade-offs and alternatives.**
- **Interview questions with answers.**

The source decisions are in [`../adr/`](../adr), the evidence in [`../experiments/`](../experiments), incidents in [`../postmortems/`](../postmortems), and operations in [`../runbooks/`](../runbooks).

## Start here

1. [Architecture overview (HLD)](00-architecture.md): the whole system, data flows, and request lifecycles.
2. [Concept glossary](concepts/glossary.md): every concept, A-Z, each linked to the doc that uses it.
3. [Final report](../FINAL_REPORT.md): what failed, why, and what changed.

## ADR deep dives

| # | Topic | Week |
|---|---|---|
| [0001](adr/ADR-0001-authoritative-server.md) | Authoritative fixed-tick server, deterministic engine | 1 |
| [0002](adr/ADR-0002-protocol-websocket.md) | Versioned JSON protocol over WebSocket | 1 |
| [0003](adr/ADR-0003-match-actor-backpressure.md) | Match actor, bounded queues, latest-wins snapshots, backpressure | 1 |
| [0004](adr/ADR-0004-postgres-pooling-replicas.md) | PostgreSQL, PgBouncer, replicas, read-your-writes | 2 |
| [0005](adr/ADR-0005-migrations.md) | Schema migrations: expand/contract | 2 |
| [0006](adr/ADR-0006-concurrency-sharding.md) | Concurrency control, idempotency, deadlocks, sharding | 2 |
| [0007](adr/ADR-0007-edge-timeouts-retries.md) | Envoy edge, load balancing, timeout budgets, retries | 3 |
| [0008](adr/ADR-0008-caching-ratelimit-breaker.md) | Caching, rate limiting, circuit breaker, CDN-style edge cache | 3 |
| [0009](adr/ADR-0009-kafka-contracts-producer.md) | Kafka backbone, topic design, protobuf contracts, non-blocking producer | 4 |
| [0010](adr/ADR-0010-outbox-cdc.md) | Transactional outbox and CDC with Debezium | 4 |
| [0011](adr/ADR-0011-kafka-streams.md) | Kafka Streams: stateful processing, exactly-once | 4 |
| [0012](adr/ADR-0012-saga.md) | Saga, retry topics, DLQ | 4 |
| [0013](adr/ADR-0013-identity-secrets.md) | OIDC/JWT identity, game tickets, secrets | 5 |
| [0014](adr/ADR-0014-authorization-opa.md) | Policy-based authorization with OPA | 5 |
| [0015](adr/ADR-0015-api-evolution-edge-security.md) | API versioning, webhooks, input and edge security, feature flags | 5 |
| [0016](adr/ADR-0016-observability.md) | Observability: traces, metrics, logs | 6 |
| [0017](adr/ADR-0017-slos-recovery.md) | SLOs, burn-rate alerting, backup and failover | 6 |
| [0018](adr/ADR-0018-gameserver-lifecycle.md) | Game server lifecycle: drain, allocation | 7 |
| [0019](adr/ADR-0019-delivery-strategies.md) | Helm, rolling, canary and blue-green deploys, migration hooks | 7 |
| [0020](adr/ADR-0020-iac-ci-gitops.md) | OpenTofu, CI, GitOps with Argo CD | 7 |
| [0021](adr/ADR-0021-multi-region.md) | Multi-region: data ownership, failover, DNS | 8 |
| [0022](adr/ADR-0022-operations.md) | Operational readiness: game days, alert coverage | 8 |

## How to use this for interviews

- **System design rounds:** draw `00-architecture.md` from memory, then go deep on one ADR's HLD/LLD.
- **Behavioral and debugging rounds:** use the postmortems and the "what failed" table in the final report. Every number in these docs was measured.
- **Rapid review:** read only the "Interview questions" section at the end of each ADR file.
