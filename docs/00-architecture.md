# Architecture overview (HLD)

LocalArena is a server-authoritative multiplayer game backend. Clients send only
*intent* (inputs). The servers own all state. Durable facts flow through
PostgreSQL and Kafka to asynchronous processors. Everything is observable and
deployable on Compose or Kubernetes.

## 1. System context

```mermaid
flowchart LR
  P[Players: browser / bots] -- WebSocket + game ticket --> GS[Game servers<br/>authoritative 20 Hz tick]
  P -- HTTPS + JWT --> EDGE[Envoy edge<br/>TLS, WAF, rate limit, LB]
  P -- OIDC login --> KC[Keycloak]
  P -- assets --> CDN[NGINX edge caches] --> OBJ[(Garage S3)]
  EDGE --> API[API replicas<br/>Go, stateless]
  API --> PGB[PgBouncer] --> PG[(PostgreSQL primary)]
  PG -. streaming replication .-> PGR[(Replica)]
  API --> PGR
  API --> VK[(Valkey<br/>cache, presence, rate limits)]
  API --> OPA[OPA policy]
  API -. JWKS .-> KC
  GS -- non-blocking events --> K[(Kafka 3 brokers, KRaft)]
  PG -- outbox via CDC --> DBZ[Debezium] --> K
  K --> KS[Kafka Streams<br/>leaderboard, sessions]
  K --> RW[Reward saga worker] --> PGB
  GS --> FL[flagd feature flags]
  subgraph Observability
    OT[OTel collector] --> TEMPO[(Tempo)]
    PROM[(Prometheus)] --> AM[Alertmanager] --> PAGER[pager]
    ALLOY[Alloy] --> LOKI[(Loki)]
    GRAF[Grafana]
  end
  API -. traces .-> OT
  GS -. traces .-> OT
```

## 2. Components and responsibilities

| Component | Responsibility | State | Scaling unit | ADR |
|---|---|---|---|---|
| Game server (Go) | Authoritative simulation, one goroutine per match, snapshots to clients, events out | live matches in memory | process, allocated by free slots | 0001-0003, 0018 |
| API (Go) | Accounts, profiles, purchases, lobbies, history, tickets, webhooks | none (stateless) | replicas behind Envoy, HPA | 0004-0008, 0013-0015 |
| Envoy | TLS, WAF route, global rate limit, health checks, retries, load balancing | none | replicas | 0007, 0015 |
| PostgreSQL + PgBouncer | Source of truth for money-like data; outbox | durable | vertical + read replicas (+ shards) | 0004-0006, 0010 |
| Valkey | Cache-aside, presence, distributed rate limits | disposable | single (lab) | 0008 |
| Kafka | Durable event log, partitioned by key | durable, retained 1-30 days | partitions/brokers | 0009 |
| Debezium | Publishes outbox rows from WAL | slot position | single task | 0010 |
| Kafka Streams (Java) | Leaderboard, sessions; exactly-once v2 | RocksDB + changelogs | instances ≤ partitions | 0011 |
| Reward saga (Go) | Reserve → grant → compensate across services | `reward_sagas` table | consumer group | 0012 |
| Keycloak / OPA / flagd | Identity, authorization, feature flags | IdP DB / policy / flags file | replicas | 0013-0015 |
| Observability stack | Traces, metrics, logs, SLO alerts | TSDB / blocks / chunks | single (lab) | 0016-0017 |

## 3. Key data flows

### 3.1 Gameplay (hot path, never blocks on I/O)

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant WS as Session (readPump/writePump)
  participant M as Match actor (1 goroutine)
  participant E as Event publisher
  C->>WS: input {seq, dx, dy, st}
  WS->>M: Submit (non-blocking, bounded queue)
  loop every 50 ms tick
    M->>M: apply inputs (seq dedupe), Step(), collisions, score
    M->>WS: offer snapshot (capacity-1 mailbox, newest wins)
    M->>E: Publish(PlayerScored) (non-blocking)
  end
  WS-->>C: snapshot {tick, ack, players, items}
  E-->>E: batch → Kafka, or disk spool if Kafka is down
```

### 3.2 Purchase (correctness path)

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant EN as Envoy
  participant A as API
  participant O as OPA
  participant DB as Postgres (via PgBouncer)
  participant D as Debezium
  participant K as Kafka
  C->>EN: POST /v2/purchases (JWT, Idempotency-Key)
  EN->>A: route (least request, per-try timeout)
  A->>A: verify JWT (JWKS), map sub → player, check ban
  A->>O: allow purchase? (fail closed)
  A->>DB: BEGIN, claim key, UPDATE stock WHERE stock>0, UPDATE credits WHERE credits>=price, INSERT inventory, INSERT outbox, COMMIT
  A-->>C: 201 (or 200 replayed)
  D->>DB: read WAL (replication slot)
  D->>K: CosmeticPurchased to game.economy-events.v1
```

### 3.3 Rewards (asynchronous, at-least-once, idempotent)

```mermaid
sequenceDiagram
  autonumber
  participant GS as Game server
  participant K as Kafka
  participant PL as Planner
  participant SG as Saga worker
  participant DB as Postgres
  GS->>K: MatchCompleted (deterministic event_id)
  K->>PL: consume
  PL->>K: RewardCommand per account player (reward_id = hash(event_id, player))
  K->>SG: consume
  SG->>DB: Reserve (pool -= credits), state=reserved
  SG->>DB: Grant (wallet += credits, outbox RewardGranted), state=granted
  alt transient failure
    SG->>K: retry topic (attempt+1, not-before)
  else permanent failure
    SG->>DB: Compensate (pool += credits), state=compensated
  end
```

## 4. Consistency model (who is the source of truth)

| Data | Truth | Copies | Staleness tolerated |
|---|---|---|---|
| Positions, scores in a live match | game server memory | snapshots | none (server decides) |
| Credits, inventory, purchases | Postgres primary | replica, cache, events | reads may be stale; writes always on the primary; RYW via LSN |
| Profiles | Postgres | Valkey (TTL 30 s + invalidation) | ≤ 30 s worst case |
| Leaderboard | derived from Kafka | Streams state stores | seconds; rebuildable by replay |
| Presence | Valkey | none | ephemeral by definition |

## 5. Failure behavior summary

| Failure | User impact | Measured in |
|---|---|---|
| API replica crash or hang | none (retries, health checks, least-request) | Exp 003 |
| Valkey down | slower reads, presence "unknown" | Exp 003 |
| Kafka down | none for gameplay; events spooled and replayed | Exp 004 |
| Postgres primary loss | writes down ~1 s (CNPG), RPO = replica lag | Exp 002, 006 |
| OPA down | v2 writes 503 (fail closed) | Exp 005 |
| Node loss (k8s) | ~60 s write outage, automatic | Exp 003 |
| Region partition | remote writes 503, reads and gameplay fine | Exp 008 |

## 6. Deployment views

- **Compose profiles:** core (gameserver, API ×3, Envoy, Postgres, replica, PgBouncer, Valkey), `streaming`, `security`, `observability`, `regions`, `shards`, `load`.
- **Kubernetes (kind, 2 workers):**
  - OpenTofu installs the operators (CNPG, metrics-server, Argo CD).
  - Argo CD syncs the Helm chart `infra/helm/localarena` from Git.
  - Migrations run as pre-upgrade hooks; the HPA scales the API.
