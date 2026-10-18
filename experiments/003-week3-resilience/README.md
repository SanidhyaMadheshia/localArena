# Experiment 003: Week 3 traffic, caching, and resilience

## Hypothesis

With Envoy health checks, outlier detection, and host-local retries, losing
or hanging one of 3 API replicas causes no failed requests. A Valkey outage
degrades only Valkey-only features (presence) and never loses durable data.
Singleflight collapses a cache stampede. Client backoff with a retry budget
bounds retry amplification during a database brownout. Content-hashed assets
behind caching edges need no purges and survive an origin outage.

## System version and configuration

- Week 3 working tree, 2026-10-18. Envoy v1.40.0, Valkey 9.0.6, Garage
  v2.4.1, NGINX 1.31.7, k6 2.3.0.
- 3 API replicas (1 CPU, 256 MB each) behind Envoy (`:10000` least request,
  `:10001` round robin), with PgBouncer and Postgres 18.6 primary plus
  replica.
- Seed: 1,000 players, 20,000 matches (`seed.sql`). Load mix
  (`load-tests/api-mixed.js`): 70% profile, 10% history, 8% catalog,
  7% presence, 5% idempotent purchase.
- Everything runs on one host, including k6, so latency numbers are
  relative.
- Reproduce: `docker compose up -d && experiments/003-week3-resilience/run.sh`
  (about 12 min), then `experiments/003-week3-resilience/edge.sh`. Raw k6
  summaries and Envoy counter deltas are in `results/`.

## 1. Replica failures (300 rps for 30 s; fault at t = 5 s)

| Scenario | Requests | Failed | p50 ms | p95 ms | p99 ms | Envoy evidence |
|---|---:|---:|---:|---:|---:|---|
| baseline, round robin | 9,001 | 0 | 2.62 | 6.10 | 8.31 | |
| baseline, least request | 9,001 | 0 | 2.57 | 6.00 | 8.12 | |
| `docker kill` replica (RR) | 9,000 | **0** | 2.56 | 6.01 | 8.40 | 5 connect fails, 5 retries, all succeeded; 1 outlier ejection |
| `docker stop` (graceful drain, RR) | 9,001 | **0** | 2.58 | 6.04 | 7.91 | 3 health-check failures (drain), 0 retries needed |
| `docker pause` 15 s, round robin | 8,844 | **0** | 2.61 | 6.70 | **2,518** | 207 per-try timeouts (2.5 s), all retried on another host |
| `docker pause` 15 s, least request | 8,976 | **0** | 2.57 | 6.23 | **12–625** (2 runs) | 76 per-try timeouts |
| CPU 0.1 on one replica, RR | 9,001 | 0 | 2.64 | 7.13 | 43.6 | |
| CPU 0.1 on one replica, LR | 9,000 | 0 | 2.59 | 6.44 | 26.6 | |

- Gate met: no stateless failure caused a failed request.
- A **hang is worse than a crash**. A dead host refuses connections, gets
  retried within milliseconds, and is ejected. A paused host accepts the
  connection and then says nothing, so every request sent to it waits the
  full per-try timeout. With round robin, one third of requests do that
  until health checks eject it (about 4 s). Least request notices the
  stuck requests piling up on the host and stops picking it. Least request
  is now the default on `:10000`.
- Retries had to be tuned twice:
  1. With `min_retry_concurrency: 3`, killing a replica caused 27 failed
     requests: the burst of connect failures overflowed the retry budget
     (`upstream_rq_retry_overflow 27`). Raising it to 10 and shortening
     `connect_timeout` from 0.5 s to 0.25 s brought this to 0.
  2. Presence `PUT`s used the non-idempotent route, which had no per-try
     timeout. They waited out the full 4 s route timeout on the paused
     replica (10 × 504). PUT moved to the retryable route.

## 2. Valkey outage (stopped for 10 s at 300 rps)

| Endpoint | Failed | p99 ms |
|---|---:|---:|
| profile | 0 | 9 |
| history | 0 | — |
| cosmetics | 0 | — |
| purchase (credits, inventory) | 0 | — |
| presence heartbeat | 52% (337 total) | 3.9 |

- Durable paths kept working (served from Postgres, with rate limits on the
  local fallback). Presence answers 503 + `Retry-After` because it exists
  only in Valkey (ADR-0008), and it fails fast (p99 3.9 ms) because the
  circuit breaker is open.
- First run: Envoy retried those presence 503s **637 times**, uselessly,
  since every replica shares the same Valkey. The API now tags
  dependency-caused 503s with `x-envoy-ratelimited`, which Envoy never
  retries. In the second run the proxy made 0 retries.
- Total failure rate was 3.7%, entirely presence. The k6 global threshold
  (`<1%`) is crossed by design. Endpoint-level thresholds are the right SLO
  unit here.

## 3. Cache stampede (one hot profile, 1 s TTL, 1,000 rps for 20 s, 30 ms fill cost)

| | Requests | Cache hits | Profile loads in Postgres | p99 ms |
|---|---:|---:|---:|---:|
| singleflight on | 20,001 | 19,352 | **62** | 28.5 |
| singleflight off | 20,000 | 19,350 | **650** | 34.8 |

- About 20 expiries × 3 replicas ≈ 60 loads is the floor for per-process
  singleflight. Without it, each expiry admits about 30 concurrent loads
  (10.5× more database work for the same traffic).
- The first attempts showed no stampede at all. A real 1 ms fill finishes
  before a second miss arrives, so the experiment needs fills that are
  expensive relative to the arrival interval (`PROFILE_LOAD_DELAY=30ms`).
  Stampedes are a function of fill cost × request rate, not of request rate
  alone.
- Pre-change unit test: 100 concurrent misses gave 100 loads without
  singleflight and 1 with it.

## 4. Retry storm during a database brownout

Fault: the history index is dropped for 20 s, so every history query scans
200k rows and the DB saturates. 150 logical requests/s; the client timeout
is 2 s.

| Client | Logical success | Goodput/s | Attempts per logical request | Offered attempts/s | p50 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| naive (≤ 5 immediate retries) | 54% | 60 | **4.04** | 446 | 6,784 | 12,011 |
| backoff + jitter + budget (≤ 3, 10% budget) | 46.5% | 67 | **1.31** | 188 | **42** | 6,214 |

- Naive retries quadruple the load on an already-saturated database, and
  each logical request is held for seconds. With backoff and a budget,
  offered load is 58% lower and median latency is 160× lower. Its goodput
  was slightly higher (67 vs 60/s), and it recovered faster after the index
  returned.
- Lower logical success for backoff is the intended tradeoff: when the
  budget is exhausted (2,099 retries denied), it fails fast instead of
  queueing. The naive client "succeeds" more often only by waiting up to
  12 s, which a real user would not do.
- Root cause found on the way: Envoy's per-try timeout (1.5 s) was shorter
  than the DB statement timeout (2 s), so during the brownout **Envoy
  itself** retried 7,482 times on other replicas against the same slow
  database. Reordering the budget (per-try 2.5 s > DB 2 s; ADR-0007)
  removed proxy retries in the brownout.

## 5. Edge asset cache (Garage + 2 NGINX edges; `results/edge.txt`)

| Check | Result |
|---|---|
| Cold then warm, edge-a and edge-b | MISS → HIT → HIT on each edge independently |
| `?v=1`, `?cachebust=…` | HIT (cache key ignores query) |
| Publish a new skin version | new hashed URL: MISS → HIT; old URL still HIT for old clients; manifest updated within its 5 s TTL; no purge |
| 200 concurrent requests, cold 2 MB object | 200 × 200 OK, **2 MISS / 199 HIT** (`proxy_cache_lock`) |
| Origin (Garage) stopped | cached object: 200 in 11 ms; uncached object: 504 after the 1 s connect timeout |
| Warm throughput, 2 MB object, 50 VUs | 120 req/s, about 240 MB/s, p50 416 ms (loopback, k6 on the same host) |

## 6. Node loss on kind (1 control plane + 2 workers; `node-loss.sh`, `results/node-loss.txt`)

`docker stop` of the worker node that hosts the CNPG primary, while a pod on
the other worker writes through Service `api` at 4/s.

| Run | Topology before the fault | Failed writes | Write outage | Promotion |
|---|---|---:|---:|---|
| 1 | CNPG operator (1 replica) on the victim node | 91 / 128 | 137 s, no recovery until the node returned | **never** |
| 2 | operator ×2, one per node | 37 / 337 | 66 s | +53 s |
| 3 | + `/readyz` no longer fails on DB loss; but pooler (1 pod) and Valkey on the victim | 73 / 110 | did not recover within the run | +60 s; writes still failing: the only pooler pod sat on the dead node |
| 4 | operator ×2, pooler ×2, API ×2, each spread one per node | 53 / 378 | **60 s** | +55 s |

- Final timeline: node stop at 0 s → node `NotReady` +46 s (kubelet
  heartbeat grace) → CNPG `FailingOver` +50 s → promoted about +55 s →
  writes succeed +61 s. Detection is the floor, not promotion: CNPG reacts
  within about 4 s of the pod becoming unready. Lowering the controller
  manager's `node-monitor-grace-period` would shorten this, at the cost of
  false positives on a slow node.
- **Single points of failure found by this drill**, each invisible in Compose:
  1. One operator pod. When it shares a node with the primary, nobody
     promotes. Fixed with 2 replicas and leader election
     (`infra/kubernetes/helm/cnpg-values.yaml`).
  2. One PgBouncer pod. Pods on a lost node stay "Running" until eviction
     (default 300 s), so the `-rw` Service keeps sending traffic there.
     Fixed with 2 pooler instances, one per node.
  3. `ScheduleAnyway` spread put both API pods on the same node, and a
     rollout counted the old ReplicaSet. Fixed with `DoNotSchedule`,
     `nodeTaintsPolicy: Honor` (the tainted control plane otherwise acts as
     an empty domain and deadlocks rollouts), and
     `matchLabelKeys: [pod-template-hash]`.
  4. `/readyz` checked the primary. During failover, every API replica went
     unready at once, removing all endpoints even for cache-served reads.
     Readiness now reflects only this replica (draining); DB state is
     reported as "degraded" (`TestReadyzIgnoresSharedDependencies`).
- Valkey is still a single pod. Losing its node costs caching and presence
  until it is rescheduled (about 5 min with default eviction), but no
  durable data (degradation per ADR-0008).

## Remaining risks / carried forward

- Per-process singleflight still allows N replicas × 1 load per key. A
  shared lease-and-wait would bring it to 1. Not needed at this scale.
- A missed invalidation (Valkey blip during a write) can serve a stale
  profile for up to 30 s. Purchases always re-check Postgres.
- Garage runs single-node (`replication_factor 1`): no durability for
  replays yet.
- Round-robin hang latency is bounded only by the per-try timeout.
  Hedging (`hedge_on_per_try_timeout`) is a possible follow-up for GETs.
- The k6 load generator, Envoy, and the services share CPUs. Absolute
  throughput is not a capacity claim.
- Node-loss write outage is about 60 s, dominated by node failure detection.
  RPO was not measured (async replication; week 6 DR drill).
