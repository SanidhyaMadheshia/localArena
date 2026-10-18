# Experiment 008: Week 8 multi-region simulation, cost per player, final game day

## Hypothesis

Region-local gameplay keeps input latency bounded by the player's distance to
the nearest region. Single-writer economy data stays correct across regions at
the cost of slow remote writes. A region partition degrades writes, not reads or
gameplay. Client-side region selection fails over in seconds, while DNS failover
is bounded by TTL. A game day with several overlapping faults is diagnosable
from telemetry.

## System version and configuration

- 2026-10-19. Toxiproxy 2.13.0, CoreDNS 1.14.7 (authoritative + caching resolver), "regions" Compose profile:
  - Region A: existing gameserver, API (via Envoy), Postgres primary.
  - Region B: `gameserver-b`, `api-b`, the replica. Writes go to A's PgBouncer through Toxiproxy.
- Simulated WAN (`infra/compose/regions/latency.sh apply`): player → A about 20 ms RTT, player → B about 40 ms, B → A 150 ms.
- Reproduce:
  ```bash
  docker compose --profile regions up -d && infra/compose/regions/latency.sh apply
  experiments/008-week8-regions/regions.sh
  experiments/008-week8-regions/gameday.sh start   # then diagnose; ... stop
  ```

## Results: regions (`regions.sh`, `results/*.txt`)

| Scenario | Region A | Region B |
|---|---|---|
| API read (`GET /v1/cosmetics`) median | 29 ms | 47 ms |
| Client region selection | picked A (22 ms vs 49 ms) | |
| Gameplay input RTT p50 / p99 | 49.9 / 56 ms | 50.5 / 94 ms |
| **Write** (`POST /v1/players`) median | 35 ms | **805 ms** (every SQL round trip in the transaction crosses the 150 ms WAN to the single writer) |
| Profile read on cache miss | 28 ms | 198 ms (cache fill reads the primary over the WAN; B has no local Valkey) |

| Fault | Result |
|---|---|
| **Stale read:** match recorded through B, B's replica lagging (replay paused) | history read in B returned **0 matches** until replay resumed, then 1 (eventual consistency, measured). Profile reads stayed correct: cache fills read the primary, and `X-Read-After-LSN` routes around a lagging replica |
| **Partition B ↔ A** | B reads 200 (35 ms), **B writes 503 in 53 ms** (fail fast within budget), B gameplay unaffected (0 connect failures), A unaffected. Healed → 201 |
| **Region A down for players** | clients sticking to A: **1,549 failed reconnects**; after re-running region selection, all 20 moved to B with **0 failures** |
| **DNS failover A → B** (TTL 30 s) | authoritative answer changed 4-14 s after the edit (CoreDNS reload interval plus Docker Desktop bind-mount propagation); a caching resolver kept serving A until its cached TTL ran out: **~34 s** total |

Finding while running the partition test: the first B write during a
partition answered **500**. A pooled connection dying mid-query surfaces as
EOF or connection reset, not as a connection-class SQLSTATE. `IsUnavailable`
now also covers `io.EOF`/`ECONNRESET`/`net.Error`/`SafeToRetry`, which gives
503 + `Retry-After`.

## Results: cost (`cost.md`)

- **Per concurrent player:**
  - 0.0025 gameserver cores.
  - ~30 KiB memory.
  - **~15 KB/s of snapshot egress (~39 GB/month).**
- **Per 2-minute, 20-player match:**
  - 6 core-seconds.
  - 36 MB of egress.
  - ~455 KB of Kafka storage (RF 3, 7-day retention).
  - ~2 KB in Postgres.
- **Bandwidth dominates.** The obvious savings are delta snapshots, binary encoding and interest management.
- Below a few hundred CCU, the fixed HA platform (3 brokers, 2 DB instances, observability) costs more than the game itself.

## Results: final game day (`gameday.sh`, `postmortems/PM-002-final-game-day.md`)

Three hidden faults were drawn from six (`kill-gameserver`, `db-latency`, `stop-streams`) and overlapped under 100 bots and 80 req/s.

| Fault | Detected | Mitigated | Signal |
|---|---|---|---|
| gameserver killed | 46 s | 1 m 16 s | `TargetDown` → container exit 137 |
| Postgres primary starved of CPU | 60 s | 1 m 56 s | burn-rate page + `PgBouncerClientsWaiting` (PM-001 alert) → `SELECT 1` 7.6 s, `NanoCpus` |
| Streams stopped | **no alert** | 1 m 40 s | manual health check; lag 1,509 growing |

- All 3 were diagnosed correctly.
- Durable events: **10,298 queued = 10,298 delivered.**
- Leaderboard caught up from Kafka within 20 s of restart.

**Latent regression found:**

- Since week 5, account bots had silently been guests, because enabling tickets disables `?player=`. As a result, **no rewards had been granted since then**.
- **Fix:** an explicit `TRUST_PLAYER_PARAM` lab flag (default off, tested), plus `LocalArenaNoRewardsGranted` and `LocalArenaConsumerLagGrowing` alerts (promtool-tested) with a Kafka lag exporter.
- Rewards were flowing again after the fix (459 → 467).

## Remaining risks

- Both "regions" share one host and one Postgres replica, so stale-read results apply to both. Real regions need their own replica and Valkey.
- Region failover in clients is not automatic yet (bots re-select only at start; PM-002 action 5).
- Kafka cross-region replication is a design (ADR-0021), not deployed.
- The cost numbers are resource units; prices must come from a provider's calculator.
