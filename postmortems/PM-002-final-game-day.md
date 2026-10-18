# PM-002: Final game day: three overlapping hidden faults, plus a latent rewards regression

- Date: 2026-10-18/19 (week 8 game day)
- Severity: SEV-2 for the API (≈ 1 min of heavy 5xx); SEV-3 for gameplay and leaderboard; latent SEV-3 for rewards since week 5
- Blameless format

## Setup

Sustained load: 100 bots (30 s rounds, reconnect churn every ~20 s) and 80 req/s of mixed API traffic, against the full stack (3 Kafka brokers, Streams ×2, rewards saga, Debezium, observability, OPA, flagd). Three faults out of six were chosen at random, injected at +60/+150/+240 s, and sealed until the end.

## Timeline (UTC)

| Time | Signal | Action |
|---|---|---|
| 20:37:41 | game day starts | |
| 20:38:42 | (fault 1 injected) | |
| 20:39:28 | ticket `LocalArenaTargetDown{job=gameserver}`; `localarena_players` absent | `docker inspect`: exit 137, not OOM, restart count 0 → **killed process** (`unless-stopped` does not restart after `docker kill`) |
| 20:39:58 | | **mitigate 1:** start the gameserver; bots reconnect (players back to 100) |
| 20:40:11 | (fault 2 injected) | |
| 20:41:11 | page `AvailabilityBurnFast`, then `PgBouncerClientsWaiting` (the PM-001 alert), `LatencyBurnFast`, postgres-exporter target down | the pool is **not** paused (`paused=0`); `SELECT 1` takes **7.6 s**; container `NanoCpus=5e7` → **primary starved of CPU (0.05 cores)** |
| 20:42:07 | | **mitigate 2:** restore 2 CPUs; `SELECT 1` 0.13 s; PgBouncer page resolved 20:42:23 |
| 20:41:43 | (fault 3 injected, while still on fault 2) | |
| 20:42:30 | **no alert**; 5xx at 0 but both Streams `/healthz` endpoints unreachable | containers exited 143 (SIGTERM); `localarena-streams` lag 1,509 and growing → **stream processors stopped** |
| 20:43:23 | | **mitigate 3:** start both; RUNNING 8 s later, lag 80 after 20 s, leaderboard serving |
| 20:44 | answer unsealed: `kill-gameserver`, `db-latency`, `stop-streams` | **3/3 diagnosed correctly** |
| 20:46-21:05 | post-game check: **0 rewards granted since week 4** | found a latent regression (below); fixed |

| Fault | Time to detect | Time to mitigate | Detected by |
|---|---|---|---|
| kill-gameserver | 46 s | 1 m 16 s | `TargetDown` (ticket) |
| db-latency | 60 s | 1 m 56 s | burn-rate page + PgBouncer page |
| stop-streams | ~47 s (manual) | 1 m 40 s | **nobody**: found by a human checking health endpoints |

## Impact

- **API:** the error-budget fast burn paged. Profile and purchase requests failed for about 1 minute while the primary was starved. The PM-001 alert pointed straight at the database tier.
- **Gameplay:**
  - Bots on the killed gameserver lost their match.
  - Over the whole run, 27,707 reconnect attempts failed, mostly during the 76 s the gameserver was down (no fallback region in this run).
  - Input RTT p99 stayed at 52 ms for connected players.
  - **10,298 durable events queued = 10,298 delivered, 0 lost.**
- **Leaderboard:** stale for about 100 s, then caught up from Kafka (lag 1,509 → 80 in 20 s). Totals stayed consistent.
- **Rewards:** **no account player had been rewarded since week 5** (latent).

## Root causes and contributing factors

1. The three injected faults (by design).
2. **Detection gap:** nothing alerted on stopped stream processors. Their containers have no Prometheus endpoint, and consumer lag was not exported.
3. **Latent regression, the most important finding:**
   - Week 5 made the gameserver ignore `?player=` whenever `GAME_TICKET_SECRET` is set (correct for security).
   - Compose loads that secret, so bots started with `-accounts` silently joined as **guests** (IDs ≥ 1e9).
   - Guests earn no rewards, so the planner correctly produced nothing.
   - No test or alert covered "matches complete but nobody is rewarded".
   - Experiment 004's numbers were collected before week 5, so they were unaffected.
4. **Mitigation side effect:** `docker compose up -d gameserver` during the incident recreated the container without `ROUND_SECONDS=30` (an environment variable from the start command), silently changing rounds to 120 s. Recovery commands must come from runbooks or the Makefile, not from memory.
5. `restart: unless-stopped` does not recover from `docker kill`. Kubernetes restarts the pod; Compose does not.

## What went well

- PM-001's action item (the PgBouncer alert) fired on the database fault and sped up diagnosis.
- Overlapping faults were separated by checking each signal against its own layer (target down → container state; 5xx → DB timing; health → processors).
- Durable events: zero loss under a gameserver kill (spool and idempotent producer).

## Action items

| # | Action | Status |
|---|---|---|
| 1 | Export consumer lag (`kafka-exporter`) and page on `LocalArenaConsumerLagGrowing` | **done**, promtool test |
| 2 | Alert `LocalArenaNoRewardsGranted` (events flowing, zero grants in 15 m) | **done** |
| 3 | Explicit lab flag `TRUST_PLAYER_PARAM` for account bots; the default is off and a test proves `?player=` is ignored without it | **done** (`TestPlayerParamOnlyWithLabFlag`); rewards flowing again (459 → 467) |
| 4 | Runbook commands use `make`/scripts that carry the full environment (e.g. `ROUND_SECONDS`) | open |
| 5 | Region-aware reconnect in the clients (bots re-run region selection on repeated failures) | open (Experiment 008 shows that re-selection removes the failures) |
| 6 | Add an end-to-end "a reward is granted after a match" check to CI | open |
