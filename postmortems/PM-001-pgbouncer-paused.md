# PM-001: API outage on profile and purchase routes (PgBouncer pool paused)

- Date: 2026-10-18 (game-day drill, hidden fault)
- Severity: SEV-2: about half of all API requests failed for 3 min 46 s
- Status: resolved; action items below
- Blameless: this describes systems and signals, not people.

## Summary

At 18:26:31 UTC the PgBouncer pool for `localarena` was paused (the injected
fault; in production the same signature comes from an admin `PAUSE` during
maintenance that is never followed by `RESUME`). Every API path that needs a new
database transaction waited in PgBouncer, then failed at the 3 s request deadline
with 503. Cached reads, the catalog, presence and match history (replica pool)
kept working. The on-call responder was paged after 1 m 40 s, found the cause
from telemetry in 1 m 6 s and resumed the pool. Errors stopped immediately.

## Impact

| | |
|---|---|
| Duration | 18:26:31 → 18:30:17 UTC (3 m 46 s) of errors |
| Affected | `GET /v1/players/{id}` on cache miss (84 req/s failing), `POST /v1/purchases` (6.4 req/s failing) |
| Unaffected | cached profile reads, `/v1/cosmetics`, `/v1/players/{id}/matches` (replica pool), presence (Valkey), gameplay |
| Error ratio | peak 5 m SLI 51% 5xx; p99 latency 4.9 s on failing routes |
| Error budget | the 30 d availability budget (0.5%) took a large hit in one drill: on the lab's short history this shows as > 100% of the remaining budget |
| Data | none lost: failed requests never committed; purchases are idempotent so client retries are safe |

## Timeline (UTC)

| Time | Event |
|---|---|
| 18:26:31 | Fault injected; load 100 req/s (k6 mixed profile) |
| 18:28:06 | `LocalArenaAvailabilityBurnSlow` (ticket) |
| 18:28:11 | **`LocalArenaAvailabilityBurnFast` (page)**: "7.7% 5xx over 1 h" |
| 18:28:31 | `LocalArenaLatencyBurnFast` (page) |
| 18:28:53 | `LocalArenaDBPoolSaturated` (ticket): all 3 replicas 10/10 primary-pool connections busy |
| 18:29:11 | Responder starts: 5xx only on two routes that need the primary; replica-pool routes healthy |
| 18:29:40 | Loki: 200 `context canceled` errors, no SQL errors. Tempo: slow trace is a 2.5 s HTTP span **with no SQL child span**, so time was spent before reaching Postgres |
| 18:30:00 | Postgres: 1 active session, 0 lock waits (not the database). PgBouncer `SHOW POOLS`: `cl_waiting=30, sv_active=0`; `SHOW DATABASES`: `paused=1` |
| 18:30:17 | **Mitigation:** `RESUME localarena`. Instant 5xx rate → 0 |
| 18:30:53 | `DBPoolSaturated` resolved |
| 18:35:11 | `AvailabilityBurnFast` resolved (5 m window drained) |
| 18:35:12 | Answer unsealed: `pgbouncer-pause`. Diagnosis correct |

Time to detect 1 m 40 s · time to diagnose 1 m 6 s after starting · time to mitigate 3 m 46 s.

## Root cause and contributing factors

- **Trigger:** a paused PgBouncer database pool.
- **Why it hurt:** the API's request deadline (3 s) is shorter than PgBouncer's `query_wait_timeout` (5 s), so requests failed as generic deadline errors at the API rather than with a PgBouncer error naming the cause. Correct for the timeout budget, but the error carried no clue.
- **Why diagnosis needed manual steps:**
  - No PgBouncer metrics existed. The decisive signal (clients waiting, server connections idle) came from an interactive `SHOW POOLS`.
  - The first look at the cause was "API pool saturated", which points at Postgres or slow queries. Only the *absence* of SQL spans in the trace pointed to the pooler.
- **Alert noise during the incident:** `LocalArenaTickBudgetBurn` paged although no match was running. With zero ticks, the SLI evaluated to `1 - 0/ε = 100%`. A false page during a real incident costs attention.
- `LocalArenaTargetDown` for the stopped `rewards` worker was also firing (known, streaming profile off). Stale tickets hide new ones.

## What went well

- Multi-window burn-rate alerts paged within ~100 s and resolved on their own after the fix.
- Graceful degradation worked as designed: replica-pool and cache-served routes stayed up, and idempotent writes made retries safe.
- Trace + log correlation (`trace_id` in logs, SQL spans in Tempo) let the responder rule out the database in under a minute.

## Action items

| # | Action | Type | Status |
|---|---|---|---|
| 1 | Export PgBouncer metrics (`pgbouncer-exporter`) and page on `clients waiting > 5 and server active == 0` for 30 s (`LocalArenaPgBouncerClientsWaiting`) | detect | **done**, promtool test |
| 2 | Guard the tick SLO alert with `tick count rate > 0` | alert quality | **done**, regression test |
| 3 | Runbook `api-availability.md`: add "SQL spans missing from slow traces → look at the pooler (`SHOW POOLS`, `paused`)" | diagnose | **done** |
| 4 | Silence or remove targets for intentionally stopped services (profile-aware scrape config) | alert quality | open (week 7: k8s service discovery) |
| 5 | Maintenance procedure: any `PAUSE` must be paired with a timed `RESUME` (or use `SUSPEND`/`RECONNECT`), announced in the channel | prevent | open |
