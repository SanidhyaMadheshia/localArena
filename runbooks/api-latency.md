# API latency (> 300 ms) and DB pool saturation

**Symptom:** `LocalArenaLatencyBurnFast`: >14% of requests slower than 300 ms; or `LocalArenaDBPoolSaturated`: pool > 90% busy.

## Check
1. Dashboard "p99 latency by route": which routes?
2. Tempo: search `{resource.service.name="api" && duration > 300ms}`. Look at the slowest child span:
   - SQL span long → query plan / locks / missing index (`EXPLAIN ANALYZE`, `pg_stat_activity` wait events).
   - Gap before the first SQL span → waiting for a pool connection (`localarena_db_pool_empty_acquires_total` rising).
   - Valkey/OPA span long → that dependency.
3. Postgres: `SELECT wait_event_type, wait_event, count(*) FROM pg_stat_activity GROUP BY 1,2;`, `pg_stat_statements` top by `mean_exec_time`.

## Mitigate
- Missing/dropped index: recreate `CONCURRENTLY` (experiment 003 showed history going from 1 ms to seqscan).
- Lock contention: find the blocker (`pg_blocking_pids`), end it if it is a runaway session.
- Pool exhaustion: shed load at the edge (lower the Envoy token bucket) rather than raising pool size past `max_connections`.

## Verify
`slo:api_slow:ratio_rate5m` < 0.01.
