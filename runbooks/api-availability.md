# API availability (5xx burn)

**Symptom:** `LocalArenaAvailabilityBurnFast` (page): >7.2% of API requests are 5xx over 1 h and 5 m. Slow variant (ticket): >3% over 6 h.

## Check (in order)
1. Dashboard "Errors (5xx) by route": one route or all?
   - One route: likely a dependency of that route (purchase → Postgres/OPA, presence → Valkey).
   - All routes: replica, Envoy, or a shared dependency.
2. `up{job="api"}`: are replicas down? `LocalArenaTargetDown` would also fire.
3. Logs: Grafana Explore → Loki `{service="api", level="ERROR"}`. Open a `trace_id` in Tempo to see which span failed.
4. Response bodies: `temporarily unavailable` means a dependency (DB/OPA/Valkey) and carries `x-envoy-ratelimited`; `internal error` means a bug (check the logged `err`).
5. Dependencies: `docker compose ps postgres pgbouncer opa valkey`.
6. **Slow traces with no SQL child span** mean time is lost before Postgres (PM-001): check the pooler.
   `psql -h pgbouncer -U localarena pgbouncer -c "SHOW POOLS"` (`cl_waiting` high, `sv_active` 0) and
   `-x -c "SHOW DATABASES"` (`paused = 1`). Fix: `RESUME localarena`. Alert: `LocalArenaPgBouncerClientsWaiting`.

## Mitigate
- Replica crash loop: `docker compose logs api --since 10m`; roll back the image (`docker compose up -d api` with the previous tag).
- DB/pooler down: see [postgres.md](postgres.md). Writes return 503 by design; reads from cache keep working.
- OPA down: `docker compose up -d opa`. v2 writes fail closed with 503 until it is back.
- Bad deploy: revert, then confirm the 5xx rate drops within 5 m (the fast alert's short window).

## Verify
`slo:api_errors:ratio_rate5m` < 0.005 and the alert resolves (Alertmanager sends `resolved` to the pager).

## Follow up
Postmortem if the error budget consumed > 5% (`slo:error_budget_remaining:avail_30d`).
