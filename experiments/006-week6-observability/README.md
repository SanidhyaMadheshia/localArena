# Experiment 006: Week 6 observability, SLOs, recovery, and a hidden incident

## Hypothesis

With traces, metrics and logs correlated by `trace_id`, multi-window burn-rate
SLO alerts page within minutes of a real failure, a responder can find the cause
from telemetry alone, a logical backup restores to a fresh instance with
verified integrity, and promoting the async replica bounds RTO to seconds while
RPO depends on replication lag.

## System version and configuration

- 2026-10-18. OpenTelemetry Go 1.47 (otelhttp 0.72, otelpgx 0.13), OTel Collector
  contrib 0.163.1, Tempo 3.0.4, Loki 3.6.18 + Alloy 1.20.1, Prometheus 3.13.4,
  Alertmanager 0.34.1, Grafana 13.2.4, postgres-exporter 0.20.1,
  pgbouncer-exporter 0.12.1.
- `docker compose --profile observability up -d`. Grafana `localhost:3000`
  (dashboard "LocalArena overview", datasources linked: metrics ↔ traces ↔ logs),
  Prometheus `:9090`, Alertmanager `:9093`, lab pager `:9095/alerts`.
- SLOs and alerts: `infra/observability/rules/slo.yml`, unit-tested with
  `promtool test rules` (`rules-tests/slo_test.yml`, 5 cases).

## Results

### Telemetry

| Signal | Evidence |
|---|---|
| Traces | `POST /v1/purchases` trace in Tempo: server span with `BEGIN → INSERT → SELECT → COMMIT → SELECT` SQL child spans (otelpgx); spans named by route pattern |
| Metrics | RED per route (`localarena_http_requests_total`, `..._duration_seconds`, label = matched pattern; `TestREDRouteLabels` proves raw paths never become labels), game, events, cache, DB pool, Postgres, PgBouncer, Envoy, collector |
| Logs | all containers via Alloy → Loki, labels `service`/`level` only; API errors carry `trace_id` (`TestLogsCarryTraceIDs`), linked to Tempo in Grafana |

### Backup and restore (`backup-restore.sh`, `results/backup-restore.txt`)

| Step | Result |
|---|---|
| `pg_dump -Fc` of the live primary **while 300 writes ran** | 1.8 MB in 0.49 s, consistent snapshot |
| `pg_restore` into a brand-new Postgres 18.6 container | 0.61 s |
| Integrity on the restored copy | players 1020, purchases 1197, credits sum, inventory, 200,000 match_players, outbox, 5 partitions, migration version 12; 0 unvalidated constraints, 0 negative balances |
| Application check | the `migrate` binary reads schema version 12 from the restored DB |

The README gate "a fresh database is restored from backup" is met. RPO of
nightly logical dumps is up to 24 h; WAL archiving is needed for less.

### Failover RPO/RTO (`failover-rpo.sh`: kill -9 the primary, promote the replica)

| Run | Replica state before the crash | Acknowledged writes lost (RPO) | RTO (promote / first write) |
|---|---|---:|---|
| healthy | streaming, replay lag 0.7 ms | **0 of 143** | 0.46 s / 0.71 s |
| partitioned 5 s | cut off from WAL for 5 s | **139 of 278 (2.78 s of writes)** | 0.55 s / 0.70 s |

Async replication has **RPO = current replication lag**: zero in the
healthy case, everything since the partition began otherwise. The client
had already been told those 139 commits succeeded. A zero-RPO requirement
for credits would need `synchronous_commit = remote_apply` with a quorum
standby, at the cost of write latency and of writes stalling when the
standby is down.

### Hidden incident (`incident.sh`, postmortem `postmortems/PM-001-pgbouncer-paused.md`)

One of three faults was picked at random under 100 req/s and the answer was sealed.

| Phase | Time |
|---|---|
| Fault → first page (`AvailabilityBurnFast`) | 1 m 40 s |
| Start of diagnosis → cause identified | 1 m 6 s |
| Fault → mitigation (`RESUME`) | 3 m 46 s |
| Fast alert resolved | 4 m 54 s after mitigation (5 m window) |
| Diagnosis | **correct** (`pgbouncer-pause`) |

The diagnosis path: errors only on primary-pool routes → logs show deadline
cancellations, not SQL errors → the slow trace has **no SQL span** → Postgres
idle with no lock waits → PgBouncer `cl_waiting=30, sv_active=0, paused=1`.

## Findings fixed during the experiment

1. **False page:** the tick-budget SLI is `1 - good/total`; with zero ticks it
   evaluated to 100% and paged during the incident. The alert now requires a
   non-zero tick rate (regression test added).
2. **Missing cause signal:** no PgBouncer metrics. Added the exporter and
   `LocalArenaPgBouncerClientsWaiting` (promtool-tested).
3. Error-ratio recording rules returned *no data* (not 0) when there were
   no errors, because the `rate()` of an absent series is empty. Numerators
   now use `or vector(0)`.
4. Trace and metric route names first all read `/v1/`: the outer mux matched
   the versioned sub-mux. The route pattern is now taken from the innermost
   mux.
5. Config drift with current versions: Tempo 3 moved retention to
   `backend_worker`, postgres-exporter renamed `replication_slot` to
   `replication_slots`, and the collector's metrics endpoint now needs an
   explicit Prometheus reader.

## Remaining risks

- The lab has little history, so 30 d error-budget numbers are not meaningful yet.
- `TargetDown` fires for services in stopped Compose profiles (PM-001 action 4).
- Failover in Compose is manual. CNPG automates it in Kubernetes
  (Experiment 003) but has the same async RPO.
- Tempo and Loki are single-node with local storage (lab retention 24 h / 72 h).
