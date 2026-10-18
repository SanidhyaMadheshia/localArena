# ADR-0017: SLOs with multi-window burn-rate alerts; backup and failover posture

- Status: accepted
- Date: 2026-10-18

## Decision

- **SLOs (30 d):**
  - API availability 99.5% non-5xx.
  - API latency 99% < 300 ms.
  - Game ticks 99.9% < 50 ms.
  - Durable events 99% delivered.
  - Recorded as `slo:*` rules.
- **Burn-rate alerts** (Google SRE workbook):
  - Page when the 1 h **and** 5 m burn rates exceed 14.4× (2% of the budget in 1 h).
  - Ticket when the 6 h **and** 30 m burn rates exceed 6×.
  - The short window makes alerts resolve soon after a fix.
  - Every alert has a `runbook_url` (`runbooks/`). Rules are unit-tested with `promtool test rules`, including "healthy traffic does not page" and "no traffic does not page".
- **Cause alerts** are tickets, except the PgBouncer-stuck page (PM-001). Alertmanager routes `severity=page` and `ticket` separately and inhibits SLO pages while a target is down. The lab pager is a webhook sink.
- **Backups:** logical `pg_dump -Fc` (consistent, online), restored and verified by a repeatable drill (`backup-restore.sh`). Continuous WAL archiving / CNPG backups are planned for lower RPO.
- **Failover:** async streaming replica promotion. Measured RTO is about 0.7 s for the DB; RPO equals replication lag at the moment of failure (0 when healthy, 2.8 s of writes after a 5 s partition). Accepted for the lab; credits would need synchronous quorum commit in production.

## Consequences

- Short incidents (a few minutes at full outage) page; low-level noise only creates tickets.
- An SLI built as `1 - good/total` must guard against `total = 0`, which reads as 100% bad (PM-001).
- RPO is a property of replication state, not of the failover tooling; it has to be monitored (replica lag alert).
