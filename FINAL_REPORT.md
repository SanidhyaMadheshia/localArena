# LocalArena: final report (8 weeks)

LocalArena is a server-authoritative 2D multiplayer arena plus the backend around it: identity, economy, matchmaking-ready lobbies, event streaming, observability, delivery and multi-region operations. It was built and measured on one Linux laptop. This report covers what was built, what failed, why, and which design changes fixed it. Details live in `adr/` (22 ADRs), `experiments/` (8 reports), `postmortems/` (2) and `runbooks/` (5). A study guide with HLD/LLD, diagrams and interview questions for every decision is in `docs/`.

## Final success criteria (README §2)

| # | Criterion | Evidence |
|---|---|---|
| 1 | Runs on Docker Compose and local Kubernetes | Compose profiles (core, streaming, security, observability, regions); kind with Helm + Argo CD (Exp 007) |
| 2 | Authoritative fixed tick; clients send input only | ADR-0001; engine clamps inputs; deterministic replay test |
| 3 | ≥ 200 bots, ≥ 10 matches, saturation documented | 1000 bots / 50 matches at tick p99 0.76 ms; overload at 2000 bots / 200 Hz / 1 CPU: fan-out-bound, not simulation-bound (Exp 001) |
| 4 | Ticks within 50 ms at 20 Hz with p50/p95/p99 | p50 0.07, p95 0.21, p99 0.76 ms at 1000 players (Exp 001); SLO alert (Exp 006) |
| 5 | No duplicate matchmaking/rewards/purchases | constraint-first DB, idempotency keys, deterministic reward IDs; 1.45 M duplicate events changed nothing (Exp 002, 004) |
| 6 | API survives a stateless replica failure | kill / pause / graceful stop: 0 failed requests (Exp 003); rolling, canary, blue-green, rollback: 0 failed (Exp 007) |
| 7 | Safe degradation (cache, queue, downstream) | Valkey down → Postgres reads; Kafka down 60 s → 0 lost events, ticks unaffected; OPA down → writes fail closed (Exp 003-005) |
| 8 | Logs, metrics, traces, dashboards, alerts | OTel → Tempo, Loki, Prometheus SLO burn-rate alerts, Grafana (Exp 006) |
| 9 | Probes, limits, rolling update, canary, rollback | Helm chart + drills, HPA 2→5→2 (Exp 007) |
| 10 | Backup/restore, failure injection, regional failover, postmortem | restore verified, RPO/RTO measured (Exp 006); regions and DNS TTL (Exp 008); PM-001, PM-002 |

## What failed, why, and what changed

| Week | Failure | Cause | Change |
|---|---|---|---|
| 1 | Slow clients seconds behind with an empty mailbox | kernel socket buffers absorb snapshots | client reports last tick; server throttles laggers (never cuts them off) |
| 2 | `FOR UPDATE` deadlocked every buyer | FK insert holds KEY SHARE; upgrade conflicts | `FOR NO KEY UPDATE`; conditional updates as the default |
| 2 | 3-minute failover, then 15 s more | pooler keeps smart shutdown waiting; cached login error | `smartShutdownTimeout: 10`, `server_login_retry: 1` → 1 s |
| 3 | 7,482 proxy retries in a DB brownout | per-try timeout shorter than DB timeout | timeout order DB < per-try < API < route; dependency 503s not retried |
| 3 | No failover on node loss | single operator pod, pooler and API on one node, `/readyz` checked the DB | ×2 with spread; readiness = this replica only |
| 4 | Spool never replayed | delivery timeout measured from the old record timestamp | stamp produce time; event time stays in the Envelope |
| 5 | WAF missed query-string attacks | Envoy `safe_regex` matches only the path | match the `:path` header |
| 6 | False page with no games running | `1 - 0/ε` reads as 100% bad | guard `total > 0`; promtool regression test |
| 7 | Helm vs Argo CD conflicts; rollback deleted the pooler | server-side apply ownership; first-install uninstall | one deployer per app; `resource-policy: keep` |
| 8 | 500s during an inter-region partition | dead pooled connections surface as EOF | EOF/reset/net errors → 503 |
| 8 | No rewards since week 5 | tickets disabled `?player=`; bots became guests | explicit lab flag + business-outcome alert |

## Benchmarks (one host; relative, not capacity claims)

- **Gameplay:**
  - 1000 players: tick p99 0.76 ms, about 30 KiB and 15 KB/s per player, 50 matches on 4 cores.
- **API:**
  - 300 req/s through Envoy: p99 8 ms.
  - 600 req/s in kind with HPA: 0 of 90,000 failed.
- **Database:**
  - Purchase contention, 200 buyers for 20 items: 129 ms with conditional updates.
  - Indexed history (limit-first): 0.8 ms.
- **Streaming:**
  - Streams failover: 11 s.
  - Full state restore: 10.9 s.
  - CDC crash mid-backlog: 0 lost, 0 duplicates.
- **Recovery:**
  - Restore of a 1.8 MB dump: 0.6 s.
  - Async replica failover: RTO 0.7 s; RPO 0 when healthy, 2.8 s of writes after a 5 s partition.
- **Regions:**
  - Remote write: 805 ms vs 35 ms local.
  - Client re-selection: 0 failures.
  - DNS TTL 30 s gives about 34 s for resolver-cached clients.
- **Cost:** see `experiments/008-week8-regions/cost.md`. Egress dominates (≈ 39 GB per CCU-month).
