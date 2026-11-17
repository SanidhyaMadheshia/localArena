# Experiment 013: Postgres failover, Streams faults, restore from S3

## Hypothesis

Killing the CNPG primary during purchases yields a short bounded error window. Streams (EXACTLY_ONCE_V2) recovers from a killed pod and a node drain with lag returning to zero. A base backup plus WAL in S3 (Pod Identity, no static keys) restores into a new cluster with the data intact.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- CNPG `arena-db` (3 instances) in `localarena`, PgBouncer pooler, bucket from `10-network` (`pg_backup_bucket`), Pod Identity for service accounts `arena-db` and `arena-db-restore`.

## Method

Each drill injects the fault and prints timings. The restore drill writes a marker row, takes a Backup, restores into `arena-db-restore`, and checks the marker.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
DRILL_CONFIRM=1 scripts/drills/pg-failover-during-purchases.sh
DRILL_CONFIRM=1 scripts/drills/streams-kill-drain.sh
DRILL_CONFIRM=1 scripts/drills/pg-restore-from-s3.sh
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| pg-failover-during-purchases | error window; status counts | a few seconds to ~30 s of 5xx | |
| streams-kill-drain | T_kill_recovered / T_drain_recovered | 1-3 min each; lag back to ~0 | |
| pg-restore-from-s3 | T_backup / T_restore_RTO; marker | completed; marker present | |

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
