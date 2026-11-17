# Experiment 014: EKS minor version upgrade under load

## Hypothesis

A one-minor upgrade (1.36 to 1.37) done as control plane, then add-ons, then node groups one at a time (kafka last) keeps the API available and Kafka producing, because of PDBs and `max_unavailable=1`.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- Starting at 1.36, target 1.37 (newest on EKS as of 2026-11-17). `upgrade_policy` STANDARD.

## Method

The drill runs pre-checks (upgrade insights, deprecated APIs, PDBs), runs an API probe every 2 s, then upgrades in order and records each phase.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
WITH_KAFKA_LOAD=1 DRILL_CONFIRM=1 scripts/drills/eks-upgrade.sh   # TARGET_VERSION=1.37
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| control plane | T_control_plane | 8-15 min | |
| add-ons | T_addons | 5-10 min | |
| node groups (6) | T_nodegroup[...] | ~5-10 min each | |
| API probe | failed /readyz probes | 0 | |
| Kafka load | producer errors | 0 | |

After the run set `kubernetes_version = "1.37"` for `20-eks` and confirm `tofu plan` shows no changes.

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
