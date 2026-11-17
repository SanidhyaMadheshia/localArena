# Experiment 015: Amazon MSK as the managed alternative

## Hypothesis

Provisioned MSK (3 brokers, one per AZ, SASL/SCRAM over TLS) runs the same producer workload with similar latency to the Strimzi cluster, at the cost of a long create time and hourly billing.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- `infra/aws/30-msk` (optional stack): 3 x `kafka.m5.large`, 20 GiB per broker, Kafka 3.9.x, KMS-encrypted SCRAM secret in Secrets Manager.

## Method

Provision, run `kafka-producer-perf-test` from a pod in the cluster, then destroy.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
DRILL_CONFIRM=1 scripts/drills/msk-provision.sh
DRILL_CONFIRM=1 scripts/drills/msk-delete.sh
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| msk-provision | T_msk_create | 25-40 min | |
| msk-provision | producer p50/p99 latency, throughput | compare with 011 | |
| msk-delete | T_msk_delete; leftovers | 10-20 min; none | |

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
