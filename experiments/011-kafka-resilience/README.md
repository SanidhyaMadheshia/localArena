# Experiment 011: Kafka (Strimzi) failure and operations drills on EKS

## Hypothesis

With 3 brokers spread one per AZ, RF=3 and min.insync.replicas=2, an acks=all producer survives the loss of a broker pod, a node, or a whole AZ without losing acknowledged records. Losing two AZs stops writes safely. Disk growth, rolling config changes and adding a broker are online operations.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- Strimzi `localarena` in namespace `data`, Kafka 4.3.1 KRaft, 3 combined controller+broker nodes, gp3 20Gi, rack awareness on zone.
- Load: `kafka-producer-perf-test`, 100 records/s, acks=all, idempotent, topic `drill.load` (6 partitions, RF 3, minISR 2).

## Method

Each drill starts load, injects one fault, and records recovery timestamps and producer errors. NACL drills replace a private subnet's ACL with an empty one and restore it on exit (`scripts/drills/nacl-restore.sh` if the shell dies).

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
DRILL_CONFIRM=1 scripts/drills/kafka-broker-pod-delete.sh
DRILL_CONFIRM=1 scripts/drills/kafka-ec2-terminate.sh
DRILL_CONFIRM=1 scripts/drills/kafka-az-isolate.sh
DRILL_CONFIRM=1 scripts/drills/kafka-two-az-isolate.sh
DRILL_CONFIRM=1 scripts/drills/kafka-disk-expand.sh
DRILL_CONFIRM=1 scripts/drills/kafka-rolling-change.sh
DRILL_CONFIRM=1 scripts/drills/kafka-add-broker-rebalance.sh
DRILL_CONFIRM=1 scripts/drills/kafka-crossaz-flowlogs  # needs 10-network enable_flow_logs=true.sh
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| kafka-broker-pod-delete | T_pod_ready / T_urp_zero / producer errors | ~1-2 min / <3 min / 0 | |
| kafka-ec2-terminate | T_broker_ready / T_urp_zero / errors | 4-8 min / <10 min / 0 | |
| kafka-az-isolate (1 AZ) | writes during isolation; T_recovered | writes continue; recovers after restore | |
| kafka-two-az-isolate | writes during isolation; data loss | writes stall; no acked loss | |
| kafka-disk-expand | T_resized; restarts | minutes; no restart loop | |
| kafka-rolling-change | T_roll; errors | 5-10 min; 0 | |
| kafka-add-broker-rebalance | T_broker_added / T_rebalanced | <10 min / <15 min | |
| kafka-crossaz-flowlogs | cross-AZ share of Kafka bytes | >= 2/3 | |

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
