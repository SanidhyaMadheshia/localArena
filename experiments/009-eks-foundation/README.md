# Experiment 009: EKS foundation, AZ placement, node loss, private API

## Hypothesis

A node group per AZ gives every AZ exactly one node per workload, so zonal EBS volumes can always be reattached to a node in their own AZ. A pod whose volume lives in a cordoned AZ stays Pending instead of silently losing data. A terminated node is replaced in minutes. The API endpoint is restricted to the admin CIDR and pods cannot reach IMDS.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- NAT: 1 gateway (single egress AZ). IMDSv2 required, hop limit 1. Access entries (API auth mode), no aws-auth ConfigMap.

## Method

1. `scripts/aws-up.sh` (preflight, network, eks, gp3, platform, ECR).
2. Run the four drills below.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
DRILL_CONFIRM=1 scripts/drills/node-per-az.sh
DRILL_CONFIRM=1 scripts/drills/pvc-az-pin.sh
DRILL_CONFIRM=1 scripts/drills/terminate-node-timing.sh
DRILL_CONFIRM=1 scripts/drills/api-private-check.sh
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| node-per-az | Ready nodes per (workload, zone) | 1 each; kafka tainted | |
| pvc-az-pin | pod state with its zone cordoned | Pending, volume node affinity conflict, then Running with data | |
| terminate-node-timing | T_notready / T_replaced | ~1 min / 3-6 min | |
| api-private-check | public CIDRs, in-VPC DNS, IMDS | no 0.0.0.0/0; 10.40.x.x; IMDS timeout | |

Bring-up timings (fill in): preflight ___ s, 10-network ___ s, 20-eks ___ s, platform ___ s.

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
