# Experiment 012: Kafka security checks on EKS

## Hypothesis

The TLS + SCRAM listener rejects wrong credentials, ACL changes apply without restarts, a cluster CA renewal rolls without downtime for clients that re-read the CA (but breaks clients holding a stale copy), and only verified TLS 1.2+ is offered.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- Strimzi listener `tls` on 9093 (SCRAM-SHA-512, `simple` authorizer), superuser `localarena-admin`.

## Method

Four short checks run from a throwaway pod in namespace `data` that mounts the CA and admin secrets.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
DRILL_CONFIRM=1 scripts/drills/kafka-bad-password.sh
DRILL_CONFIRM=1 scripts/drills/kafka-acl-removal.sh
DRILL_CONFIRM=1 scripts/drills/kafka-ca-renewal.sh
DRILL_CONFIRM=1 scripts/drills/kafka-tls-check.sh
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| kafka-bad-password | wrong password / unknown user | both DENIED | |
| kafka-acl-removal | T_denied after ACL removal | < 2 min | |
| kafka-ca-renewal | T_renewal; stale client | roll completes; stale client fails or overlaps | |
| kafka-tls-check | verify code; TLS1.1; port 9092 | 0 (ok); refused; closed | |

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
