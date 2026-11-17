# Experiment 010: platform and application on EKS

## Hypothesis

The same Helm charts and kustomize overlays that run on kind run on EKS with only the EKS values overlays (ECR registry, gp3, zone spread): CNPG with 3 instances across AZs, Strimzi Kafka on the dedicated kafka nodes, metrics-server without `--kubelet-insecure-tls`.

## System version and configuration

- Burner AWS account, one region (`AWS_REGION`, default us-east-1), 7-day window. Date run: _fill in_.
- Stacks: `infra/aws/10-network`, `infra/aws/20-eks` (EKS 1.36, `m7i.large` nodes, one managed node group per AZ for `general` and `kafka`), platform via `infra/aws/platform` (see `docs/building-blocks/aws-stacks.md`).
- Images pushed to ECR repositories `localarena/{api,gameserver,bots,streams}`. `insecure_kubelet_tls=false`.

## Method

1. Platform: `scripts/aws-up.sh --step platform` (CNPG operator, metrics-server, Argo CD; `manage_app=false`).
2. Push images, apply Kafka overlay, install the app chart with `values-eks.yaml`.
3. Check pods spread across zones, `kubectl top nodes` works, PVCs bound on gp3.

Every drill refuses to run without `DRILL_CONFIRM=1` and writes a timestamped log to `results/`.

## How to run

```bash
scripts/aws-up.sh --yes                 # day 1 only, once
scripts/aws-up.sh --step platform --yes
kubectl --context localarena apply -k infra/kafka/overlays/eks
helm upgrade --install localarena infra/helm/localarena -n localarena -f infra/helm/localarena/values-eks.yaml --set image.registry=<account>.dkr.ecr.<region>.amazonaws.com
```

## Results (fill in)

| Drill / check | Metric | Expected | Measured |
|---|---|---|---|
| metrics-server | kubectl top nodes works without insecure TLS | works | |
| CNPG pods per AZ | instances spread | 1 per AZ | |
| Kafka brokers per AZ | pods on kafka nodes | 1 per AZ | |
| App smoke | GET /healthz through the load balancer | 200 | |

## Findings

_Write after the run: what surprised you, what you would change in the building blocks._
