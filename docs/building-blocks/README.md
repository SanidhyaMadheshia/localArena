# Building blocks for the AWS week

Reusable pieces prepared on a laptop so the 7-day AWS account is spent on AWS-specific lessons. The schedule is in [../localarena-aws-plan.md](../localarena-aws-plan.md).

| Block | Doc | Status |
|---|---|---|
| 3-zone kind + Strimzi Kafka, topics, users, ACLs | [kafka-strimzi.md](kafka-strimzi.md) | Built and verified on kind |
| Kafka client auth (Go `kafkaconf`, Java Streams env mapping, Helm `kafka.auth`) | [kafka-client-auth.md](kafka-client-auth.md) | Built. Go tests, Streams tests and a live Go read pass |
| AWS stacks: preflight, network, EKS, MSK (optional), platform, scripts | [aws-stacks.md](aws-stacks.md) | Written. `tofu validate` and `fmt` pass. Never applied (no credentials) |
| Drill scripts per experiment | [aws-drills.md](aws-drills.md) | Written. Syntax-checked only |

## Fast path on the day

```bash
# Before the account: prove the local half
make k8s3z-cluster k8s3z-kafka k8s3z-verify

# Day 1, with burner-account credentials
infra/aws/00-preflight/preflight.sh
scripts/aws-up.sh --yes
make k8s3z-kafka K3Z_CTX=<eks-context> K3Z_OVERLAY=eks
helm upgrade --install localarena infra/helm/localarena -f infra/helm/localarena/values-eks.yaml -n localarena --create-namespace

# End of week
scripts/aws-teardown.sh
```

Check [aws-stacks.md](aws-stacks.md) for the exact flags and required variables before running `aws-up.sh`.

## Known gaps

- No AWS command in this repo has run against a real account. Expect small fixes on day 1.
- Rewards, Streams, Connect, Apicurio and kube-prometheus-stack have no Kubernetes manifests yet (listed in [kafka-strimzi.md](kafka-strimzi.md)).
- Docker Desktop's socket is dead on this machine. Use `DOCKER_CONTEXT=default`.
