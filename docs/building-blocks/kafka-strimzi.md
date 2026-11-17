# Kafka on Strimzi (kind 3-zone, then EKS)

One set of manifests runs on a local 3-zone `kind` cluster and on EKS. Only the kustomize overlay changes.

## What is in the repo

| Path | Purpose |
|---|---|
| `infra/kubernetes/kind/cluster-3zone.yaml` | kind cluster `localarena-3z`: 1 control plane, 3 workers labelled `topology.kubernetes.io/zone=a/b/c` |
| `infra/kubernetes/helm/strimzi-values.yaml` | Strimzi operator values (chart 1.3.1, watches namespace `data` only) |
| `infra/kafka/base/kafka.yaml` | `KafkaNodePool` (3 combined controller+broker nodes, one per zone) and `Kafka` (KRaft, Kafka 4.3.1) |
| `infra/kafka/base/topics.yaml` | 13 `KafkaTopic` resources, converted from `scripts/kafka-topics.sh` |
| `infra/kafka/base/users.yaml` | `KafkaUser` per service with ACLs (comments say why each ACL exists) |
| `infra/kafka/base/metrics-configmap.yaml` | JMX exporter rules for Prometheus |
| `infra/kafka/overlays/kind` | 2Gi volumes on `standard` |
| `infra/kafka/overlays/eks` | 20Gi `gp3`, `Retain` volumes, 4Gi memory, `workload=kafka` node affinity, `dedicated=kafka` toleration |
| `infra/kafka/verify/noacl-user.yaml` | A user with no ACLs, used to prove denial |
| `scripts/k8s-kafka-copy-secrets.sh` | Copies KafkaUser secrets and the cluster CA from `data` to `localarena` |
| `scripts/k8s-kafka-verify.sh` | End-to-end check (see below) |
| `scripts/k8s-kafka-gen-topics.sh` | Regenerates `topics.yaml` |

Broker settings: replication factor 3, `min.insync.replicas=2`, `auto.create.topics.enable=false`, `RackAwareReplicaSelector`, TLS listener on 9093 with SCRAM-SHA-512, `authorization: simple`, Cruise Control and Kafka Exporter enabled.

Topic resource names use dashes (`game-match-events-v1`). The Kafka topic name stays `game.match-events.v1` through `spec.topicName`.

## Run it on kind

```bash
make k8s3z-cluster     # about 1 minute
make k8s3z-kafka       # operator, Kafka, topics, users, secret copy; about 5 minutes
make k8s3z-verify      # end-to-end check
make k8s3z-down        # delete the cluster
```

`DOCKER_CONTEXT=default` is set by the targets. Docker Desktop's socket may be dead on this laptop. The native engine works.

## Switch to EKS

```bash
make k8s3z-kafka K3Z_CTX=<eks-context> K3Z_OVERLAY=eks
```

Needs the `gp3` StorageClass (`infra/aws/manifests/gp3-storageclass.yaml`) and the `kafka` node group from `infra/aws/20-eks`. The Strimzi helm step is the same.

## What `k8s3z-verify` proves

Verified on 2026-11-17 on `kind-localarena-3z`:

- All 13 topics are `Ready` with replication factor 3.
- The 3 brokers run on 3 different worker nodes, with `broker.rack` = `c`, `b`, `a`.
- `localarena-gameserver` produces over TLS + SCRAM (`PRODUCE_OK`).
- `localarena-eventcat` reads that record back.
- `localarena-noacl` is rejected with `TopicAuthorizationException`.
- `localarena-gameserver` is rejected when reading a topic it has no ACL for.

The Go client path was also checked: `eventcat` built with `kafkaconf` read a record from a pod using only `KAFKA_*` env vars (see [kafka-client-auth.md](kafka-client-auth.md)).

## Gotchas hit while building this

- **Namespaces.** The Strimzi User Operator writes secrets only into `data`. Apps in `localarena` need the copy script. Secret names and SCRAM usernames are both `localarena-<service>` (the Helm `kafka.users` value uses the same name).
- **YAML booleans.** A volume named `no` parses as `false` and the API rejects the pod. Use names like `na`.
- **`set -o pipefail` with `grep -q`.** `grep -q` exits at the first match, the writer gets SIGPIPE, and the pipeline reports failure. Capture output in a variable, then grep it.
- **Rack awareness needs two things.** `rack.topologyKey` only labels the broker. The pod `topologySpreadConstraints` in `kafka.yaml` is what puts one broker in each zone.
- **Memory.** Local brokers use a 512 MiB heap. EKS brokers use 2 GiB heap and a 4 GiB limit on `m7i.large` (8 GiB).
- **Silent client failures.** `eventcat` prints nothing with a wrong password or missing CA. franz-go reports auth errors only through a logger. Drill 012 should add a logger to see the error.

## Not built yet

The plan's section 3.1 also lists these. None exists as a Kubernetes manifest yet:

- rewards Deployment and Streams StatefulSet (state PVC, 1 standby replica)
- Strimzi `KafkaConnect` with Debezium plus a `KafkaConnector` from `connect/outbox-connector.json`
- Apicurio Registry
- CNPG settings for logical replication (`wal_level=logical`)
- kube-prometheus-stack and the `PrometheusRule` for `slo.yml`

The Go and Java clients already speak TLS + SCRAM, so these are Kubernetes packaging work. Build them on this kind cluster before the AWS week.

## Troubleshooting

- `Kafka` stuck not `Ready`: `kubectl -n data describe kafka localarena` and the operator log `kubectl -n strimzi-system logs deploy/strimzi-cluster-operator`.
- Pod `Pending` on EKS: a PVC is bound to another AZ's volume. Check `kubectl get pv -o wide` and the node zone.
- User secret missing in `localarena`: rerun `scripts/k8s-kafka-copy-secrets.sh`.
- TLS errors from a client: the CA is in secret `localarena-cluster-ca-cert`, key `ca.crt`. Point `KAFKA_CA_FILE` at the mounted file.
