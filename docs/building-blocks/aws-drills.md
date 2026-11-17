# AWS drills

Scripts in `scripts/drills/`. Every drill is safe by default: it exits with code 64 unless
`DRILL_CONFIRM=1`. Each prints UTC timestamps and writes a log to `experiments/<exp>/results/`.
Prerequisite: `scripts/aws-up.sh` finished, and for Kafka drills the Strimzi overlay
(`kubectl apply -k infra/kafka/overlays/eks`). Experiment names for 010 to 015 are proposed here;
the README in each `experiments/0NN-*` directory has the fill-in results table.

Plan days are a suggestion for the 7-day window (day 1 = bring-up).

| Drill script | Proves | Expected result | Plan day |
|---|---|---|---|
| `node-per-az.sh` | One Ready node per workload per AZ; kafka taint present | PASS, 1 node in each cell | 1 |
| `pvc-az-pin.sh` | EBS volume pins a pod to its AZ | Pending (volume node affinity conflict) with zone cordoned, then Running with data | 1 |
| `terminate-node-timing.sh` | Node loss detection and replacement time | NotReady in ~1 min, replacement Ready in 3 to 6 min | 1 |
| `api-private-check.sh` | API not open to 0.0.0.0/0; private DNS in VPC; pods blocked from IMDS | No world CIDR; 10.40.x.x; IMDS times out | 1 |
| (010: platform + app) | `values-eks.yaml`, Strimzi overlay, metrics-server without insecure TLS | Pods spread per AZ, `kubectl top` works | 2 |
| `kafka-broker-pod-delete.sh` | One broker pod lost under acks=all load | 0 producer errors, URP 0 in under 3 min | 3 |
| `kafka-ec2-terminate.sh` | Broker node lost; volume reattaches in same AZ | Broker back in 4 to 8 min, 0 errors | 3 |
| `kafka-disk-expand.sh` | Online volume growth, no shrink | PVCs at new size, no restart loop | 3 |
| `kafka-rolling-change.sh` | Static config change rolls one broker at a time | 5 to 10 min, 0 errors | 3 |
| `kafka-az-isolate.sh` | One AZ isolated by NACL (restore trap) | Writes continue (minISR 2 of 3); recovers after restore | 4 |
| `kafka-two-az-isolate.sh` | Two AZs isolated | Writes stall, no acked loss, recovers after restore | 4 |
| `kafka-add-broker-rebalance.sh` | 4th broker via separate pool, Cruise Control add-brokers | Partitions land on new broker, 0 errors; needs 4 kafka nodes | 4 |
| `kafka-crossaz-flowlogs.sh` | Cross-AZ share of Kafka bytes from flow logs | >= 2/3 cross-AZ; needs `enable_flow_logs=true` (waits ~12 min for delivery) | 4 |
| `kafka-bad-password.sh` | SCRAM rejects wrong password and unknown user | Both DENIED, broker log shows failure | 5 |
| `kafka-acl-removal.sh` | ACL removal takes effect live | Produce denied within 1 to 2 min | 5 |
| `kafka-ca-renewal.sh` | Cluster CA renewal roll; stale client copies break | Roll completes, fresh client OK, stale client fails or overlaps | 5 |
| `kafka-tls-check.sh` | openssl s_client: verified TLS 1.2+, no plaintext | Verify code 0, TLS 1.1 refused, 9092 closed | 5 |
| `pg-failover-during-purchases.sh` | Primary kill during purchases | 5xx window of seconds up to ~30 s, then clean | 5 |
| `streams-kill-drain.sh` | Streams pod kill and node drain | Pod back in 1 to 3 min, lag returns to ~0 | 5 |
| `pg-restore-from-s3.sh` | Backup and WAL in S3 restore into a new cluster | Marker row present; RTO printed | 5 |
| `eks-upgrade.sh` | 1.36 to 1.37: control plane, add-ons, node groups, API probe | 0 failed probes; each phase timed | 6 |
| `msk-provision.sh` | MSK SCRAM cluster via `30-msk`, perf test from a pod | Create 25 to 40 min; produce test completes | 7 |
| `msk-delete.sh` | MSK stack destroys cleanly | No cluster left; then run `aws-teardown.sh` | 7 |
| `nacl-restore.sh <state>` | Emergency restore of NACL associations | Subnets back on original ACLs | any |

Environment shared by the drills: `AWS_REGION` (us-east-1), `CLUSTER_NAME` / `KUBE_CONTEXT` (`localarena`),
`KAFKA` (`localarena`), `KAFKA_NS` (`data`), `APP_NS` (`localarena`), `KAFKA_IMAGE`, `LOAD_TOPIC`
(`drill.load`). Kafka drills create a throwaway pod `drill-kafka-tools` and delete it on exit.

Order matters on day 4: run `kafka-az-isolate` and `kafka-two-az-isolate` alone, never together with a
rolling change, and wait for `URP == 0` between drills.
