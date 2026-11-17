#!/usr/bin/env bash
# Drill 011g: kafka-add-broker-rebalance
# Proves: a 4th broker (separate broker-only node pool, so the KRaft quorum is untouched) joins
#         empty and Cruise Control moves partitions onto it (KafkaRebalance mode add-brokers)
#         under load without errors.
# Expected: new broker Ready; rebalance proposal approved; partitions on the new broker id >0
#           afterwards. Needs a 4th kafka node: raise kafka_nodes_per_az for one AZ or set
#           kafka_nodes_per_az=2 in 20-eks first. The script removes the pool again unless
#           KEEP_BROKER=1 (removal reassigns partitions away first).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-add-broker-rebalance
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
[[ $(ready_nodes workload=kafka) -ge 4 ]] || fail "need >= 4 Ready kafka nodes (have $(ready_nodes workload=kafka)); set kafka_nodes_per_az=2 and apply 20-eks"
kafka_tools_up
load_start 100 900
t0=$SECONDS
kk apply -f - <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata: { name: drill-broker, labels: { strimzi.io/cluster: $KAFKA } }
spec:
  replicas: 1
  roles: [broker]
  storage:
    type: jbod
    volumes: [{ id: 0, type: persistent-claim, size: 20Gi, class: gp3, deleteClaim: true }]
  resources: { requests: { memory: 3Gi, cpu: "1" }, limits: { memory: 4Gi } }
  jvmOptions: { "-Xms": 2g, "-Xmx": 2g }
  template:
    pod:
      tolerations: [{ key: dedicated, operator: Equal, value: kafka, effect: NoSchedule }]
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms: [{ matchExpressions: [{ key: workload, operator: In, values: [kafka] }] }]
YAML
wait_until 1200 "kafka Ready with new broker" kafka_ready
newid=$(kk get kafkanodepool drill-broker -o jsonpath='{.status.nodeIds[0]}')
say "T_broker_added=$((SECONDS - t0))s new broker id=$newid"
kcmd kafka-topics.sh --describe --topic "$LOAD_TOPIC" | grep -c "Replicas:.*\b$newid\b" | sed 's/^/    partitions on new broker before rebalance: /' || true

kk apply -f - <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaRebalance
metadata: { name: drill-add, labels: { strimzi.io/cluster: $KAFKA } }
spec: { mode: add-brokers, brokers: [$newid] }
YAML
cleanup_add "kk delete kafkarebalance drill-add --ignore-not-found --wait=false"
rb() { [[ $(kk get kafkarebalance drill-add -o jsonpath='{.status.conditions[-1].type}') == "$1" ]]; }
wait_until 600 "proposal ready" rb ProposalReady
kk annotate kafkarebalance drill-add strimzi.io/rebalance=approve --overwrite
wait_until 1200 "rebalance done" rb Ready
say "T_rebalanced=$((SECONDS - t0))s"
kcmd kafka-topics.sh --describe | grep -c "Replicas:.*\b$newid\b" | sed 's/^/    partitions on new broker after rebalance (all topics): /' || true
load_wait 900
load_report
if [[ ${KEEP_BROKER:-0} != 1 ]]; then
  say "removing broker: Strimzi rebalance mode remove-brokers, then delete the pool"
  kk apply -f - <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaRebalance
metadata: { name: drill-remove, labels: { strimzi.io/cluster: $KAFKA } }
spec: { mode: remove-brokers, brokers: [$newid] }
YAML
  cleanup_add "kk delete kafkarebalance drill-remove --ignore-not-found --wait=false"
  rr() { [[ $(kk get kafkarebalance drill-remove -o jsonpath='{.status.conditions[-1].type}') == "$1" ]]; }
  wait_until 600 "remove proposal ready" rr ProposalReady
  kk annotate kafkarebalance drill-remove strimzi.io/rebalance=approve --overwrite
  wait_until 1200 "remove rebalance done" rr Ready
  kk delete kafkanodepool drill-broker
  wait_until 900 "kafka Ready, back to 3 brokers" kafka_ready
fi
