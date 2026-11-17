#!/usr/bin/env bash
# Shared helpers for the AWS drills. Source it, do not run it.
#   EXP=009-eks-foundation DRILL=node-per-az; source "$(dirname "$0")/_lib.sh"; drill_init
# shellcheck shell=bash disable=SC2034,SC2016
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION" AWS_PAGER=""
CLUSTER_NAME="${CLUSTER_NAME:-localarena}"
KUBE_CONTEXT="${KUBE_CONTEXT:-$CLUSTER_NAME}"
APP_NS="${APP_NS:-localarena}"
KAFKA_NS="${KAFKA_NS:-data}"
KAFKA="${KAFKA:-localarena}" # Strimzi Kafka CR name (infra/kafka/base/kafka.yaml)
KAFKA_BS="${KAFKA}-kafka-bootstrap.${KAFKA_NS}.svc:9093"
KAFKA_IMAGE="${KAFKA_IMAGE:-quay.io/strimzi/kafka:1.3.1-kafka-4.3.1}"
LOAD_TOPIC="${LOAD_TOPIC:-drill.load}"
AWS_INFRA="$ROOT/infra/aws"

ts() { date -u +%H:%M:%SZ; }
say() { printf '[%s] %s\n' "$(ts)" "$*"; }
fail() { say "FAIL: $*"; exit 1; }
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
kk() { k -n "$KAFKA_NS" "$@"; }

_CLEANUP=()
cleanup_add() { _CLEANUP+=("$1"); }
_run_cleanup() {
  local i
  for ((i = ${#_CLEANUP[@]} - 1; i >= 0; i--)); do eval "${_CLEANUP[i]}" || true; done
}

# Safe by default: nothing runs without DRILL_CONFIRM=1. Output is also written to
# experiments/$EXP/results/$DRILL-<timestamp>.log
drill_init() {
  : "${EXP:?}" "${DRILL:?}"
  if [[ ${DRILL_CONFIRM:-} != 1 ]]; then
    echo "Refusing to run drill '$DRILL': it changes or breaks live infrastructure." >&2
    echo "Read the header of $0, then re-run with DRILL_CONFIRM=1." >&2
    exit 64
  fi
  RESULTS="$ROOT/experiments/$EXP/results"
  mkdir -p "$RESULTS"
  LOG="$RESULTS/$DRILL-$(date -u +%Y%m%dT%H%M%SZ).log"
  exec > >(tee -a "$LOG") 2>&1
  trap _run_cleanup EXIT
  say "drill=$DRILL exp=$EXP region=$AWS_REGION context=$KUBE_CONTEXT log=$LOG"
  command -v kubectl >/dev/null || fail "kubectl missing"
  k get ns >/dev/null || fail "cluster unreachable with context $KUBE_CONTEXT"
}

# wait_until <timeout-s> <description> <cmd...>; prints seconds waited, sets WAITED.
wait_until() {
  local timeout=$1 desc=$2 start=$SECONDS
  shift 2
  until "$@" >/dev/null 2>&1; do
    ((SECONDS - start < timeout)) || { say "TIMEOUT after ${timeout}s waiting for: $desc"; WAITED=$timeout; return 1; }
    sleep 5
  done
  WAITED=$((SECONDS - start))
  say "$desc: ${WAITED}s"
}

# ---- AWS helpers
node_zone() { k get node "$1" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'; }
node_instance() { k get node "$1" -o jsonpath='{.spec.providerID}' | sed 's|.*/||'; }
ready_nodes() { k get nodes -l "$1" --no-headers 2>/dev/null | awk '$2=="Ready"' | wc -l; }
private_subnet_for_az() { # az-name -> subnet id (from 10-network state)
  local az=$1 i=0 a
  for a in $(tofu -chdir="$AWS_INFRA/10-network" output -json azs | jq -r '.[]'); do
    [[ $a == "$az" ]] && { tofu -chdir="$AWS_INFRA/10-network" output -json private_subnet_ids | jq -r ".[$i]"; return; }
    i=$((i + 1))
  done
  return 1
}

# ---- NACL isolation (restores on exit; `restore` state lives in the results dir)
# Replaces a subnet's NACL with an empty custom one (implicit deny all in and out).
nacl_isolate() { # subnet-id...
  local vpc new subnet assoc orig newassoc
  vpc=$(tofu -chdir="$AWS_INFRA/10-network" output -raw vpc_id)
  NACL_STATE="$RESULTS/nacl-restore-$(date -u +%Y%m%dT%H%M%SZ).state"
  new=$(aws ec2 create-network-acl --vpc-id "$vpc" --tag-specifications 'ResourceType=network-acl,Tags=[{Key=Name,Value=drill-isolate},{Key=project,Value=localarena}]' --query NetworkAcl.NetworkAclId --output text)
  NACL_NEW=$new
  cleanup_add "nacl_restore"
  for subnet in "$@"; do
    read -r assoc orig < <(aws ec2 describe-network-acls --filters "Name=association.subnet-id,Values=$subnet" \
      --query "NetworkAcls[0].Associations[?SubnetId=='$subnet'].[NetworkAclAssociationId,NetworkAclId]|[0]" --output text)
    echo "$subnet $orig" >>"$NACL_STATE"
    newassoc=$(aws ec2 replace-network-acl-association --association-id "$assoc" --network-acl-id "$new" --query NewAssociationId --output text)
    say "ISOLATED $subnet (was $orig, now $new, assoc $newassoc)"
  done
  say "if this shell dies: scripts/drills/nacl-restore.sh $NACL_STATE"
}
nacl_restore() {
  [[ -f ${NACL_STATE:-} ]] || return 0
  local subnet orig assoc
  while read -r subnet orig; do
    assoc=$(aws ec2 describe-network-acls --filters "Name=association.subnet-id,Values=$subnet" \
      --query "NetworkAcls[0].Associations[?SubnetId=='$subnet'].NetworkAclAssociationId|[0]" --output text)
    aws ec2 replace-network-acl-association --association-id "$assoc" --network-acl-id "$orig" >/dev/null &&
      say "RESTORED $subnet -> $orig"
  done <"$NACL_STATE"
  [[ -n ${NACL_NEW:-} ]] && aws ec2 delete-network-acl --network-acl-id "$NACL_NEW" 2>/dev/null || true
  rm -f "$NACL_STATE"
}

# ---- Kafka helpers (Strimzi in namespace data; admin user localarena-admin)
kafka_tools_up() {
  kk delete pod drill-kafka-tools --ignore-not-found --wait=true >/dev/null
  kk apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata: { name: drill-kafka-tools }
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: $KAFKA_IMAGE
      command: [sleep, "86400"]
      resources: { requests: { memory: 256Mi, cpu: 100m }, limits: { memory: 768Mi } }
      volumeMounts:
        - { name: ca, mountPath: /s/ca }
        - { name: ad, mountPath: /s/admin }
  volumes:
    - { name: ca, secret: { secretName: ${KAFKA}-cluster-ca-cert } }
    - { name: ad, secret: { secretName: ${KAFKA}-admin } }
YAML
  cleanup_add "kk delete pod drill-kafka-tools --ignore-not-found --wait=false"
  kk wait pod/drill-kafka-tools --for=condition=Ready --timeout=180s >/dev/null
  kafka_props admin admin /tmp/admin.properties
}
# kafka_props <secret-dir> <username-suffix> <outfile>
kafka_props() {
  kx "printf 'security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nssl.truststore.type=PEM\nssl.truststore.certificates=%s\nsasl.jaas.config=%s\n' \"\$(tr '\n' ' ' </s/ca/ca.crt | sed 's/ *\$//')\" \"org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"${KAFKA}-$2\\\" password=\\\"\$(cat /s/$1/password)\\\";\" > $3"
}
# kafka_props_pw <user> <password> <outfile>: SCRAM props for any user, trusting the cluster CA.
kafka_props_pw() {
  kx "printf 'security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nssl.truststore.type=PEM\nssl.truststore.certificates=%s\nsasl.jaas.config=%s\n' \"\$(tr '\n' ' ' </s/ca/ca.crt | sed 's/ *\$//')\" 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$1\" password=\"$2\";' > $3"
}
kx() { kk exec drill-kafka-tools -- bash -c "$1"; }
kcmd() { kx "bin/$1 --bootstrap-server $KAFKA_BS --command-config /tmp/admin.properties ${*:2}"; }
urp_count() {
  local out
  out=$(kcmd kafka-topics.sh --describe --under-replicated-partitions 2>/dev/null) || { echo n/a; return 0; }
  grep -c 'Partition:' <<<"$out" || true
}
urp_zero() { [[ $(urp_count) == 0 ]]; }
kafka_ready() { [[ $(kk get kafka "$KAFKA" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}') == True ]]; }
broker_node() { kk get pod "$1" -o jsonpath='{.spec.nodeName}'; }

load_topic() {
  kcmd kafka-topics.sh --create --if-not-exists --topic "$LOAD_TOPIC" --partitions 6 --replication-factor 3 \
    --config min.insync.replicas=2 >/dev/null
}
# load_start <records-per-second> <seconds>: acks=all idempotent producer, runs in the tools pod.
load_start() {
  load_topic
  kx "nohup bin/kafka-producer-perf-test.sh --topic $LOAD_TOPIC --num-records $(($1 * $2)) --record-size 200 --throughput $1 \
    --producer.config /tmp/admin.properties --producer-props acks=all enable.idempotence=true delivery.timeout.ms=60000 \
    > /tmp/load.out 2>&1 &" || true
  say "load started: $1 rec/s for ${2}s (acks=all, minISR=2, idempotent)"
}
load_wait() { wait_until "${1:-900}" "load finished" bash -c "! kubectl --context $KUBE_CONTEXT -n $KAFKA_NS exec drill-kafka-tools -- pgrep -f ProducerPerformance"; }
load_report() {
  say "load result (last lines of perf-test):"
  kx "tail -n 4 /tmp/load.out" | sed 's/^/    /'
  kx "grep -ci 'error\|exception\|expired' /tmp/load.out || true" | sed 's/^/    error lines: /'
}
