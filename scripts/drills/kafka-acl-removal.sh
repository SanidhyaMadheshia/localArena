#!/usr/bin/env bash
# Drill 012b: kafka-acl-removal
# Proves: removing ACLs from a KafkaUser takes effect without restarting brokers or the client:
#         a user that could write is denied after the User Operator reconciles.
# Expected: produce OK before; after patching acls to [], produce fails with
#           TopicAuthorizationException within about 1 to 2 min (T_denied printed).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=012-kafka-security DRILL=kafka-acl-removal
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
kafka_tools_up
load_topic
kk apply -f - <<YAML >/dev/null
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata: { name: drill-acl, labels: { strimzi.io/cluster: $KAFKA } }
spec:
  authentication: { type: scram-sha-512 }
  authorization:
    type: simple
    acls:
      - { resource: { type: topic, name: $LOAD_TOPIC, patternType: literal }, operations: [Write, Describe] }
YAML
cleanup_add "kk delete kafkauser drill-acl --ignore-not-found --wait=false"
wait_until 180 "KafkaUser Ready" kk wait kafkauser/drill-acl --for=condition=Ready --timeout=5s
pw=$(kk get secret drill-acl -o jsonpath='{.data.password}' | base64 -d)
kafka_props_pw drill-acl "$pw" /tmp/acl.properties
produce() { kx "echo drill-\$(date +%s) | bin/kafka-console-producer.sh --bootstrap-server $KAFKA_BS --producer.config /tmp/acl.properties --topic $LOAD_TOPIC 2>&1"; }
produce | grep -q TopicAuthorizationException && fail "denied before ACL removal"
say "produce with ACL: OK"
t0=$SECONDS
kk patch kafkauser drill-acl --type merge -p '{"spec":{"authorization":{"acls":[]}}}' >/dev/null
say "ACLs removed at T0"
denied() { produce | grep -q TopicAuthorizationException; }
wait_until 600 "produce denied" denied
say "T_denied=$((SECONDS - t0))s"
say "RESULT: PASS"
