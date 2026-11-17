#!/usr/bin/env bash
# End-to-end check of the Strimzi cluster: topics, rack awareness, SCRAM+TLS produce/consume,
# and ACL denial for a user with no ACLs. Run inside the kind cluster (context kind-localarena-3z).
set -euo pipefail
cd "$(dirname "$0")/.."
export DOCKER_CONTEXT=default
K=(kubectl --context "${KUBECTL_CONTEXT:-kind-localarena-3z}" -n data)
IMG=${KAFKA_IMAGE:-quay.io/strimzi/kafka:1.3.1-kafka-4.3.1}
BS=localarena-kafka-bootstrap.data.svc:9093
TOPIC=game.match-events.v1

echo "== topics Ready"; "${K[@]}" get kafkatopics.kafka.strimzi.io
echo "== brokers per node"; "${K[@]}" get pods -l strimzi.io/kind=Kafka,strimzi.io/name=localarena-kafka -o wide
"${K[@]}" apply -f infra/kafka/verify/noacl-user.yaml >/dev/null
"${K[@]}" wait kafkauser/localarena-noacl --for=condition=Ready --timeout=120s

# One throwaway pod, props files built from the mounted secrets.
"${K[@]}" delete pod kafka-verify --ignore-not-found >/dev/null
"${K[@]}" apply -f - <<YAML >/dev/null
apiVersion: v1
kind: Pod
metadata: { name: kafka-verify }
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: $IMG
      command: [sleep, "900"]
      resources: { requests: { memory: 256Mi, cpu: 50m }, limits: { memory: 512Mi } }
      volumeMounts:
        - { name: ca, mountPath: /s/ca }
        - { name: gs, mountPath: /s/gameserver }
        - { name: ec, mountPath: /s/eventcat }
        - { name: na, mountPath: /s/noacl }
        - { name: ad, mountPath: /s/admin }
  volumes:
    - { name: ca, secret: { secretName: localarena-cluster-ca-cert } }
    - { name: gs, secret: { secretName: localarena-gameserver } }
    - { name: ec, secret: { secretName: localarena-eventcat } }
    - { name: na, secret: { secretName: localarena-noacl } }
    - { name: ad, secret: { secretName: localarena-admin } }
YAML
"${K[@]}" wait pod/kafka-verify --for=condition=Ready --timeout=180s
trap '"${K[@]}" delete pod kafka-verify --ignore-not-found --wait=false >/dev/null' EXIT

X() { "${K[@]}" exec kafka-verify -- bash -c "$1"; }
X '
for u in gameserver eventcat noacl admin; do
  printf "security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nssl.truststore.type=PEM\nssl.truststore.certificates=%s\nsasl.jaas.config=%s\n" \
    "$(tr "\n" " " </s/ca/ca.crt | sed "s/ *$//")" \
    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"localarena-$u\" password=\"$(cat /s/$u/password)\";" > /tmp/$u.properties
done'

echo "== rack awareness: describe $TOPIC"
X "bin/kafka-topics.sh --bootstrap-server $BS --command-config /tmp/admin.properties --describe --topic $TOPIC"
echo "== broker.rack"
X "bin/kafka-configs.sh --bootstrap-server $BS --command-config /tmp/admin.properties --describe --entity-type brokers --all 2>/dev/null | grep -E '^(Dynamic|All) configs for broker|broker.rack' | head -12" || true

echo "== produce as localarena-gameserver (allowed)"
X "echo verify-\$(date +%s) | bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config /tmp/gameserver.properties --topic $TOPIC && echo PRODUCE_OK"
echo "== consume as localarena-eventcat (allowed, no group)"
X "bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config /tmp/eventcat.properties --topic $TOPIC --partition 0 --offset earliest --max-messages 1 --timeout-ms 20000 --consumer-property enable.auto.commit=false 2>&1 | tail -3"
echo "== localarena-noacl produce (must be denied)"
out=$(X "echo x | bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config /tmp/noacl.properties --topic $TOPIC" 2>&1 || true); echo "$out" | tail -3
if grep -q TopicAuthorizationException <<<"$out"; then echo DENIED_OK; else echo "FAIL: noacl was not denied"; exit 1; fi
echo "== gameserver consume from a topic it cannot read (must be denied)"
out=$(X "bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config /tmp/gameserver.properties --topic $TOPIC --partition 0 --offset earliest --max-messages 1 --timeout-ms 15000 --consumer-property enable.auto.commit=false" 2>&1 || true); echo "$out" | tail -3
if grep -q 'Authorization' <<<"$out"; then echo DENIED_OK; else echo "FAIL: gameserver read was not denied"; exit 1; fi
