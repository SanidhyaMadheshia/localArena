#!/usr/bin/env bash
# Drill 015a: msk-provision
# Proves: the same producer workload runs against Amazon MSK (SASL/SCRAM over TLS, provisioned,
#         3 brokers) and records create time and produce latency to compare with Strimzi.
# Expected: tofu apply of infra/aws/30-msk finishes in about 25 to 40 min; the pod-side
#           produce test completes with acks=all; latency/throughput lines are saved.
# Cost: MSK bills hourly. Destroy afterwards with scripts/drills/msk-delete.sh.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=015-msk DRILL=msk-provision
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
t0=$SECONDS
tofu -chdir="$AWS_INFRA/30-msk" init -input=false >/dev/null
tofu -chdir="$AWS_INFRA/30-msk" apply -input=false -auto-approve
say "T_msk_create=$((SECONDS - t0))s"
bs=$(tofu -chdir="$AWS_INFRA/30-msk" output -raw bootstrap_brokers_sasl_scram)
secret=$(tofu -chdir="$AWS_INFRA/30-msk" output -raw scram_secret_arn)
creds=$(aws secretsmanager get-secret-value --secret-id "$secret" --query SecretString --output text)
user=$(jq -r .username <<<"$creds")
pass=$(jq -r .password <<<"$creds")
say "bootstrap: $bs user: $user"
POD=drill-msk-client
k -n "$KAFKA_NS" delete pod $POD --ignore-not-found --wait=true >/dev/null
k -n "$KAFKA_NS" run $POD --image="$KAFKA_IMAGE" --restart=Never --command -- sleep 3600 >/dev/null
cleanup_add "k -n $KAFKA_NS delete pod $POD --ignore-not-found --wait=false"
k -n "$KAFKA_NS" wait pod/$POD --for=condition=Ready --timeout=180s >/dev/null
X() { k -n "$KAFKA_NS" exec $POD -- bash -c "$1"; }
# MSK brokers use Amazon-trusted certificates: no custom CA needed.
X "printf 'security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$user\" password=\"$pass\";\n' > /tmp/msk.properties"
X "bin/kafka-topics.sh --bootstrap-server $bs --command-config /tmp/msk.properties --create --if-not-exists --topic drill.load --partitions 6 --replication-factor 3 --config min.insync.replicas=2"
X "bin/kafka-producer-perf-test.sh --topic drill.load --num-records 30000 --record-size 200 --throughput 100 --producer.config /tmp/msk.properties --producer-props acks=all enable.idempotence=true" | tail -3 | tee "$RESULTS/msk-perf.txt"
say "compare with the Strimzi run (kafka-broker-pod-delete baseline) in experiments/011-kafka-resilience/results"
