#!/usr/bin/env bash
# Drill 011c: kafka-az-isolate   (011d kafka-two-az-isolate sets AZ_COUNT=2)
# Proves: with brokers spread one per AZ, RF=3 and minISR=2, isolating ONE AZ's private subnet
#         (empty NACL = deny all in and out) leaves Kafka writable; isolating TWO AZs stops
#         writes (quorum and ISR lost) without data loss, and it recovers after restore.
# Expected (1 AZ): producer continues, URP > 0 during isolation, 0 after restore.
#          (2 AZ): producer stalls with timeouts, no acknowledged data lost, recovery after
#          restore. A trap restores the NACLs on exit; ISOLATE_SECONDS default 180.
# RISK: the isolated nodes also lose the EKS API and SSM. Everything in those subnets is cut off.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=${DRILL_NAME:-kafka-az-isolate}
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
AZ_COUNT=${AZ_COUNT:-1}
ISOLATE_SECONDS=${ISOLATE_SECONDS:-180}
mapfile -t azs < <(tofu -chdir="$AWS_INFRA/10-network" output -json azs | jq -r '.[]')
subnets=()
for ((i = 0; i < AZ_COUNT; i++)); do subnets+=("$(private_subnet_for_az "${azs[i]}")"); done
say "isolating AZ(s): ${azs[*]:0:AZ_COUNT} subnets: ${subnets[*]} for ${ISOLATE_SECONDS}s"
kafka_tools_up
load_start 100 $((ISOLATE_SECONDS + 300))
sleep 30
t0=$SECONDS
nacl_isolate "${subnets[@]}"
sleep "$ISOLATE_SECONDS"
say "URP during isolation (queried via bootstrap, may fail if quorum is lost): $(urp_count || echo n/a)"
k get nodes --no-headers | awk '{print $1, $2}' | sed 's/^/    /'
nacl_restore
say "T_restored=$((SECONDS - t0))s"
wait_until 900 "all nodes Ready" bash -c "! kubectl --context $KUBE_CONTEXT get nodes --no-headers | awk '\$2!=\"Ready\"' | grep -q ."
wait_until 900 "kafka Ready" kafka_ready
wait_until 900 "URP == 0" urp_zero
say "T_recovered=$((SECONDS - t0))s"
load_wait 600
load_report
