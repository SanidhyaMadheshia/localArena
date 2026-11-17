#!/usr/bin/env bash
# Drill 011a: kafka-broker-pod-delete
# Proves: losing one broker pod (not the node) under acks=all load loses no data and the
#         cluster returns to 0 under-replicated partitions on its own.
# Expected: producer finishes with no errors (a latency spike is normal); pod Ready in about
#           1 to 2 min; URP back to 0 shortly after. Env: BROKER (default ${KAFKA}-dual-role-0).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-broker-pod-delete
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
BROKER=${BROKER:-${KAFKA}-dual-role-0}
kafka_tools_up
load_start 100 180
sleep 30
t0=$SECONDS
say "deleting pod $BROKER"
kk delete pod "$BROKER" --wait=false >/dev/null
sleep 5
wait_until 600 "pod $BROKER Ready" kk wait pod/"$BROKER" --for=condition=Ready --timeout=5s
say "T_pod_ready=$((SECONDS - t0))s"
wait_until 600 "URP == 0" urp_zero
say "T_urp_zero=$((SECONDS - t0))s"
load_wait 400
load_report
