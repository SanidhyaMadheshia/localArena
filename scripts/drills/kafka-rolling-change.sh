#!/usr/bin/env bash
# Drill 011f: kafka-rolling-change
# Proves: a static broker config change rolls brokers one at a time and producers see no
#         errors (RF=3, minISR=2, Strimzi waits for ISR before rolling the next broker).
# Expected: all 3 brokers restart sequentially; total roll about 5 to 10 min; the producer
#           finishes without errors. Env: CONFIG_KEY / CONFIG_VALUE (default a harmless static
#           key, num.recovery.threads.per.data.dir=2).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-rolling-change
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
KEY=${CONFIG_KEY:-num.recovery.threads.per.data.dir}
VAL=${CONFIG_VALUE:-2}
kafka_tools_up
load_start 100 900
sleep 20
t0=$SECONDS
kk patch kafka "$KAFKA" --type merge -p "{\"spec\":{\"kafka\":{\"config\":{\"$KEY\":$VAL}}}}"
say "patched $KEY=$VAL; watching restarts"
(
  prev=""
  while sleep 3; do
    cur=$(kk get pods -l "strimzi.io/cluster=$KAFKA,strimzi.io/kind=Kafka" --no-headers 2>/dev/null | awk '{printf "%s=%s ", $1, $2}')
    [[ $cur != "$prev" ]] && say "pods: $cur"
    prev=$cur
  done
) &
watcher=$!
cleanup_add "kill $watcher 2>/dev/null"
sleep 20
wait_until 1500 "kafka Ready after roll" kafka_ready
wait_until 600 "URP == 0" urp_zero
say "T_roll=$((SECONDS - t0))s"
load_wait 900
load_report
