#!/usr/bin/env bash
# Drill 011e: kafka-disk-expand
# Proves: broker volumes grow online (gp3 StorageClass allowVolumeExpansion + Strimzi) with no
#         broker restart loop and no data loss. Volumes can never shrink.
# Expected: all PVCs report NEW_SIZE within a few minutes; brokers stay Ready. Env: NEW_SIZE
#           (default 30Gi), POOL (default dual-role).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-disk-expand
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
NEW_SIZE=${NEW_SIZE:-30Gi}
POOL=${POOL:-dual-role}
kafka_tools_up
kk get pvc -l "strimzi.io/cluster=$KAFKA" -o custom-columns=NAME:.metadata.name,SIZE:.status.capacity.storage,SC:.spec.storageClassName
load_start 100 300
t0=$SECONDS
say "patching KafkaNodePool/$POOL volume 0 to $NEW_SIZE"
kk patch kafkanodepool "$POOL" --type json -p "[{\"op\":\"replace\",\"path\":\"/spec/storage/volumes/0/size\",\"value\":\"$NEW_SIZE\"}]"
want=${NEW_SIZE%Gi}
resized() { [[ $(kk get pvc -l "strimzi.io/cluster=$KAFKA" -o jsonpath='{range .items[*]}{.status.capacity.storage}{"\n"}{end}' | grep -c "^${want}Gi$") -ge 3 ]]; }
wait_until 900 "3 PVCs at $NEW_SIZE" resized
say "T_resized=$((SECONDS - t0))s"
kk get pods -l "strimzi.io/cluster=$KAFKA,strimzi.io/kind=Kafka" -o custom-columns=NAME:.metadata.name,RESTARTS:.status.containerStatuses[0].restartCount
wait_until 600 "kafka Ready" kafka_ready
load_wait 600
load_report
