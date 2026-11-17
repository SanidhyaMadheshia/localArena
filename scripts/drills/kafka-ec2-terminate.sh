#!/usr/bin/env bash
# Drill 011b: kafka-ec2-terminate
# Proves: losing the EC2 node under a broker (and its AZ-pinned volume's attachment) recovers:
#         the kafka node group launches a replacement in the same AZ, the EBS volume reattaches,
#         the broker rejoins and catches up. No acknowledged write is lost.
# Expected: recovery in roughly 4 to 8 min (node launch + volume attach + catch-up). Producer
#           keeps running through the outage thanks to minISR=2 of 3. Env: BROKER.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-ec2-terminate
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
BROKER=${BROKER:-${KAFKA}-dual-role-0}
kafka_tools_up
node=$(broker_node "$BROKER")
id=$(node_instance "$node")
say "broker=$BROKER node=$node zone=$(node_zone "$node") instance=$id"
load_start 100 600
sleep 30
t0=$SECONDS
aws ec2 terminate-instances --instance-ids "$id" >/dev/null
say "T0 instance terminated"
wait_until 900 "$BROKER Ready again (new node)" kk wait pod/"$BROKER" --for=condition=Ready --timeout=5s
new=$(broker_node "$BROKER")
say "T_broker_ready=$((SECONDS - t0))s on $new (zone $(node_zone "$new"))"
wait_until 900 "URP == 0" urp_zero
say "T_urp_zero=$((SECONDS - t0))s"
load_wait 900
load_report
