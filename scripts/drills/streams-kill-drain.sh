#!/usr/bin/env bash
# Drill 013b: streams-kill-drain
# Proves: the Kafka Streams app (EXACTLY_ONCE_V2) survives (1) a killed pod and (2) a node drain
#         (PDB-respecting eviction): consumer lag spikes then drains, no crash loop.
# Expected: after each fault the pod is Running again in ~1 to 3 min and group lag returns to
#           ~0. Env: STREAMS_NS (localarena), STREAMS_DEPLOY (streams), STREAMS_SELECTOR
#           (app=streams). Adjust if the chart names differ.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=013-data-resilience DRILL=streams-kill-drain
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
NS=${STREAMS_NS:-$APP_NS} DEP=${STREAMS_DEPLOY:-streams} SEL=${STREAMS_SELECTOR:-app=streams}
kafka_tools_up
lag() { kcmd kafka-consumer-groups.sh --describe --group "${STREAMS_GROUP:-localarena-streams}" 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'; }
lag_small() { [[ $(lag) -le ${LAG_OK:-50} ]]; }
say "baseline lag=$(lag) pods: $(k -n "$NS" get pods -l "$SEL" --no-headers | awk '{print $1"="$3}' | tr '\n' ' ')"

say "FAULT 1: kill one streams pod"
t0=$SECONDS
pod=$(k -n "$NS" get pods -l "$SEL" -o jsonpath='{.items[0].metadata.name}')
k -n "$NS" delete pod "$pod" --force --grace-period=0 >/dev/null 2>&1
wait_until 600 "deployment available" k -n "$NS" rollout status "deploy/$DEP" --timeout=5s
wait_until 600 "lag <= ${LAG_OK:-50}" lag_small
say "T_kill_recovered=$((SECONDS - t0))s lag=$(lag)"

say "FAULT 2: drain the node hosting a streams pod"
node=$(k -n "$NS" get pods -l "$SEL" -o jsonpath='{.items[0].spec.nodeName}')
cleanup_add "k uncordon $node"
t0=$SECONDS
k drain "$node" --ignore-daemonsets --delete-emptydir-data --timeout=300s || say "drain reported errors (PDB or pinned pods); continuing"
wait_until 600 "deployment available" k -n "$NS" rollout status "deploy/$DEP" --timeout=5s
wait_until 600 "lag <= ${LAG_OK:-50}" lag_small
say "T_drain_recovered=$((SECONDS - t0))s lag=$(lag)"
say "restarts: $(k -n "$NS" get pods -l "$SEL" --no-headers | awk '{print $1"="$4}' | tr '\n' ' ')"
