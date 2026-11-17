#!/usr/bin/env bash
# Drill 009a: node-per-az
# Proves: the node group per AZ pattern puts exactly NODES_PER_AZ node(s) of each workload
#         (general, kafka) in each of 3 zones, with the kafka taint in place.
# Expected: every (workload, zone) cell shows NODES_PER_AZ Ready nodes; kafka nodes carry
#           taint dedicated=kafka:NoSchedule. Read-only.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=009-eks-foundation DRILL=node-per-az
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
want=${NODES_PER_AZ:-1}
k get nodes -L topology.kubernetes.io/zone,workload,node.kubernetes.io/instance-type
bad=0
for w in general kafka; do
  for z in $(k get nodes -o jsonpath='{range .items[*]}{.metadata.labels.topology\.kubernetes\.io/zone}{"\n"}{end}' | sort -u); do
    n=$(k get nodes -l "workload=$w,topology.kubernetes.io/zone=$z" --no-headers | awk '$2=="Ready"' | wc -l)
    say "workload=$w zone=$z ready=$n (want $want)"
    [[ $n == "$want" ]] || bad=1
  done
done
taints=$(k get nodes -l workload=kafka -o jsonpath='{range .items[*]}{.spec.taints[*].key}={.spec.taints[*].value}:{.spec.taints[*].effect}{"\n"}{end}' | sort -u)
say "kafka taints: $taints"
[[ $taints == "dedicated=kafka:NoSchedule" ]] || bad=1
((bad == 0)) && say "RESULT: PASS" || { say "RESULT: FAIL"; exit 1; }
