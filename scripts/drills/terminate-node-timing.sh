#!/usr/bin/env bash
# Drill 009c: terminate-node-timing
# Proves: how long the platform needs to notice a dead node and replace it (managed node group
#         + AZ-pinned group), and that pods reschedule.
# Expected: node NotReady within ~1 min, replacement Ready in about 3 to 6 min. Timings are
#           printed as T_notready and T_replaced. Terminates ONE general node in ZONE.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=009-eks-foundation DRILL=terminate-node-timing
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
WORKLOAD=${WORKLOAD:-general}
node=$(k get nodes -l "workload=$WORKLOAD${ZONE:+,topology.kubernetes.io/zone=$ZONE}" -o jsonpath='{.items[0].metadata.name}')
id=$(node_instance "$node")
before=$(ready_nodes "workload=$WORKLOAD")
say "victim=$node instance=$id zone=$(node_zone "$node") ready_before=$before"
t0=$SECONDS
aws ec2 terminate-instances --instance-ids "$id" >/dev/null
say "T0 terminate-instances issued"
wait_until 600 "node $node NotReady or gone" bash -c "! kubectl --context $KUBE_CONTEXT get node $node --no-headers 2>/dev/null | awk '\$2==\"Ready\"' | grep -q ."
say "T_notready=$((SECONDS - t0))s"
wait_until 1200 "ready general-class nodes back to $before" bash -c "[[ \$(kubectl --context $KUBE_CONTEXT get nodes -l workload=$WORKLOAD --no-headers | awk '\$2==\"Ready\"' | wc -l) -ge $before ]]"
say "T_replaced=$((SECONDS - t0))s"
k get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded | head -20
