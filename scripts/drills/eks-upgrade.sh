#!/usr/bin/env bash
# Drill 014: eks-upgrade
# Proves: the full one-minor EKS upgrade sequence works on this platform and how long each phase
#         takes: control plane -> add-ons -> node groups one at a time (kafka last), while an API
#         probe and an optional Kafka load run.
# Expected: control plane 1.36 -> 1.37 in roughly 8 to 15 min with no API probe failures
#           (EKS keeps the API available); add-ons ACTIVE; each node group rolls with
#           max_unavailable=1; Kafka load finishes without errors. Timestamps for every phase.
# Env: TARGET_VERSION (default 1.37), WITH_KAFKA_LOAD=1 to run load, SKIP_NODEGROUPS=1.
# After: set kubernetes_version = "$TARGET_VERSION" in 20-eks tfvars and run `tofu plan`
#        (expect no changes) so state matches.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=014-eks-upgrade DRILL=eks-upgrade
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
TARGET=${TARGET_VERSION:-1.37}
cur=$(aws eks describe-cluster --name "$CLUSTER_NAME" --query cluster.version --output text)
say "cluster $CLUSTER_NAME: $cur -> $TARGET"
[[ $cur != "$TARGET" ]] || fail "already at $TARGET"

say "0. pre-checks: upgrade insights and deprecated API usage"
aws eks list-insights --cluster-name "$CLUSTER_NAME" --filter kubernetesVersions="$TARGET" \
  --query 'insights[].[name,insightStatus.status]' --output text | sed 's/^/    /' || say "list-insights unavailable"
k get --raw /metrics 2>/dev/null | grep -E '^apiserver_requested_deprecated_apis' | head -5 | sed 's/^/    deprecated: /' || true
k get pdb -A --no-headers | sed 's/^/    pdb: /'

probe_log="$RESULTS/eks-upgrade-probe-$(date -u +%H%M%S).log"
( while sleep 2; do
    if k get --raw /readyz >/dev/null 2>&1; then echo "$(ts) ok"; else echo "$(ts) FAIL"; fi
  done >"$probe_log" ) &
probe=$!
cleanup_add "kill $probe 2>/dev/null; say \"api probe: \$(grep -c ok $probe_log) ok, \$(grep -c FAIL $probe_log) fail ($probe_log)\""
if [[ ${WITH_KAFKA_LOAD:-0} == 1 ]]; then kafka_tools_up; load_start 100 3000; fi

wait_update() { # update-id [nodegroup]
  local id=$1 ng=${2:-} st
  while sleep 20; do
    st=$(aws eks describe-update --name "$CLUSTER_NAME" --update-id "$id" ${ng:+--nodegroup-name "$ng"} --query update.status --output text)
    say "update $id: $st"
    [[ $st == Successful ]] && return 0
    [[ $st == Failed || $st == Cancelled ]] && fail "update $id $st"
  done
}

say "1. control plane"
t=$SECONDS
uid=$(aws eks update-cluster-version --name "$CLUSTER_NAME" --kubernetes-version "$TARGET" --query update.id --output text)
wait_update "$uid"
say "T_control_plane=$((SECONDS - t))s"

say "2. add-ons"
t=$SECONDS
for addon in vpc-cni kube-proxy coredns eks-pod-identity-agent aws-ebs-csi-driver; do
  ver=$(aws eks describe-addon-versions --kubernetes-version "$TARGET" --addon-name "$addon" \
    --query 'addons[0].addonVersions[?compatibilities[?defaultVersion==`true`]].addonVersion|[0]' --output text)
  say "$addon -> $ver"
  aws eks update-addon --cluster-name "$CLUSTER_NAME" --addon-name "$addon" --addon-version "$ver" --resolve-conflicts OVERWRITE >/dev/null
  aws eks wait addon-active --cluster-name "$CLUSTER_NAME" --addon-name "$addon"
done
say "T_addons=$((SECONDS - t))s"

if [[ ${SKIP_NODEGROUPS:-0} != 1 ]]; then
  say "3. node groups, one at a time (general first, kafka last)"
  mapfile -t ngs < <(aws eks list-nodegroups --cluster-name "$CLUSTER_NAME" --query 'nodegroups[]' --output text | tr '\t' '\n' | sort -t- -k2,2r)
  for ng in "${ngs[@]}"; do
    t=$SECONDS
    say "node group $ng"
    uid=$(aws eks update-nodegroup-version --cluster-name "$CLUSTER_NAME" --nodegroup-name "$ng" --query update.id --output text)
    wait_update "$uid" "$ng"
    say "T_nodegroup[$ng]=$((SECONDS - t))s"
    [[ $ng == *kafka* ]] && wait_until 900 "kafka Ready" kafka_ready
  done
fi
k get nodes -o wide
[[ ${WITH_KAFKA_LOAD:-0} == 1 ]] && { load_wait 3000; load_report; }
say "done. Now set kubernetes_version=$TARGET in 20-eks and run tofu plan (expect no diff)."
