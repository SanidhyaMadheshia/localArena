#!/usr/bin/env bash
# Drill 015b: msk-delete
# Proves: the optional MSK stack tears down cleanly (cluster, SCRAM secret, KMS key scheduled
#         for deletion) so it stops billing.
# Expected: tofu destroy of infra/aws/30-msk completes (10 to 20 min); no MSK cluster remains.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=015-msk DRILL=msk-delete
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
t0=$SECONDS
tofu -chdir="$AWS_INFRA/30-msk" init -input=false >/dev/null
tofu -chdir="$AWS_INFRA/30-msk" destroy -input=false -auto-approve
say "T_msk_delete=$((SECONDS - t0))s"
left=$(aws kafka list-clusters-v2 --query "ClusterInfoList[?starts_with(ClusterName, '${CLUSTER_NAME}-msk')].ClusterName" --output text)
[[ -z $left ]] && say "RESULT: CLEAN" || fail "MSK cluster still listed: $left"
k -n "$KAFKA_NS" delete pod drill-msk-client --ignore-not-found --wait=false >/dev/null 2>&1 || true
