#!/usr/bin/env bash
# Drill 011h: kafka-crossaz-flowlogs
# Proves: how much Kafka traffic crosses AZ boundaries (cross-AZ transfer is billed per GB),
#         measured from VPC flow logs, counted once at the sending ENI.
# Expected: with acks=all and RF=3 spread over 3 AZs, most replication and produce bytes are
#           cross-AZ (~2/3 or more). Needs 10-network applied with enable_flow_logs=true.
#           Flow logs land in S3 about 5 to 10 min after the traffic; the script waits.
# Env: LOAD_SECONDS (default 300), LOG_WAIT (default 720).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=011-kafka-resilience DRILL=kafka-crossaz-flowlogs
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
bucket=$(tofu -chdir="$AWS_INFRA/10-network" output -raw flow_logs_bucket 2>/dev/null || true)
[[ -n $bucket && $bucket != null ]] || fail "flow logs bucket missing: apply 10-network with -var enable_flow_logs=true"
subnets=$(tofu -chdir="$AWS_INFRA/10-network" output -json private_subnet_ids | jq -r 'join(",")')
LOAD_SECONDS=${LOAD_SECONDS:-300}
kafka_tools_up
start=$(date -u +%s)
load_start 200 "$LOAD_SECONDS"
load_wait $((LOAD_SECONDS + 300))
load_report
say "waiting ${LOG_WAIT:-720}s for flow log delivery to s3://$bucket"
sleep "${LOG_WAIT:-720}"
work=$(mktemp -d)
cleanup_add "rm -rf $work"
aws s3 sync "s3://$bucket/AWSLogs/" "$work" --only-show-errors
say "analysing $(find "$work" -name '*.log.gz' | wc -l) files"
# fields: 3 srcaddr 4 dstaddr 5 srcport 6 dstport 8 bytes 9 start 11 action 13 pkt-srcaddr 14 pkt-dstaddr 15 subnet-id
find "$work" -name '*.log.gz' -print0 | xargs -0 zcat | awk -v subnets="$subnets" -v start="$start" '
  BEGIN { n = split(subnets, S, ","); for (i = 1; i <= n; i++) idx[S[i]] = i - 1 }
  function az(ip,   o) { split(ip, o, "."); return (o[1] == 10 && o[2] == 40) ? int(o[3] / 16) : -1 }
  $1 == "version" || $11 != "ACCEPT" || $9 < start { next }
  ($5 == 9093 || $6 == 9093 || $5 == 9091 || $6 == 9091) {
    s = az($13); d = az($14)
    if (s < 0 || d < 0 || idx[$15] != s) next        # count once, at the sender
    if (s == d) same += $8; else cross += $8
  }
  END { printf "same-AZ bytes: %d\ncross-AZ bytes: %d\ncross-AZ share: %.1f%%\n", same, cross, (same + cross) ? 100 * cross / (same + cross) : 0 }' | tee "$RESULTS/crossaz-summary-$(date -u +%H%M%S).txt"
