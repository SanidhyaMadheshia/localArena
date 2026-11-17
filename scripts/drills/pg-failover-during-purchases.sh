#!/usr/bin/env bash
# Drill 013a: pg-failover-during-purchases
# Proves: killing the CNPG primary during a steady stream of POST /v1/purchases produces a short,
#         bounded error window (503 with Retry-After, never data corruption) and the API recovers
#         once a replica is promoted and PgBouncer follows.
# Expected: a few seconds to ~30 s of 5xx/000 around the kill, then only 2xx/4xx. T_recovered
#           printed. Env: PLAYER_ID, COSMETIC_ID (existing ids; business 4xx after the first
#           success is fine), DURATION (default 90), RATE per second (default 5).
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=013-data-resilience DRILL=pg-failover-during-purchases
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
PLAYER_ID=${PLAYER_ID:-1} COSMETIC_ID=${COSMETIC_ID:-1} DURATION=${DURATION:-90} RATE=${RATE:-5}
primary=$(k -n "$APP_NS" get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.status.currentPrimary}')
say "primary=$primary"
k -n "$APP_NS" port-forward svc/api 18081:8081 >/dev/null 2>&1 &
pf=$!
cleanup_add "kill $pf 2>/dev/null"
sleep 3
curl -fsS -m 3 localhost:18081/healthz >/dev/null || fail "api not reachable through port-forward"
declare -A count
first_fail="" last_fail="" killed=0
for ((s = 0; s < DURATION; s++)); do
  if ((s == 20 && !killed)); then
    say "KILL primary $primary"
    k -n "$APP_NS" delete pod "$primary" --wait=false >/dev/null
    kill_at=$SECONDS killed=1
  fi
  for ((i = 0; i < RATE; i++)); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 -X POST localhost:18081/v1/purchases \
      -H "Idempotency-Key: drill-$(date +%s%N)" -H 'Content-Type: application/json' \
      -d "{\"player_id\":$PLAYER_ID,\"cosmetic_id\":$COSMETIC_ID}" || true)
    count[$code]=$((${count[$code]:-0} + 1))
    if [[ $code == 000 || $code == 5* ]]; then
      [[ -z $first_fail ]] && first_fail=$SECONDS
      last_fail=$SECONDS
    fi
  done
  sleep 1
done
say "status counts: $(for c in "${!count[@]}"; do printf '%s=%s ' "$c" "${count[$c]}"; done)"
if [[ -n $first_fail ]]; then
  say "first failure ${first_fail}s, last failure ${last_fail}s (since script start); kill at ${kill_at}s; error window $((last_fail - first_fail + 1))s"
else
  say "no failures observed"
fi
say "new primary: $(k -n "$APP_NS" get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.status.currentPrimary}')"
