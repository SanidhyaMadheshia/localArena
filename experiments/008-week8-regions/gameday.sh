#!/usr/bin/env bash
# Final game day (Week 8). Sustained load: 100 bots playing (30 s rounds, account
# players -> rewards saga) + 80 req/s API. Then THREE faults from a pool of six
# are injected at staggered times (t+60 s, +150 s, +240 s). The plan is sealed
# in results/gameday-answer.sealed. Responders use alerts/metrics/traces/logs only.
#   experiments/008-week8-regions/gameday.sh start | stop
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/008-week8-regions/results; mkdir -p "$OUT"
DC=(docker compose --profile streaming --profile observability --profile security)
P=(docker compose exec -T postgres psql -U localarena -d localarena -qtA)
fault() { case $1 in
  kill-broker)      docker kill localarena-kafka-2-1 >/dev/null ;;               # under-replicated partitions
  stop-streams)     docker stop localarena-streams-1-1 localarena-streams-2-1 >/dev/null ;; # leaderboard stale, lag grows
  db-latency)       docker update --cpus 0.05 localarena-postgres-1 >/dev/null ;;   # starved (slow) primary
  bad-policy)       curl -s -X PUT localhost:8181/v1/policies/localarena --data-binary 'package localarena.authz
default allow := false' >/dev/null ;;                                               # bad OPA deploy: every decision undefined
  kill-gameserver)  docker kill localarena-gameserver-1 >/dev/null ;;               # restart: unless-stopped brings it back
  rewards-down)     docker stop localarena-rewards-1 >/dev/null ;;                  # rewards lag, credits not granted
esac; }
case ${1:-start} in
start)
  nohup bin/bots -server http://localhost:8080 -bots 100 -accounts 2 -ramp 5s -duration 12m -churn 20s -json > "$OUT/gameday-bots.json" 2>/dev/null &
  echo $! > /tmp/gd-bots.pid
  nohup k6 run -q --no-color --log-output=none -e MIN_ID=2 -e MAX_ID=1001 -e RATE=80 -e DURATION=12m load-tests/api-mixed.js >/dev/null 2>&1 &
  echo $! > /tmp/gd-k6.pid
  mapfile -t plan < <(printf '%s\n' kill-broker stop-streams db-latency bad-policy kill-gameserver rewards-down | shuf -n 3 --random-source=<(openssl rand 64))
  printf '%s\n' "t+60s ${plan[0]}" "t+150s ${plan[1]}" "t+240s ${plan[2]}" "started $(date -u +%FT%TZ)" | base64 > "$OUT/gameday-answer.sealed"
  ( sleep 60; fault "${plan[0]}"; sleep 90; fault "${plan[1]}"; sleep 90; fault "${plan[2]}" ) >/dev/null 2>&1 &
  echo $! > /tmp/gd-faults.pid
  echo "game day started $(date -u +%T); 3 hidden faults at +60/+150/+240 s" ;;
stop)
  kill "$(cat /tmp/gd-bots.pid)" "$(cat /tmp/gd-k6.pid)" "$(cat /tmp/gd-faults.pid)" 2>/dev/null || true
  echo "load stopped" ;;
esac
