#!/usr/bin/env bash
# Hidden-incident drill. Starts 100 rps of mixed load, then after 60 s injects
# ONE fault chosen at random and writes the answer, base64-sealed, to
# results/incident-answer.sealed. The responder diagnoses from alerts, metrics,
# traces and logs only, then: base64 -d results/incident-answer.sealed
#   experiments/006-week6-observability/incident.sh start   (returns immediately)
#   experiments/006-week6-observability/incident.sh stop    (ends load + fault)
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/006-week6-observability/results
P=(docker compose exec -T postgres psql -U localarena -d localarena -qtA)
case ${1:-start} in
start)
  nohup k6 run -q --no-color --log-output=none -e MIN_ID=2 -e MAX_ID=1001 -e RATE=100 -e DURATION=15m load-tests/api-mixed.js >/dev/null 2>&1 &
  echo $! > /tmp/incident-k6.pid
  faults=(lock-profiles pgbouncer-pause drop-history-index)
  f=${faults[$(( $(od -An -N2 -tu2 /dev/urandom) % ${#faults[@]} ))]}
  echo "$f injected at $(date -u +%FT%TZ)" | base64 > "$OUT/incident-answer.sealed"
  (
    sleep 60
    case $f in
      lock-profiles) # a "migration" grabs ACCESS EXCLUSIVE on profiles and forgets to commit
        "${P[@]}" -c "SET statement_timeout=0; BEGIN; LOCK TABLE profiles IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(600);" >/dev/null 2>&1 ;;
      pgbouncer-pause)
        docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD:-localarena-dev}" pgbouncer psql -h localhost -p 5432 -U localarena pgbouncer -c "PAUSE localarena" >/dev/null 2>&1; sleep 600 ;;
      drop-history-index)
        "${P[@]}" -c "DROP INDEX CONCURRENTLY IF EXISTS match_players_player_started_idx" >/dev/null; sleep 600 ;;
    esac
  ) >/dev/null 2>&1 &
  echo $! > /tmp/incident-fault.pid
  echo "load started; a fault will be injected in 60 s (answer sealed in $OUT/incident-answer.sealed)" ;;
stop)
  kill "$(cat /tmp/incident-k6.pid)" "$(cat /tmp/incident-fault.pid)" 2>/dev/null || true
  "${P[@]}" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE '%pg_sleep(600)%' AND pid <> pg_backend_pid()" >/dev/null
  docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD:-localarena-dev}" pgbouncer psql -h localhost -p 5432 -U localarena pgbouncer -c "RESUME localarena" >/dev/null 2>&1 || true
  "${P[@]}" -c "SET statement_timeout=0" -c "CREATE INDEX CONCURRENTLY IF NOT EXISTS match_players_player_started_idx ON match_players (player_id, started_at DESC)" >/dev/null
  echo "load stopped; all possible faults reverted" ;;
esac
