#!/usr/bin/env bash
# Experiment 003 runner (Compose stack must be up: docker compose up -d).
# Usage: experiments/003-week3-resilience/run.sh [scenario ...]   (default: all)
# Each scenario runs k6 through Envoy while injecting exactly one fault, then
# restores the stack. Results: results/<scenario>.json (+ .envoy.txt counters).
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/003-week3-resilience/results
mkdir -p "$OUT"
read -r MIN MAX < <(docker compose exec -T postgres psql -U localarena -d localarena -qtA -v ON_ERROR_STOP=1 -f - < experiments/003-week3-resilience/seed.sql | tail -1)
K6=(k6 run -q --no-color --log-output=none -e MIN_ID="$MIN" -e MAX_ID="$MAX")

envoy_stats() { # snapshot of the counters that explain each run
  curl -s localhost:9901/stats | grep -E "^cluster\.api_(rr|lr)\.(upstream_rq_retry|upstream_rq_retry_overflow|upstream_rq_retry_success|upstream_rq_timeout|upstream_rq_per_try_timeout|outlier_detection\.ejections_enforced_total|health_check\.failure|upstream_rq_pending_overflow|upstream_cx_connect_fail|upstream_rq_5xx|upstream_rq_2xx):" || true
}
delta() { # before-file after-file -> non-zero deltas
  join <(sort "$1") <(sort "$2") | awk '{d=$3-$2; if (d!=0) print $1, d}'
}
reset() {
  docker compose unpause api >/dev/null 2>&1 || true
  docker compose up -d --wait api valkey envoy >/dev/null 2>&1
  for c in $(docker compose ps -q api); do docker update --cpus 1 "$c" >/dev/null; done
  docker compose exec -T valkey valkey-cli flushall >/dev/null
  sleep 3
}
run() { # name, k6 script, k6 args..., then '--', fault command (runs in background after 5 s)
  local name=$1 script=$2; shift 2
  local args=() fault=()
  while [ $# -gt 0 ] && [ "$1" != -- ]; do args+=("$1"); shift; done
  [ $# -gt 0 ] && { shift; fault=("$@"); }
  echo "== $name"
  reset
  envoy_stats > /tmp/la-before
  if [ ${#fault[@]} -gt 0 ]; then (sleep 5; "${fault[@]}") & FP=$!; fi
  "${K6[@]}" "${args[@]}" -e SUMMARY="$OUT/$name.json" "$script" | grep '^{' | jq -c '{requests,failed_rate,status_5xx,rate_limited,cache_hits,p50,p95,p99,max,mode,goodput_per_s,amplification,logical_success_rate,logical_p99_ms}' || true
  [ ${#fault[@]} -gt 0 ] && wait $FP
  envoy_stats > /tmp/la-after
  delta /tmp/la-before /tmp/la-after | tee "$OUT/$name.envoy.txt"
}

# Executions of the profile query, from pg_stat_statements (counts DB loads behind cache misses).
pg_stat_profile() {
  docker compose exec -T postgres psql -U localarena -d localarena -qtA -c \
    "SELECT coalesce(sum(calls),0) FROM pg_stat_statements WHERE query LIKE 'SELECT p.id, p.username, pr.display_name%'" | tr -d ' '
}
api2=localarena-api-2
kill_replica()  { docker kill "$api2" >/dev/null; echo "  killed $api2"; }
pause_replica() { docker pause "$api2" >/dev/null; echo "  paused $api2"; sleep 15; docker unpause "$api2" >/dev/null; echo "  unpaused"; }
graceful_stop() { docker stop -t 20 "$api2" >/dev/null; echo "  stopped $api2 gracefully"; }
cpu_throttle()  { docker update --cpus 0.1 "$api2" >/dev/null; echo "  throttled $api2 to 0.1 CPU"; }
valkey_down()   { docker compose stop valkey >/dev/null; echo "  valkey stopped"; sleep 10; docker compose start valkey >/dev/null; echo "  valkey started"; }
history_slow()  { # make history ~10x slower: drop the index for 15 s (CONCURRENTLY: no write lock)
  docker compose exec -T postgres psql -U localarena -d localarena -qc 'DROP INDEX CONCURRENTLY IF EXISTS match_players_player_started_idx' >/dev/null
  echo "  history index dropped"; sleep 15
  docker compose exec -T -e PGOPTIONS='-c statement_timeout=0' postgres psql -U localarena -d localarena -v ON_ERROR_STOP=1 -qc 'CREATE INDEX CONCURRENTLY IF NOT EXISTS match_players_player_started_idx ON match_players (player_id, started_at DESC)'
  echo "  index restored"
}
db_slow() { # brownout: history index gone for 20 s -> each history query seq-scans 200k rows
  docker compose exec -T postgres psql -U localarena -d localarena -qc 'DROP INDEX CONCURRENTLY IF EXISTS match_players_player_started_idx' >/dev/null
  echo "  history index dropped (db brownout)"; sleep 20
  docker compose exec -T -e PGOPTIONS='-c statement_timeout=0' postgres psql -U localarena -d localarena -v ON_ERROR_STOP=1 -qc 'CREATE INDEX CONCURRENTLY IF NOT EXISTS match_players_player_started_idx ON match_players (player_id, started_at DESC)'
  echo "  index restored"
}

M=load-tests/api-mixed.js; R=load-tests/retry-storm.js
LR=http://localhost:10000 RR=http://localhost:10001   # :10000 is least request (default), :10001 round robin
export BASE=$RR # scenarios without an explicit BASE use round robin, as when measured
all=(baseline-rr baseline-lr kill-replica pause-replica-rr pause-replica-lr graceful-stop cpu-throttle-rr cpu-throttle-lr valkey-outage stampede-sf stampede-nosf retry-naive retry-backoff)
want=("${@:-${all[@]}}")
for s in "${want[@]}"; do case $s in
  baseline-rr)      run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR ;;
  baseline-lr)      run $s $M -e RATE=300 -e DURATION=30s -e BASE=$LR ;;
  kill-replica)     run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR -- kill_replica ;;
  pause-replica-rr) run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR -- pause_replica ;;
  pause-replica-lr) run $s $M -e RATE=300 -e DURATION=30s -e BASE=$LR -- pause_replica ;;
  graceful-stop)    run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR -- graceful_stop ;;
  cpu-throttle-rr)  run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR -- cpu_throttle ;;
  cpu-throttle-lr)  run $s $M -e RATE=300 -e DURATION=30s -e BASE=$LR -- cpu_throttle ;;
  valkey-outage)    run $s $M -e RATE=300 -e DURATION=30s -e BASE=$RR -- valkey_down ;;
  # Hot key with a 1 s TTL under 1000 rps: the key expires ~20 times, each a stampede chance.
  stampede-sf|stampede-nosf)
    sf=1; [ $s = stampede-nosf ] && sf=0
    # PROFILE_LOAD_DELAY makes each cache fill cost 30 ms, like a slow DB query,
    # so concurrent misses overlap (at ~1 ms per fill there is nothing to collapse).
    # Exported so reset's `compose up` keeps them (otherwise it recreates api with defaults).
    export CACHE_SINGLEFLIGHT=$sf PROFILE_TTL=1s PROFILE_LOAD_DELAY=30ms
    pg0=$(pg_stat_profile)
    run $s $M -e RATE=1000 -e DURATION=20s -e HOT="$MIN" -e BASE=$RR
    echo "  profile loads on postgres: $(( $(pg_stat_profile) - pg0 ))" | tee -a "$OUT/$s.envoy.txt"
    unset CACHE_SINGLEFLIGHT PROFILE_TTL PROFILE_LOAD_DELAY ;;
  retry-naive)      run $s $R -e MODE=naive   -e RATE=150 -e DURATION=30s -e BASE=$RR -- db_slow ;;
  retry-backoff)    run $s $R -e MODE=backoff -e RATE=150 -e DURATION=30s -e BASE=$RR -- db_slow ;;
  *) echo "unknown scenario $s" >&2; exit 2 ;;
esac; done
reset
docker compose exec -T postgres psql -U localarena -d localarena -qtA -c "SELECT 'history index present: ' || (count(*) = 1) FROM pg_indexes WHERE indexname = 'match_players_player_started_idx'"
echo "results in $OUT"
