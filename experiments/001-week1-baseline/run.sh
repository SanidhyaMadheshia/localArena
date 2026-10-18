#!/usr/bin/env bash
# Experiment 001 runner. Usage: experiments/001-week1-baseline/run.sh [outdir]
# Server is pinned to CPUs 0-3 and bots to 4-15 to limit same-host contention.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-experiments/001-week1-baseline/results}
mkdir -p "$OUT" bin
(cd backend && go build -o ../bin/ ./cmd/gameserver ./cmd/bots)

PORT=18080
start_server() {
  if curl -s "localhost:$PORT/healthz" >/dev/null; then echo "port $PORT already in use" >&2; exit 1; fi
  GOMAXPROCS=4 taskset -c 0-3 ./bin/gameserver -addr ":$PORT" -debug-addr 127.0.0.1:16060 "$@" >"$OUT/server.log" 2>&1 &
  SERVER=$!
  for _ in $(seq 50); do curl -sf "localhost:$PORT/healthz" >/dev/null && return; sleep 0.1; done
  echo "server did not start" >&2; exit 1
}
stop_server() { kill "$SERVER"; wait "$SERVER" 2>/dev/null || true; }
bots() { # name, bot args...
  local name=$1; shift
  echo "== $name"
  taskset -c 4-15 ./bin/bots -server "http://localhost:$PORT" -json "$@" | tee "$OUT/$name.json" |
    jq -c '{bots,connects,connect_failures,server_drops,rtt:.rtt_ms,tick:{p50:.server.tick_p50_ms,p99:.server.tick_p99_ms,overruns:.server.tick_overruns},heapKiB:.server.heap_inuse_per_player_kib,txBps:.server.tx_bytes_per_s_per_player,gor:.server.peak_goroutines,gorAfter:.server.goroutines_after}'
  sleep 2
  curl -s "localhost:$PORT/metrics" | grep -E '^(go_goroutines|localarena_players|localarena_matches) ' > "$OUT/$name.after.txt"
}

start_server
# Scale
for n in 10 50 100 200 500 1000; do bots "scale-$n" -bots "$n" -ramp 5s -duration 20s; done
# Faults at 200 bots
bots latency-100ms -bots 200 -ramp 5s -duration 20s -latency 100ms
bots latency-500ms -bots 200 -ramp 5s -duration 20s -latency 500ms
bots loss-5pct     -bots 200 -ramp 5s -duration 20s -loss 0.05
bots slow-10pct    -bots 200 -ramp 5s -duration 20s -slow 0.1
bots churn-1s      -bots 200 -ramp 2s -duration 20s -churn 1s
# Heap profile at load for memory/player evidence
taskset -c 4-15 ./bin/bots -server "http://localhost:$PORT" -bots 200 -ramp 2s -duration 15s -json >/dev/null &
B=$!; sleep 10
curl -s "localhost:16060/debug/pprof/heap" -o "$OUT/heap-200.pb.gz"
curl -s "localhost:16060/debug/pprof/profile?seconds=4" -o "$OUT/cpu-200.pb.gz"
wait $B
stop_server

# GC pressure: same 1000-bot load with GOGC=5 (collector runs ~20x more often)
GOGC=5 GOMAXPROCS=4 taskset -c 0-3 ./bin/gameserver -addr ":$PORT" -debug-addr "" >"$OUT/server-gc.log" 2>&1 &
SERVER=$!; sleep 0.5; kill -0 "$SERVER"
bots gc-pressure-gogc5-1000 -bots 1000 -ramp 5s -duration 20s
stop_server

# Tick overload: 200 Hz with 2000 bots in 20-player matches, server on 1 CPU
GOMAXPROCS=1 taskset -c 0 ./bin/gameserver -addr ":$PORT" -debug-addr "" -tick-hz 200 >"$OUT/server-overload.log" 2>&1 &
SERVER=$!; sleep 0.5; kill -0 "$SERVER"
bots overload-200hz-1cpu -bots 2000 -ramp 10s -duration 20s -input-hz 20
stop_server
echo "results in $OUT"
