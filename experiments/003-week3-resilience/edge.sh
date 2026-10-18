#!/usr/bin/env bash
# Edge-cache experiment (part of Experiment 003). Needs: garage edge-a edge-b up, assets published.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/003-week3-resilience/results/edge.txt
exec > >(tee "$OUT") 2>&1
EDGES=(http://localhost:8090 http://localhost:8091)
hdr() { curl -s -o /dev/null -D - "$1" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-cache-status"{c=$2} tolower($1)=="x-edge"{e=$2} END{printf "%s %s", e, c}'; }

echo "## 1. cold vs warm, two independent edges"
docker compose restart edge-a edge-b >/dev/null; sleep 2   # empty tmpfs caches
skin=$(curl -s ${EDGES[0]}/assets/manifest.json | jq -r '."skin-gold.svg"')
for e in "${EDGES[@]}"; do for i in 1 2 3; do echo "$e$skin -> $(hdr "$e$skin")"; done; done

echo; echo "## 2. query strings do not fragment the cache"
echo "?v=1 -> $(hdr "${EDGES[0]}$skin?v=1")    ?cachebust=$RANDOM -> $(hdr "${EDGES[0]}$skin?cachebust=$RANDOM")"

echo; echo "## 3. 'invalidation' by publishing a new version (content-hashed URL)"
old=$skin
cp web/assets/skin-gold.svg /tmp/skin-gold.svg.bak
trap 'cp /tmp/skin-gold.svg.bak web/assets/skin-gold.svg' EXIT
sed -i 's/#ffd54f/#ffb300/' web/assets/skin-gold.svg
infra/compose/assets/publish.sh 2>/dev/null | grep -E 'uploaded' || echo "(new version already in bucket)"
cp /tmp/skin-gold.svg.bak web/assets/skin-gold.svg
sleep 6 # manifest TTL 5 s
new=$(curl -s ${EDGES[0]}/assets/manifest.json | jq -r '."skin-gold.svg"')
echo "old url $old -> $(hdr "${EDGES[0]}$old") (still cached, still valid for old clients)"
echo "new url $new -> $(hdr "${EDGES[0]}$new") then $(hdr "${EDGES[0]}$new")"
infra/compose/assets/publish.sh >/dev/null # restore manifest to the original content

echo; echo "## 4. miss collapsing: 200 concurrent requests for a cold 2 MB object"
docker compose restart edge-a >/dev/null; sleep 2
big=$(curl -s ${EDGES[0]}/assets/manifest.json | jq -r '."replay-sample.bin"')
since=$(date -u +%Y-%m-%dT%H:%M:%SZ); sleep 1
seq 200 | xargs -P 200 -I{} curl -s -o /dev/null -w '%{http_code}\n' "${EDGES[0]}$big" | sort | uniq -c
sleep 1
echo "edge-a cache status for the burst (MISS = origin fetch; proxy_cache_lock on):"
docker compose logs edge-a --since "$since" | grep -o 'cache=[A-Z]*' | sort | uniq -c

echo; echo "## 5. origin down: cached assets keep serving, uncached ones fail fast"
docker compose stop garage >/dev/null
echo "cached   $big -> $(hdr "${EDGES[0]}$big") $(curl -s -o /dev/null -w '%{http_code} %{time_total}s' "${EDGES[0]}$big")"
echo "uncached on edge-b /assets/skin-default -> $(curl -s -o /dev/null -w '%{http_code} %{time_total}s' "${EDGES[1]}$(echo "$big" | sed 's/replay-sample.f28b5e85fca0.bin/skin-default.36a67fdbaca4.svg/')")"
docker compose start garage >/dev/null
echo "edge-a access log during outage:"; docker compose logs edge-a --since 10s | grep -o '"GET [^"]*" [0-9]* [0-9]* cache=[A-Z]*' | tail -2

echo; echo "## 6. throughput: 2,000 requests (50 VUs) for the 2 MB replay, warm edge"
k6 run -q --no-color --log-output=none -e URL="${EDGES[0]}$big" - <<'K6' | tail -1
import http from 'k6/http';
export const options = { vus: 50, iterations: 2000, summaryTrendStats: ['p(50)','p(99)'] };
export default function () { http.get(__ENV.URL, { responseType: 'none' }); }
export function handleSummary(d) {
  const m = d.metrics;
  return { stdout: JSON.stringify({ reqs: m.http_reqs.values.count, rps: Math.round(m.http_reqs.values.rate),
    p50_ms: m.http_req_duration.values['p(50)'], p99_ms: m.http_req_duration.values['p(99)'],
    MBps: Math.round(m.data_received.values.rate / 1e6) }) + '\n' };
}
K6
