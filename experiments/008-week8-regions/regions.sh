#!/usr/bin/env bash
# Experiment 008 regional scenarios (needs: docker compose --profile regions up -d + latency.sh apply).
#   experiments/008-week8-regions/regions.sh [rtt cross-region-write stale-read partition failover dns-ttl]
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/008-week8-regions/results; mkdir -p "$OUT"
L=infra/compose/regions/latency.sh
A=http://localhost:21081 B=http://localhost:22081
log() { echo "$*" | tee -a "$OUT/$S.txt"; }
ms() { curl -s -o /dev/null -w '%{time_total}' "$@" | awk '{printf "%.0f", $1*1000}'; }
med() { sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }

scen_rtt() {
  for r in A B; do u=${!r}
    log "player -> region $r API GET cosmetics: median $(for _ in $(seq 15); do ms "$u/v1/cosmetics"; echo; done | med) ms"; done
  log "bots region pick: $(bin/bots -servers http://localhost:21080,http://localhost:22080 -bots 1 -ramp 1s -duration 1s -json 2>&1 | grep 'region RTTs')"
  for r in a:21080 b:22080; do
    log "gameplay input RTT in region ${r%%:*}: $(bin/bots -server http://localhost:${r##*:} -bots 10 -ramp 1s -duration 10s -json 2>/dev/null | jq -c .rtt_ms)"; done
}

scen_cross_region_write() { # single-writer: B's writes pay the inter-region RTT
  for r in A B; do u=${!r}
    log "region $r POST /v1/players (write): median $(for i in $(seq 15); do ms -X POST "$u/v1/players" -d "{\"username\":\"cr_${r}_$$_$i\"}"; echo; done | med) ms"
    log "region $r GET /v1/players/{id} (read, local replica/cache): median $(for _ in $(seq 15); do ms "$u/v1/players/2"; echo; done | med) ms"
  done
}

scen_stale_read() { # write through region B (to A's primary), read back in B from its replica
  pid=$(curl -s -X POST "$B/v1/players" -d "{\"username\":\"st_$$\"}" | jq .id)
  docker compose exec -T postgres-replica psql -U localarena -d localarena -qtAc "SELECT pg_wal_replay_pause()" >/dev/null
  now=$(date -u +%FT%TZ); start=$(date -u -d '-5 min' +%FT%TZ)
  curl -s -D /tmp/h -o /dev/null -X POST "$B/v1/matches" -d "{\"started_at\":\"$start\",\"ended_at\":\"$now\",\"players\":[{\"player_id\":$pid,\"score\":7}]}"
  log "recorded a match for player $pid via region B (write went to A's primary); region B replica replay paused (simulated lag)"
  log "  history read in B (replica):  $(curl -s "$B/v1/players/$pid/matches" | jq length) matches  (0 = stale read)"
  log "  history read in A (replica):  $(curl -s "$A/v1/players/$pid/matches" | jq length) matches  (same replica in this lab: also stale)"
  docker compose exec -T postgres-replica psql -U localarena -d localarena -qtAc "SELECT pg_wal_replay_resume()" >/dev/null; sleep 1
  log "  after replay resumes, history in B: $(curl -s "$B/v1/players/$pid/matches" | jq length) matches"
  log "  profile reads never go stale here: cache fills read the primary, and X-Read-After-LSN reads wait for/skip a lagging replica (adr/0004, 0008)"
}

scen_partition() { # B loses the link to A: reads continue, writes fail fast
  $L partition-b >/dev/null
  log "partition B<->A: region B read HTTP $(curl -s -o /dev/null -w '%{http_code}' "$B/v1/cosmetics") in $(ms "$B/v1/cosmetics") ms"
  log "  region B write HTTP $(curl -s -o /dev/null -m 10 -w '%{http_code}' -X POST "$B/v1/players" -d "{\"username\":\"pt_$$\"}") in $(ms -m 10 -X POST "$B/v1/players" -d "{\"username\":\"pt2_$$\"}") ms (503 within the 3 s budget, not a hang)"
  log "  region B gameplay unaffected: $(bin/bots -server http://localhost:22080 -bots 5 -ramp 1s -duration 5s -json 2>/dev/null | jq -c '{connects,connect_failures,rtt_p50:.rtt_ms.p50}')"
  log "  region A unaffected: write HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/v1/players" -d "{\"username\":\"pa_$$\"}")"
  $L heal-b >/dev/null; sleep 2
  log "healed: region B write HTTP $(curl -s -o /dev/null -w '%{http_code}' -X POST "$B/v1/players" -d "{\"username\":\"ph_$$\"}")"
}

scen_failover() { # region A goes dark for players mid-session; bots reconnect to B
  bin/bots -servers http://localhost:21080,http://localhost:22080 -bots 20 -ramp 1s -duration 30s -churn 3s -json > "$OUT/failover-bots-a.json" 2>"$OUT/failover-pick.txt" &
  sleep 8; $L down-a >/dev/null; t0=$(date +%s); log "region A down for players at +8 s"
  wait
  log "  bots pinned to A after the outage: $(jq -c '{connects,connect_failures}' "$OUT/failover-bots-a.json") (reconnects fail: sticky region choice)"
  log "  re-running region selection: $(bin/bots -servers http://localhost:21080,http://localhost:22080 -bots 20 -ramp 1s -duration 10s -json 2>&1 >"$OUT/failover-bots-b.json" | grep 'region RTTs')"
  log "  after re-selection: $(jq -c '{connects,connect_failures,rtt_p50:.rtt_ms.p50}' "$OUT/failover-bots-b.json")"
  $L up-a >/dev/null
}

scen_dns_ttl() { # authoritative failover is instant; clients behind a caching resolver see it after <= TTL
  Z=infra/compose/regions/localarena.test.db
  q() { docker run --rm --network localarena_default alpine:3.22 sh -c "apk add -q bind-tools >/dev/null 2>&1; dig +noall +answer @$1 play.localarena.test TXT" | awk '{print "ttl="$2, $5, $6}'; }
  # Rewrite in place (same inode: sed -i replaces the file, which a directory bind mount
  # on Docker Desktop surfaced late) with a strictly increasing serial.
  setr() { local ser; ser=$(( $(grep -oE '\( [0-9]+ 60' "$Z" | grep -oE '[0-9]+' | head -1) + 1 ))
    sed -E "s/region=[ab] port=[0-9]+/region=$1 port=$2/; s/\( [0-9]+ 60/( $ser 60/" "$Z" > /tmp/zone.new && cat /tmp/zone.new > "$Z"; }
  docker compose --profile regions up -d resolver >/dev/null 2>&1; docker compose --profile regions restart resolver >/dev/null 2>&1; sleep 2
  log "before: authoritative $(q coredns) | resolver $(q resolver)"
  setr a 21080; sleep 35 # make sure the resolver holds a fresh A answer
  log "resolver cache primed: $(q resolver)"
  setr b 22080; t0=$(date +%s); sleep 4
  log "failover A->B at t=0 (zone edit + serial bump)"
  while :; do t=$(( $(date +%s)-t0 )); a=$(q resolver)
    log "  t=${t}s authoritative: $(q coredns | cut -d' ' -f2-) | resolver (cached): $a"
    case $a in *region=b*) break;; esac; [ $t -gt 60 ] && break; sleep 5; done
  log "clients behind the caching resolver switched after ~$(( $(date +%s)-t0 )) s (record TTL 30 s); clients that cache longer (JVM networkaddress.cache.ttl, browsers, OS) are slower still"
  setr a 21080; sleep 15; log "restored: $(q coredns)"
}

for S in ${@:-rtt cross-region-write stale-read partition failover dns-ttl}; do
  : > "$OUT/$S.txt"; echo "== $S"; "scen_${S//-/_}"
done
