#!/usr/bin/env bash
# Apply the simulated WAN with Toxiproxy (adr/0021):
#   player -> region A: 20 ms +-5 (players are "near" A)
#   player -> region B: 40 ms +-10
#   region B -> region A database: 75 ms each way (150 ms RTT)
# Usage: latency.sh apply | clear | partition-b | heal-b
set -euo pipefail
T=http://localhost:8474
tox() { curl -sf -X POST "$T/proxies/$1/toxics" -H 'Content-Type: application/json' -d "$2" >/dev/null; }
clear() { for p in $(curl -s $T/proxies | jq -r 'keys[]'); do
  for t in $(curl -s "$T/proxies/$p/toxics" | jq -r '.[].name'); do curl -sf -X DELETE "$T/proxies/$p/toxics/$t" >/dev/null; done; done; }
case ${1:-apply} in
apply)
  clear
  for d in downstream upstream; do
    tox player-to-a-game "{\"name\":\"lat-$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":10,\"jitter\":3}}"
    tox player-to-a-api  "{\"name\":\"lat-$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":10,\"jitter\":3}}"
    tox player-to-b-game "{\"name\":\"lat-$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":20,\"jitter\":5}}"
    tox player-to-b-api  "{\"name\":\"lat-$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":20,\"jitter\":5}}"
    tox b-to-a-postgres  "{\"name\":\"lat-$d\",\"type\":\"latency\",\"stream\":\"$d\",\"attributes\":{\"latency\":75,\"jitter\":5}}"
  done
  echo "WAN latency applied" ;;
clear) clear; echo "cleared" ;;
partition-b) # region B loses its link to region A (database)
  curl -sf -X POST $T/proxies/b-to-a-postgres -d '{"enabled":false}' >/dev/null; echo "region B partitioned from A" ;;
heal-b) curl -sf -X POST $T/proxies/b-to-a-postgres -d '{"enabled":true}' >/dev/null; echo "healed" ;;
down-a) # region A unreachable for players
  for p in player-to-a-game player-to-a-api; do curl -sf -X POST "$T/proxies/$p" -d '{"enabled":false}' >/dev/null; done; echo "region A down for players" ;;
up-a) for p in player-to-a-game player-to-a-api; do curl -sf -X POST "$T/proxies/$p" -d '{"enabled":true}' >/dev/null; done; echo "region A back" ;;
esac
