#!/usr/bin/env bash
# Experiment 004 runner (Week 4 streaming). Needs: ROUND_SECONDS=30 docker compose --profile streaming up -d
#   experiments/004-week4-streaming/run.sh [scenario ...]
# Scenarios: kafka-down broker-loss streams-failover state-wipe poison duplicates hot-partition cdc-outage schema
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/004-week4-streaming/results
mkdir -p "$OUT" bin
(cd backend && go build -o ../bin/ ./cmd/bots ./cmd/eventcat)
DC=(docker compose --profile streaming)
KT=("${DC[@]}" exec -T kafka-1 /opt/kafka/bin)
PSQL=("${DC[@]}" exec -T postgres psql -U localarena -d localarena -qtA -c)

ends() { "${KT[@]}/kafka-get-offsets.sh" --bootstrap-server kafka-1:19092 --topic "$1" | awk -F: '{s+=$3} END{print s+0}'; }
metric() { curl -s localhost:8080/metrics | awk -v m="$1" '$1==m{print $2}' | head -1; }
ev() { metric "localarena_events_total{class=\"durable\",result=\"$1\"}"; }
wait_streams() { for _ in $(seq 120); do [ "$(curl -s localhost:7071/healthz | jq -r .state 2>/dev/null)" = RUNNING ] && [ "$(curl -s localhost:7072/healthz | jq -r .state 2>/dev/null)" = RUNNING ] && return; sleep 1; done; }
log() { echo "$*" | tee -a "$OUT/$SCEN.txt"; }

scen_kafka_down() { # all brokers down for 60 s while 100 bots play
  "${DC[@]}" restart gameserver >/dev/null; sleep 3
  local before_m before_p; before_m=$(ends game.match-events.v1); before_p=$(ends game.player-events.v1)
  bin/bots -server http://localhost:8080 -bots 100 -accounts 1 -ramp 3s -duration 120s -json > "$OUT/kafka-down.bots.json" &
  local B=$!; sleep 25
  log "t=25s stop kafka-1..3"; "${DC[@]}" stop kafka-1 kafka-2 kafka-3 >/dev/null
  for t in 15 30 45 60; do sleep 15; log "  +${t}s down: spool_bytes=$(metric localarena_events_spool_bytes) spooled=$(ev spooled) delivered=$(ev delivered)"; done
  log "start kafka"; "${DC[@]}" start kafka-1 kafka-2 kafka-3 >/dev/null
  wait $B
  sleep 15
  log "bots: $(jq -c '{connects,connect_failures,rtt_ms,tick_p99:.server.tick_p99_ms,tick_overruns:.server.tick_overruns}' "$OUT/kafka-down.bots.json")"
  local q d s r dq; q=$(ev queued); d=$(ev delivered); s=$(ev spooled); r=$(ev replayed); dq=$(metric 'localarena_events_total{class="durable",result="dropped_queue_full"}')
  sleep 0
  log "durable events: queued=$q delivered=$d spooled=$s replayed=$r dropped_queue_full=${dq:-0} spool_bytes_now=$(metric localarena_events_spool_bytes)"
  for _ in $(seq 30); do [ "$(metric localarena_events_spool_bytes)" = 0 ] && break; sleep 2; done
  log "spool drained: spool_bytes=$(metric localarena_events_spool_bytes) replayed=$(ev replayed)"
  local inst; inst=$(bin/eventcat -topic game.match-events.v1 -from-start -idle 5s | jq -r '.event.correlationId' | tail -1 | cut -d/ -f1)
  bin/eventcat -topic game.match-events.v1,game.player-events.v1 -from-start -idle 8s | jq -r --arg i "$inst/" 'select(.event.correlationId|startswith($i)) | .event.eventId' > /tmp/ids.txt
  local dist; dist=$(sort -u /tmp/ids.txt | wc -l)
  log "this server instance ($inst): records=$(wc -l < /tmp/ids.txt) distinct_event_ids=$dist duplicates=$(( $(wc -l < /tmp/ids.txt) - dist )) lost=$(( q - dist )) (of $q queued)"
  log "topic growth: match +$(( $(ends game.match-events.v1) - before_m )), player +$(( $(ends game.player-events.v1) - before_p ))"
}

scen_broker_loss() { # one broker: ISR shrinks, writes continue; two brokers: below min ISR
  bin/bots -server http://localhost:8080 -bots 40 -accounts 1 -ramp 2s -duration 90s -json > "$OUT/broker-loss.bots.json" &
  local B=$!; sleep 15
  local m0; m0=$(ends game.match-events.v1)
  log "kill -9 kafka-2"; docker kill localarena-kafka-2-1 >/dev/null; sleep 10
  log "  under-replicated partitions: $("${KT[@]}/kafka-topics.sh" --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions | grep -c 'Topic: game' || true)"
  log "  sample: $("${KT[@]}/kafka-topics.sh" --bootstrap-server kafka-1:19092 --describe --topic game.match-events.v1 | sed -n 2p | awk '{print $5,$6,$7,$8,$9,$10}')"
  sleep 10; log "  match events written with 2/3 brokers: +$(( $(ends game.match-events.v1) - m0 ))  delivered=$(ev delivered) spooled=$(ev spooled)"
  local s1 m1; s1=$(ev spooled); m1=$(ends game.match-events.v1)
  log "kill -9 kafka-3 (1/3 left, below min.insync.replicas=2)"; docker kill localarena-kafka-3-1 >/dev/null; sleep 25
  # Offsets can't be read reliably with 1 broker (most partitions have no ISR quorum); use producer metrics.
  log "  +25s with 1 broker: durable spooled +$(( $(ev spooled) - s1 )) (acks=all needs 2 in-sync replicas: NOT_ENOUGH_REPLICAS)"
  "${DC[@]}" start kafka-2 kafka-3 >/dev/null; wait $B; sleep 20
  log "restarted; under-replicated now: $("${KT[@]}/kafka-topics.sh" --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions | grep -c 'Topic: game' || true); spool_bytes=$(metric localarena_events_spool_bytes) replayed=$(ev replayed)"
  log "bots: $(jq -c '{connect_failures,rtt_ms,tick_p99:.server.tick_p99_ms}' "$OUT/broker-loss.bots.json")"
}

top() { curl -s "localhost:$1/top?n=200" | jq -c 'map([.player_id,.total])'; }
scen_streams_failover() { # kill one instance; the other takes over active + standby tasks
  wait_streams; local before; before=$(top 7071); local t0; t0=$(date +%s.%N)
  log "kill -9 streams-2 (standby replicas: ${STREAMS_STANDBY:-1})"; docker kill localarena-streams-2-1 >/dev/null
  for _ in $(seq 120); do
    if curl -s localhost:7071/healthz | jq -e '.state=="RUNNING"' >/dev/null && curl -sf "localhost:7071/top?n=200" >/dev/null; then
      local after; after=$(curl -s localhost:7071/top?n=200 | jq -c 'map([.player_id,.total])')
      [ "$after" != "[]" ] && break
    fi; sleep 0.5; done
  local after; after=$(top 7071)
  log "  serving all partitions from streams-1 after $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) s; totals identical: $([ "$after" = "$before" ] && echo yes || echo "no: $(diff <(jq -c '.[]' <<<"$before" | sort) <(jq -c '.[]' <<<"$after" | sort) | grep '^[<>]' | head -4 | tr '\n' ' ')")"
  "${DC[@]}" start streams-2 >/dev/null; wait_streams; log "  streams-2 rejoined"
}

scen_state_wipe() { # delete one instance's RocksDB state; it restores from the changelog
  wait_streams; local before; before=$(top 7071)
  "${DC[@]}" rm -sf streams-1 streams-2 >/dev/null 2>&1
  docker volume rm -f localarena_streams-1 localarena_streams-2 >/dev/null
  log "both instances stopped, both state volumes deleted"
  local t0; t0=$(date +%s.%N); "${DC[@]}" up -d streams-1 streams-2 >/dev/null 2>&1; wait_streams
  log "  restored to RUNNING in $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-5) s; totals identical to before: $([ "$(top 7071)" = "$before" ] && echo yes || echo "no ($before vs $(top 7071))")"
  "${DC[@]}" logs streams-1 --since 2m 2>/dev/null | grep -oE 'Finished restoring changelog [^ ]+ to store [^ ]+ with a total number of [0-9]+ records' | sort | uniq | head -6 | tee -a "$OUT/$SCEN.txt" || true
}

scen_poison() {
  local inv0; inv0=$(ends game.invalid-events.v1); local dlq0; dlq0=$(ends game.reward-commands-dlq.v1)
  printf 'not-a-protobuf\xff\xff\n' | "${KT[@]}/kafka-console-producer.sh" --bootstrap-server kafka-1:19092 --topic game.match-events.v1 >/dev/null
  printf 'garbage\xff\n' | "${KT[@]}/kafka-console-producer.sh" --bootstrap-server kafka-1:19092 --topic game.reward-commands.v1 >/dev/null
  sleep 10
  log "poison into match-events: game.invalid-events.v1 +$(( $(ends game.invalid-events.v1) - inv0 )); streams state: $(curl -s localhost:7071/healthz) $(curl -s localhost:7072/healthz)"
  log "poison into reward-commands: dlq +$(( $(ends game.reward-commands-dlq.v1) - dlq0 ))"
}

scen_duplicates() { # re-deliver real MatchCompleted/PlayerScored records verbatim: totals must not change
  bin/bots -server http://localhost:8080 -bots 20 -accounts 1 -ramp 1s -duration 40s -json >/dev/null   # >= 1 completed 30 s round
  sleep 15
  local c0 g0 lb0
  c0=$("${PSQL[@]}" "select coalesce(sum(credits),0) from profiles")
  g0=$("${PSQL[@]}" "select count(*) from reward_sagas where state='granted'")
  lb0=$(top 7071)
  log "re-producing up to 5000 MatchCompleted/PlayerScored records byte for byte: $(timeout 120 bin/eventcat -topic game.match-events.v1 -from-start -idle 5s -replay MatchCompleted,PlayerScored -replay-max 5000 2>&1 | tail -1)"
  sleep 20
  log "credits sum: $c0 -> $("${PSQL[@]}" "select coalesce(sum(credits),0) from profiles"); granted sagas: $g0 -> $("${PSQL[@]}" "select count(*) from reward_sagas where state='granted'")"
  log "leaderboard identical: $([ "$(top 7071)" = "$lb0" ] && echo yes || echo no)"
}

scen_hot_partition() { # per-match keys: one match = one partition
  local before; before=$("${KT[@]}/kafka-get-offsets.sh" --bootstrap-server kafka-1:19092 --topic game.match-events.v1 | awk -F: '{print $3}' | paste -sd' ')
  bin/bots -server http://localhost:8080 -bots 20 -accounts 1 -ramp 1s -duration 20s -json >/dev/null
  local mid; mid=$("${KT[@]}/kafka-get-offsets.sh" --bootstrap-server kafka-1:19092 --topic game.match-events.v1 | awk -F: '{print $3}' | paste -sd' ')
  bin/bots -server http://localhost:8080 -bots 200 -accounts 1 -ramp 2s -duration 20s -json >/dev/null
  local after; after=$("${KT[@]}/kafka-get-offsets.sh" --bootstrap-server kafka-1:19092 --topic game.match-events.v1 | awk -F: '{print $3}' | paste -sd' ')
  log "1 match (20 bots), records per partition: $(paste -d- <(tr ' ' '\n' <<<"$mid") <(tr ' ' '\n' <<<"$before") | bc | paste -sd' ')"
  log "10 matches (200 bots), records per partition: $(paste -d- <(tr ' ' '\n' <<<"$after") <(tr ' ' '\n' <<<"$mid") | bc | paste -sd' ')"
}

scen_cdc_outage() { # Debezium down while purchases commit; then a hard crash mid-backlog
  local e0; e0=$(ends game.economy-events.v1)
  "${DC[@]}" stop connect >/dev/null; log "connect stopped"
  local pid0; pid0=$("${PSQL[@]}" "select coalesce(max(id),0) from purchases")
  local item; item=$("${PSQL[@]}" "insert into cosmetics (sku, name, price) values ('cdc-'||extract(epoch from now())::bigint, 'CDC drill', 1) returning id" | head -1)
  for i in $(seq 200); do # 200 distinct players buy a brand-new item: every purchase commits
    curl -s -o /dev/null -X POST localhost:10000/v1/purchases -H "Idempotency-Key: cdc-$item-$i" \
      -d "{\"player_id\":$((1 + i)),\"cosmetic_id\":$item}" ; done
  local n; n=$("${PSQL[@]}" "select count(*) from purchases where id > $pid0")
  log "  $n purchases committed while CDC was down; topic +$(( $(ends game.economy-events.v1) - e0 ))"
  log "  replication slot retained WAL: $("${PSQL[@]}" "select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) from pg_replication_slots where slot_name='debezium_outbox'")"
  "${DC[@]}" start connect >/dev/null; sleep 4; docker kill localarena-connect-1 >/dev/null; log "  connect started, then kill -9 after 4 s (mid-backlog)"
  sleep 3; "${DC[@]}" start connect >/dev/null
  for _ in $(seq 60); do [ $(( $(ends game.economy-events.v1) - e0 )) -ge "$n" ] && break; sleep 2; done; sleep 5
  bin/eventcat -topic game.economy-events.v1 -from-start -idle 6s | jq -r --argjson p "$pid0" 'select(.event.cosmeticPurchased and ((.event.cosmeticPurchased.purchaseId|tonumber) > $p)) | .event.eventId' > /tmp/cdc.txt
  log "  after recovery: records=$(wc -l < /tmp/cdc.txt) distinct=$(sort -u /tmp/cdc.txt | wc -l) committed=$n (lost=$(( n - $(sort -u /tmp/cdc.txt | wc -l) )), duplicates=$(( $(wc -l < /tmp/cdc.txt) - $(sort -u /tmp/cdc.txt | wc -l) )))"
}

scen_schema() {
  sed 's/  int64 player_id = 2;\n  uint32 score = 3;/X/' schemas/localarena/events/v1/events.proto | python3 -c "
import sys;s=sys.stdin.read();print(s.replace('message PlayerScored {\n  string match_id = 1;\n  int64 player_id = 2;','message PlayerScored {\n  string match_id = 1;\n  string player_id = 2;'),end='')" > /tmp/break.proto
  sed 's/  string source = 7;/  string source = 7;\n  string region = 8;/' schemas/localarena/events/v1/events.proto > /tmp/add.proto
  log "additive field:            $(scripts/register-schemas.sh --check /tmp/add.proto || true)"
  log "int64 -> string on field:  $(scripts/register-schemas.sh --check /tmp/break.proto || true)"
}

all=(kafka-down broker-loss streams-failover state-wipe poison duplicates hot-partition cdc-outage schema)
for SCEN in "${@:-${all[@]}}"; do
  : > "$OUT/$SCEN.txt"; echo "== $SCEN"
  "scen_${SCEN//-/_}"
done
