#!/usr/bin/env bash
# Create LocalArena topics (idempotent). Every topic is an explicit decision:
# key, partition count, retention, cleanup policy (adr/0009). RF 3 / min ISR 2
# comes from the broker defaults. Usage: scripts/kafka-topics.sh
set -euo pipefail
cd "$(dirname "$0")/.."
K=(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092)

#     name                          partitions  config                                                      key
topics=(
  "game.match-events.v1             6  retention.ms=604800000                                        match_id"
  "game.player-events.v1            6  retention.ms=604800000                                        player_id"
  "game.economy-events.v1           6  retention.ms=2592000000                                       player_id"
  "game.matchmaking-events.v1       3  retention.ms=259200000                                        region:skill_bucket"
  "game.server-telemetry.v1         3  retention.ms=86400000,compression.type=zstd                   server_id"
  "game.profile-table.v1            6  cleanup.policy=compact,min.compaction.lag.ms=60000            player_id"
  "game.rules-table.v1              1  cleanup.policy=compact                                        rule_id"
  "game.invalid-events.v1           3  retention.ms=1209600000                                       original_key"
  "game.leaderboard-updates.v1      6  cleanup.policy=compact                                        player_id"
  "game.player-sessions.v1          6  retention.ms=604800000                                        player_id"
  "game.reward-commands.v1          6  retention.ms=604800000                                        player_id"
  "game.reward-commands-retry.v1    6  retention.ms=604800000                                        player_id"
  "game.reward-commands-dlq.v1      3  retention.ms=2592000000                                       player_id"
)
for t in "${topics[@]}"; do
  read -r name parts cfg _key <<<"$t"
  args=(--create --if-not-exists --topic "$name" --partitions "$parts")
  IFS=, read -ra kv <<<"$cfg"; for c in "${kv[@]}"; do args+=(--config "$c"); done
  "${K[@]}" "${args[@]}" >/dev/null
done
"${K[@]}" --describe --topics-with-overrides | grep -E '^Topic: game\.' | awk '{print $2, $4, $6, $8, $10}' | column -t
