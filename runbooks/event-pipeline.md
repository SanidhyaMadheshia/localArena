# Event pipeline (Kafka, spool, Streams, rewards)

**Symptom:** `LocalArenaDurableEventsDropped` (page): durable events dropped (spool full / failed). `LocalArenaEventSpoolGrowing` (ticket): gameserver spooling because Kafka is unreachable.

## Check
1. `localarena_events_total{class="durable"}` by result; `localarena_events_spool_bytes`.
2. Kafka: `docker compose --profile streaming ps kafka-1 kafka-2 kafka-3`; under-replicated partitions:
   `docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --under-replicated-partitions`.
   Fewer than 2 brokers up = below `min.insync.replicas`, producers spool by design.
3. Consumer lag: `kafka-consumer-groups.sh --describe --group localarena-streams` / `localarena-rewards-saga`.
4. DLQs: `bin/eventcat -topic game.reward-commands-dlq.v1,game.invalid-events.v1 -from-start -idle 3s`.

## Mitigate
- Restore brokers; the spool drains automatically (`replayed` counter rises, spool returns to 0). No manual replay.
- Spool full (`dropped_spool_full`): raise `MaxSpoolBytes` or free disk; events beyond the cap are lost: record the window for the postmortem.
- DLQ'd reward commands: fix the cause, then re-produce them to `game.reward-commands.v1` (idempotent by `reward_id`).

## Verify
`localarena_events_spool_bytes == 0`, lag back to ~0, no new DLQ records.
