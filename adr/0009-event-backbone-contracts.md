# ADR-0009: Kafka event backbone, topic design, protobuf contracts, non-blocking producer

- Status: accepted
- Date: 2026-10-18

## Context

Matches, players and the economy must publish facts for analytics, rewards and
leaderboards without the game tick ever waiting for Kafka, and consumers must
tolerate duplicates and schema evolution.

## Decision

- **Kafka 4.3 in KRaft mode**, 3 combined broker+controller nodes, RF 3,
  `min.insync.replicas=2`, auto topic creation off. Topics are created only by
  `scripts/kafka-topics.sh`, where key, partitions, retention and cleanup
  policy are written down per topic (match events keyed by match ID for
  per-match order; player/economy keyed by player ID; tables compacted).
- **Contracts:** one protobuf `Envelope` (`schemas/localarena/events/v1`) with
  `event_id`, `event_time`, `produced_at`, `schema_version`, `correlation_id`,
  `causation_id`, `source`, and a `oneof` payload. It is registered in
  Apicurio with `BACKWARD_TRANSITIVE` compatibility; a breaking change means
  a new package and a `.v2` topic.
- **Producer (`internal/events`):** `Publish` is a non-blocking send into a
  bounded queue. A pump calls franz-go `TryProduce` (idempotent, `acks=all`,
  20 ms linger, zstd).
  - Durable events that cannot be delivered within 10 s go to a bounded,
    CRC-framed disk spool and are replayed when Kafka answers.
  - Best-effort telemetry is dropped.
  - Event IDs are deterministic (hash of run/match/tick/kind).
  - The Kafka record timestamp is produce time; event time lives in the
    Envelope.

## Consequences

- Gameplay is unaffected by Kafka outages (Experiment 004: tick p99 0.44 ms with all brokers down).
- Losing 2 of 3 brokers stops writes (below min ISR) by design: durability over availability. Producers spool.
- One very active match is one hot partition; that is the price of per-match ordering.
- Spool writes are flushed but not fsynced per record (bounded host-crash loss).

## Alternatives considered

- Avro: equally good with Apicurio; protobuf matches the Go/gRPC stack.
- Synchronous produce from the tick: simplest, and it would freeze matches whenever Kafka is slow.
