# Experiment 004: Week 4 Kafka, Kafka Streams, CDC outbox, and the reward saga

## Hypothesis

Gameplay never waits for Kafka. Durable gameplay events survive a full Kafka
outage (spooled, then replayed) with no loss and no new duplicate IDs. Critical
economy events survive a Debezium outage and crash because they are committed
in Postgres first (outbox). Duplicate delivery cannot change credits or the
leaderboard. Streams state is restored from changelogs. Poison records and
breaking schemas are contained.

## System version and configuration

- 2026-10-18. Kafka 4.3.1 (3 combined KRaft broker+controller nodes, RF 3,
  `min.insync.replicas=2`, auto topic creation off), Debezium Connect 3.7.0,
  Apicurio 3.3.3, Kafka Streams 4.4.0 (2 instances, EOS v2, 1 standby),
  franz-go 1.22.2.
- Gameserver publishes through `internal/events` (bounded queue → franz-go
  `TryProduce`, idempotent `acks=all`, 20 ms linger, zstd; durable class
  spooled to disk on failure). Rounds are 30 s (`ROUND_SECONDS=30`).
- Reproduce: `ROUND_SECONDS=30 docker compose --profile streaming up -d`,
  `scripts/kafka-topics.sh`, `scripts/register-schemas.sh`, `scripts/connect.sh`,
  then `experiments/004-week4-streaming/run.sh [scenario ...]`. Raw output is
  in `results/`.
- One host, about 3.2 GB for all containers.

## Results

| Scenario | Fault | Result |
|---|---|---|
| Kafka down | all 3 brokers stopped 60 s during 100-bot play | tick p99 **0.44 ms**, 0 overruns, 0 connect failures; 372 durable events spooled (42 KB), all replayed; **883 queued = 883 distinct IDs in Kafka, 0 lost, 0 duplicates** |
| Broker loss | kill -9 one broker, then a second | 1 lost: 61 under-replicated partitions, ISR 2/3, writes continue. 2 lost: below min ISR, `acks=all` refuses writes, events spool; after restart 0 under-replicated, spool drained (531 replayed). Gameplay unaffected (tick p99 0.46 ms) |
| Streams failover | kill -9 one of 2 instances | all partitions served by the survivor after **11 s**; totals identical |
| State wipe | both instances' RocksDB volumes deleted | RUNNING again in **10.9 s** from changelogs (e.g. 155/175/99 records per leaderboard partition); totals identical |
| Poison record | garbage bytes on `game.match-events.v1` and `game.reward-commands.v1` | Streams skipped it and wrote it to `game.invalid-events.v1`, both instances stayed RUNNING; the saga dead-lettered its poison command (+1 DLQ) |
| Duplicates | re-produced 46 real MatchCompleted/PlayerScored records verbatim (and, by accident, ~1.45 M in an earlier runaway replay) | credits sum unchanged, granted sagas 459 → 459, leaderboard identical |
| Hot partition | 1 match vs 10 matches | 1 match: all 24 records in **one** partition (key = match ID, needed for per-match order); 10 matches: 56/69/29/0/46/75 across 6 partitions |
| CDC outage + crash | Connect stopped, 200 purchases, then started and kill -9 after 4 s mid-backlog | slot retained 674 kB WAL; after recovery **200/200 published, 0 lost, 0 duplicates** |
| Breaking schema | `PlayerScored.player_id` int64 → string | Apicurio (BACKWARD_TRANSITIVE) **rejected** it; an additive field is accepted |

## Bugs found and fixed during the experiment

1. **The spool never replayed.** franz-go measures the delivery timeout from
   `Record.Timestamp`. Records were stamped with event time, so every replayed
   record (older than 10 s) failed instantly, forever. Fix: record timestamp =
   produce time (re-stamped on replay); event time stays in the Envelope, where
   Streams' timestamp extractor reads it. Regression test
   `TestReplayAfterDeliveryTimeoutElapsed`.
2. **Streams failover took 46 s.** The default consumer `session.timeout.ms`
   (45 s) decides how fast a killed instance is noticed. Set to 10 s with 1 s
   heartbeats: 11 s.
3. **Streams crashed at start on Alpine.** RocksDB's JNI library needs glibc's
   `libstdc++`. Runtime image is now `eclipse-temurin:21-jre` (Ubuntu).
4. **The replay tool chased its own output.** It produced into the topic it
   read with no stop offset, and pushed about 1.45 M duplicates before it was
   stopped. It now stops at the end offsets that existed at start and
   produces asynchronously. Side effect: that run was a far larger duplicate
   test than planned, and credits and leaderboard were still unchanged.
5. **`eventcat -idle` exited immediately** (it cancelled its own poll context).

## Root explanation

- **Gameplay is isolated from Kafka by design:** the tick only does a
  non-blocking channel send, and everything slow happens on other goroutines.
- **No loss comes from different mechanisms per class:** durable gameplay
  events use the disk spool; critical economy events use the outbox in the
  same Postgres transaction plus Debezium's replication-slot position.
  Best-effort telemetry is dropped (342 of 606 in the Kafka-down run).
- **No double effects come from idempotent consumers, not from exactly-once
  delivery:** deterministic event IDs, reward IDs derived from the causing
  event, state-guarded saga steps, and a leaderboard built from "latest score
  per player per match" instead of a running sum. EOS v2 covers only Kafka →
  Kafka inside Streams.

## Remaining risks

- The spool flushes to the OS but does not fsync per record; a host crash can
  lose the last few spooled events.
- The poison record produced 2 records in `game.invalid-events.v1`, not 1.
  Not investigated; a task retry under EOS is the likely cause.
- The `TopologyTestDriver` (4.4) does not surface deserialization-handler DLQ
  records, so DLQ behavior is verified only against the real cluster.
- A dead-lettered reward stays `reserved` (credits held from the pool) until
  an operator acts. A reconciliation job is week 6 work.
- Apicurio uses in-memory H2: schemas are re-registered from `schemas/` on restart.
- Hot partitions are inherent to per-match keys. A very large match would
  need sub-keying (e.g. match/shard) and per-shard ordering.
