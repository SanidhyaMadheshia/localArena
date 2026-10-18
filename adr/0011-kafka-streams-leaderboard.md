# ADR-0011: Kafka Streams leaderboard and sessions with exactly-once v2

- Status: accepted
- Date: 2026-10-18

## Context

The leaderboard and player sessions are stateful, partitioned computations that
must survive instance loss, restore after state loss, and not double count
duplicate events.

## Decision

- Java 21 Kafka Streams 4.4 app (`streams/`), 2 instances, 1 standby replica,
  `processing.guarantee=exactly_once_v2`, RF 3 internal topics, consumer
  session timeout 10 s.
- **Leaderboard:**
  - `PlayerScored` (a running per-match total) is re-keyed to
    `player/match` and reduced to the highest-tick record, which is the
    latest score per player per match.
  - That table is re-grouped by player and summed with an adder/subtractor
    into the `leaderboard-totals` store, then written to
    `game.leaderboard-updates.v1` (compacted).
  - Duplicates and out-of-order replays are no-ops by construction.
- **Sessions:** player activity is grouped into session windows (30 s gap,
  10 s grace) on Envelope event time, with suppression until the window
  closes, then written to `game.player-sessions.v1`.
- **Interactive queries** over HTTP (`/leaderboard/{id}` forwards to the key
  owner; `/top` scatter-gathers).
- Undecodable records are skipped and copied to `game.invalid-events.v1`.

## Consequences

- Failover in 11 s; a full state wipe restored in 10.9 s; totals identical (Experiment 004).
- EOS covers Kafka input, state and output only. The Postgres reward
  grant is a separate idempotent consumer (ADR-0012), not part of this
  guarantee.
- RocksDB requires a glibc runtime image.
- Interactive-query endpoints have no auth (week 5).
