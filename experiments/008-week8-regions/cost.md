# Cost model: resources per player and per match (Week 8)

Derived from measurements in Experiments 001-008 on one laptop
(i5-13450HX, Docker Desktop). These are **resource** numbers. Prices depend on
the provider and change often: plug them in from your provider's pricing
calculator instead of trusting a number written here.

## Measured inputs

| Input | Value | Source |
|---|---|---|
| Gameserver CPU at 200 players (10 matches, 20 Hz) | 0.50 cores (50% of one core) | `docker stats`, this week |
| Gameserver memory at 200 players | 53 MiB; at 1000 players 46 MiB RSS peak in Exp 001 (≈ 30 KiB/player slope + ~15 MiB base) | Exp 001, this week |
| Snapshot egress per player | 14.5-18.8 KB/s (≈ 725 B snapshot × 20 Hz, full 20-player match) | Exp 001, this week |
| API CPU at 300 req/s (3 replicas) | ≈ 0.1-0.2 cores total; one API replica idles at ~12 MiB | Exp 003, this week |
| Kafka storage per match event | ≈ 245 B/record on disk per replica (357 MB / 1.46 M records, zstd batches) | `kafka-log-dirs`, this week |
| Match events per 2-min, 20-player round | ≈ 1 MatchStarted + 20 Joined + ~600 PlayerScored + 1 Completed ≈ 620 | Exp 004 (~24 records per short round at low scoring; scales with pickups) |
| Postgres per finished match | 1 `matches` row + 20 `match_players` rows ≈ 2 KB incl. indexes | Exp 002 |
| Streams state per player | leaderboard total + per-match rows ≈ 100 B/player/match in RocksDB + changelog | Exp 004 |

## Per concurrent player (CCU)

| Resource | Per CCU | Per 1,000 CCU |
|---|---|---|
| Gameserver CPU | 0.0025 cores | 2.5 cores |
| Gameserver memory | ~30 KiB (+ fixed base per process) | ~30 MiB + base |
| Egress (snapshots) | ~15 KB/s ≈ 0.12 Mbit/s ≈ **39 GB/month** at 24/7 | **~39 TB/month** |

**Egress dominates.** At 1,000 concurrent players the CPU fits on a few
cores, while snapshot bandwidth is about 39 TB per month. That makes
bandwidth the first optimization target:

- Delta snapshots, which send only what changed: probably a 3-10× saving on typical frames. Not measured here.
- A binary encoding (protobuf) instead of JSON: about 2-3× saving.
- Interest management, which sends only nearby players.

## Per match (2-minute round, 20 players)

| Resource | Per match |
|---|---|
| Gameserver CPU | 20 × 0.0025 cores × 120 s = 6 core-seconds |
| Egress | 20 players × 15 KB/s × 120 s ≈ 36 MB |
| Kafka storage | 620 events × 245 B × RF 3 ≈ 455 KB, kept 7 days (retention), then deleted |
| Postgres | ≈ 2 KB per match, kept forever (partitioned by month; old partitions can be detached/archived) |
| Rewards | 20 saga rows + 20 outbox rows ≈ 10 KB |

Kafka storage at 1,000 CCU, running continuously:

- That is 50 matches every 2 minutes, which is 36,000 matches/day.
- 36,000 × 455 KB ≈ **16 GB/day**, or about **115 GB** at 7-day retention across the 3 replicas.

Retention is the cost lever: halving it halves storage. Compaction does not
help for event topics.

## Fixed cost (independent of players)

On a "minimum HA" footprint, the platform itself costs more than the game at
low player counts:

| Component | Minimum HA footprint (lab requests/limits) |
|---|---|
| Kafka | 3 brokers × ~0.4-0.8 GB memory |
| Postgres | 2 instances + 2 poolers |
| Kafka Streams | 2 instances |
| API | 2 replicas (HPA min) |
| Supporting services | OPA ×2, Valkey, flagd, Keycloak, observability stack (Prometheus, Loki, Tempo, Grafana ≈ 1.5 GB together) |

Below a few hundred CCU the fixed platform cost is larger than the
per-player cost. That is the case for managed services or scale-to-zero
for spiky, non-latency-critical work such as replay archiving. Gameplay
servers cannot scale to zero, because cold starts are visible to players.

## Cold start (measured where it matters)

| Component | Cold start observed |
|---|---|
| Go API pod | ready in ~3 s (startup probe) |
| Gameserver | ready in < 2 s |
| Kafka Streams instance | ~10-45 s to RUNNING (rebalance + state restore; Exp 004) |
| CNPG Postgres replica | minutes (base backup) |

This is why game servers and stream processors stay warm (a buffer of
ready servers). Only stateless request handlers are good candidates for
aggressive autoscaling.

## Reproduce

```bash
docker stats --no-stream $(docker compose ps -q gameserver)   # under bin/bots -bots 200
docker compose exec kafka-1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server kafka-1:19092 --describe --topic-list game.match-events.v1
```
