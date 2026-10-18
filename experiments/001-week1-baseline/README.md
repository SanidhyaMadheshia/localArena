# Experiment 001: Week 1 baseline, scale, and network/runtime faults

## Hypothesis

At 20 Hz with 20-player matches, the actor-per-match server (adr/0003) keeps
tick p99 well under the 50 ms budget at ≥ 200 bots. Faults on one client
(latency, loss, slowness, churn) do not affect the tick loop or other
players. Under CPU starvation the server degrades by skipping ticks and
replacing snapshots, and it does not queue without bound.

## System version and configuration

- Commit: Week 1 working tree (2026-10-18). Go 1.27.2, gorilla/websocket v1.5.3.
- Host: i5-13450HX (16 logical CPUs), 15 GiB RAM, Ubuntu 24.04, loopback networking.
- Server: `GOMAXPROCS=4 taskset -c 0-3`, 20 Hz, capacity 20/match.
  Bots: `taskset -c 4-15`, 20 inputs/s each. Pinning the two sides to
  separate CPUs keeps bot CPU use from showing up as server latency.
- Reproduce: `experiments/001-week1-baseline/run.sh` (about 6 min).
  Raw JSON per scenario is in `results/`, and `*.after.txt` holds the
  goroutine/player gauges 2 s after each scenario. CPU and heap profiles at
  200 bots: `go tool pprof bin/gameserver results/cpu-200.pb.gz`.
- Metrics: server-side numbers come from `/metrics` deltas (tick histogram,
  interpolated like `histogram_quantile`). RTT is measured by bots, from
  sending input `seq` to the first snapshot with `ack >= seq`.

## Baseline traffic and measurements

5 s ramp, 20 s measured window.

| Bots | Matches | Input RTT p50/p95/p99 ms | Tick p50/p95/p99 ms | Tick start lag p99 ms | RSS MiB | Heap MiB | Server tx B/s/player | Goroutines (peak → after) |
|---:|---:|---|---|---:|---:|---:|---:|---|
| 10 | 1 | 49 / 50 / 50 | 0.09 / 0.23 / 0.25 | 2.1 | 15 | 2.8 | 8,154 | 31 → 10 |
| 50 | 3 | 49 / 50 / 51 | 0.07 / 0.23 / 0.39 | 2.3 | 16 | 4.0 | 13,011 | 113 → 10 |
| 100 | 5 | 49 / 50 / 51 | 0.09 / 0.24 / 0.77 | 2.3 | 16 | 3.6 | 14,299 | 215 → 10 |
| 200 | 10 | 25 / 51 / 52 | 0.08 / 0.24 / 0.88 | 2.9 | 19 | 5.0 | 14,488 | 420 → 10 |
| 500 | 25 | 29 / 50 / 52 | 0.07 / 0.22 / 0.84 | 4.9 | 29 | 12.5 | 14,524 | 1035 → 10 |
| 1000 | 50 | 27 / 50 / 51 | 0.07 / 0.21 / 0.76 | 5.0 | 45 | 20.6 | 14,830 | 2061 → 10 |

Zero tick overruns, missed ticks, connect failures, and server drops in every row.

- Memory per player: about 30 KiB RSS and 18 KiB heap, from the slope
  between 10 and 1000 players. Heap profile at 200 players: match state,
  gorilla `newConn` buffers, and per-client mailboxes dominate.
- Bandwidth per player: about 14.5 KB/s (≈ 725 B snapshot × 20 Hz) in a
  full 20-player match, and 8 KB/s with 10 players. Snapshot size grows
  linearly with match size, so per-match bandwidth is O(players²).
- RTT sits at 25–50 ms even on loopback. That is the tick quantization
  (an input waits for the next tick, average period/2) plus the up-to-one-
  period phase between the bot's input timer and the server tick. It is
  inherent to a 20 Hz authoritative loop and is the reason client-side
  prediction exists.
- 2 goroutines per connection plus 1 per match. Every scenario returns to
  the idle baseline of 9–10 goroutines with 0 players.

## Faults injected (200 bots unless noted)

| Scenario | Input RTT p50/p95/p99 ms | Tick p99 ms | Notes |
|---|---|---:|---|
| +100 ms latency (bot-side delay on received snapshots) | 126 / 151 / 152 | 0.90 | RTT = baseline + delay; server unaffected |
| +500 ms latency | 526 / 551 / 551 | 0.60 | same |
| 5% input loss (dropped before send) | 25 / 50 / 51 | 0.95 | seq gaps are harmless: held-input semantics |
| 10% slow clients (200–1000 ms per frame) | 25 / 50 / 5034 | 0.83 | 9,201 snapshots skipped for lagging clients; 0 write timeouts |
| Reconnect storm (mean session 1 s; 4,443 connects in ~22 s) | 25 / 47 / 50 | 0.75 | no failures; gauges back to 0 players / 10 goroutines |
| GC pressure: 1000 bots, `GOGC=5` | 33 / 55 / 61 | 19.2 | 962 GC cycles vs 74; tick p95 8.2 ms; still within budget |
| Tick overload: 2000 bots, 200 Hz, 100 matches, server on 1 CPU | 57 / 152 / 212 | 0.07 | start lag p99 99 ms, 126,439 ticks missed, 3.29 M snapshots replaced |

## Expected vs actual behavior

- Scale: as expected. Saturation was **not** reached at 1000 bots / 50
  matches on 4 cores. Tick work stays below 1 ms p99 because `Step` takes
  about 1.1 µs for 20 players (`make bench`, 0 allocs).
- Tick overload behaved as designed, and it showed where time actually
  goes. Simulation work per tick stayed at 0.07 ms p99, while the delay
  before a tick even started reached about 99 ms. The CPU profile shows 40%
  of samples in `Syscall6` (socket writes) and the rest mostly in
  scheduling and JSON. **The saturating resource is network fan-out, not
  the simulation.** `tick_duration_seconds` alone hid the overload, so
  `tick_start_lag_seconds` was added.
- Under overload the bounded design held. `time.Ticker` dropped ticks
  instead of bursting, and mailboxes replaced stale snapshots instead of
  queuing them. RSS peaked at 89 MiB for 2000 players. No write timeouts
  occurred. The 1,286 `write_error` disconnects all happened at shutdown,
  when bots closed while snapshots were in flight.
- GC: `GOGC=5` raised tick p99 about 25× but stayed under 50 ms. At the
  default `GOGC`, GC ran about 74 times in 20 s with no visible tick impact.

## Root explanation of two surprises (found in run 1, fixed, retested)

Run 1 raw data is kept in `results-run1-before-lagfix/`.

1. **Latency injection was wrong in the harness (bot bug).** In run 1, the
   bots slept `latency` on the read path before processing each frame. That
   reduces throughput to one frame per `latency` instead of adding delay,
   so stale frames piled up and RTT p50 was 6.4 s at +100 ms. The fix: a
   reader goroutine timestamps frames, and the processor releases each
   frame at arrival + delay. RTT after the fix is baseline + delay, which
   is the expected result.
2. **The latest-wins mailbox never triggered for slow clients.** In run 1,
   `snapshots_replaced` was 0 even with 10% slow bots. Linux socket buffers
   (`tcp_wmem` max 4 MiB on the sender, `tcp_rmem` 32 MiB on the receiver)
   absorbed hundreds of snapshots, so `WriteMessage` never blocked and the
   capacity-1 mailbox was always drained. The client was seconds behind
   while the server considered it healthy. Bounded application queues do
   not bound end-to-end staleness when the kernel buffers underneath them.
   - Fix (application-level flow control, protocol v1-compatible):
     clients add `st` (last processed tick) to each input. If the last tick
     sent is more than 1 s ahead of `st`, the server throttles that client
     to 1 snapshot/s until it catches up
     (`snapshots_total{result="skipped_lagging"}`).
   - First attempt: cut lagging clients off completely. That livelocked:
     the client could never catch up, and 5 bots were dropped. Second bug:
     comparing `st` against the *current* tick kept a throttled client
     throttled forever. The fix compares against the tick last *sent*.
     Both are covered by `TestLaggingClientIsThrottled`.

## Mitigation and retest result

- Fast clients are unaffected by slow ones: RTT p50/p95 are 25/50 ms both
  with and without slow bots, and tick p99 is unchanged.
- The server now stops spending bandwidth on lagging clients: 9,201
  snapshots skipped, and tx/player fell from 14.8 to 13.3 KB/s.
- RTT p99 (5 s) is the slow bots' **own** experience. They sleep up to 1 s
  per frame by construction, so the bot report does not yet split fast and
  slow bots (see remaining risks).

## Other Week 1 required checks

- Data race: `make race` (`go test -race -count=3` in `golang:1.27.2-alpine`)
  is clean for all packages. A deliberately introduced race was not run.
- Goroutine leak: goleak `VerifyTestMain` covers the `match` and `server`
  packages. The load runs show idle goroutines returning to 9–10 after every
  scenario, including 4,443 reconnects.
- Determinism: `TestDeterministicReplay` (20 players × 2,000 ticks, identical hash).
- Disconnect cleanup: `TestDisconnectCleansUp` covers both an abrupt TCP
  close and a close frame.

## Remaining risks / carried into later weeks

- Loss is application-level only (inputs dropped before send). Real TCP
  packet loss (head-of-line blocking) needs `tc netem` (root) or Toxiproxy;
  scheduled with the Week 3 fault tooling and the UDP comparison.
- Latency is simulated in the bot process, not on the link. Same follow-up.
- WebSocket vs SSE vs long-poll for status: all three are implemented and
  tested for correctness but not yet compared quantitatively
  (connections/s, bytes/update). The gRPC internal-call comparison is not
  done.
- Saturation point for 20 Hz on 4 cores was not found. The next run should
  step 2000/5000 bots with the bots on a separate machine or
  network namespace, because the bots will contend for CPU before the
  server does.
- Bandwidth is O(players²) per match. Delta or interest-managed snapshots
  are the lever if match size grows.
- The bot report should separate slow and fast bot percentiles.
- Browser check: `make run` serves the Canvas client at `http://localhost:8080`.
  Two browser tabs playing together was not verified in this run.
