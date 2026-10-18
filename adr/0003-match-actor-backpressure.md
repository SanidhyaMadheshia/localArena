# ADR-0003: One actor per match, bounded queues, latest-wins snapshots

- Status: accepted
- Date: 2026-10-18

## Context

Many goroutines (two per connection) interact with shared match state. Locks
around the world would make tick duration depend on lock contention. A
blocking send to a slow client would stall the tick loop for every player.

## Decision

- Each match is a single goroutine (`match.run`) that exclusively owns its
  `engine.World`. Join, leave, and input requests reach it through channels.
  The world has no locks.
- Inputs: one buffered channel per match (1,024 entries). `Submit` never
  blocks; when the channel is full the input is dropped and counted
  (`inputs_total{result="queue_full"}`). Held-input semantics make this
  safe, because the next input restates the intent.
- Snapshots: each client has a **capacity-1 mailbox**. The match offers
  without blocking and replaces an unread snapshot
  (`snapshots_total{result="replaced"}`). A slow client therefore costs at
  most one buffered snapshot and always receives the newest state.
- The mailbox alone is not enough: kernel socket buffers (MiBs) absorb
  hundreds of snapshots before a write blocks, so a slow client can be
  seconds stale while the mailbox never fills (Experiment 001). Clients
  therefore report `st` (last processed tick). When the last tick sent is
  more than 1 s ahead of `st`, the session throttles to 1 snapshot/s until
  the client catches up (`snapshots_total{result="skipped_lagging"}`). The
  session throttles instead of stopping entirely, because a client that
  receives nothing can never catch up.
- A client that cannot absorb a write within 5 s (`writeWait`) is
  disconnected (`disconnects_total{reason="write_timeout"}`), which bounds
  the kernel send buffer and goroutine lifetime.
- The manager assigns players to the first match with free capacity under a
  mutex, so concurrent joins cannot overfill a match (tested with 200
  concurrent joins). Empty matches are stopped.
- Shutdown: `Manager.Close` cancels every match. Each match closes its
  clients' `Done` channels, writePump sends close 1001, and readPump exits.
  goleak verifies that no goroutines remain.

## Consequences

- A match uses at most one CPU core. Scale comes from more matches (one
  goroutine each), not from parallelizing a single tick.
- Snapshot fan-out is O(players) per tick. The shared body is encoded once
  per tick; only `ack` is spliced per client.
- Dropping data is a deliberate policy. Ordinary bounded queues would add
  latency (stale state) before they ever dropped anything.

## Alternatives considered

- `sync.RWMutex` around the world: simpler, but readers (fan-out) and the
  writer (step) contend, and a slow client still blocks if a send happens
  under the lock.
- Large per-client buffers (the chat example uses 256): appropriate for chat,
  where every message matters, but wrong for state snapshots, where only the
  newest one matters.
