# ADR-0001: Authoritative fixed-tick server with a deterministic engine

- Status: accepted
- Date: 2026-10-18

## Context

LocalArena players compete for score. If a client could report its own
position or score, any modified client could teleport or award itself points.
Later weeks also need replays, anti-cheat analytics, and reproducible
experiments, and all of those require that the same inputs always produce the
same world.

## Decision

- The server owns all gameplay state: position, collisions, pickups, score,
  and outcome. A client sends only *intent* (`dx, dy ∈ {-1,0,1}` plus a
  sequence number). The engine clamps every input, so `dx=100` moves at
  normal speed.
- The simulation advances in fixed ticks (default 20 Hz, 50 ms budget). A
  tick is a pure `Step()` with no wall-clock reads.
- Determinism rules in `internal/engine`: integer coordinates, players
  iterated in ascending ID order, pickup ties won by the lowest ID, and all
  randomness from a seeded PCG owned by the world. `World.Hash()` digests the
  full state; `TestDeterministicReplay` replays 2,000 ticks × 20 players
  twice and requires identical hashes.
- Inputs are applied in arrival order within a tick. Stale or duplicate
  sequence numbers are dropped, so inputs are idempotent.

## Consequences

- Cheating is limited to choosing inputs; speed hacks and score injection
  are impossible by construction.
- Each player sees their own movement one RTT plus up to one tick late.
  Client-side prediction and reconciliation (Gambetta's "Fast-Paced
  Multiplayer") are a stretch goal; `ack` is already in every snapshot to
  support them.
- `math/rand/v2` is used on purpose (gosec G404 is excluded): the
  simulation needs reproducibility, not unpredictability. Secrets and tokens
  must still use `crypto/rand`.
- Server CPU grows with players × ticks. Experiment 001 measures it.

## Alternatives considered

- Client-authoritative with server validation: less server CPU, but every
  rule has to be duplicated as a validator, and missing a rule becomes an exploit.
- Lockstep peer-to-peer (RTS style): needs every peer to be deterministic
  and lets the slowest peer stall everyone. Not suitable for 20 players with
  churn.
