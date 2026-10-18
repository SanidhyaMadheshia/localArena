# ADR-0012: Match-reward saga with retry topic and DLQ

- Status: accepted
- Date: 2026-10-18

## Context

A completed match grants credits from a limited season pool to each account
player. In a split system, the pool and the wallet live in different services,
so there is no single transaction; partial failure needs compensation.

## Decision

- **Orchestrated saga** in Go (`internal/rewards`, `cmd/rewards`):
  - The planner turns `MatchCompleted` into one `RewardCommand` per account
    player with points (guests, IDs ≥ 1e9, get nothing).
    `reward_id = hash(event_id, player)`.
  - The saga worker runs **reserve** (pool decrement, one transaction), then
    **grant** (wallet credit plus `RewardGranted` outbox row, one
    transaction). A permanent grant failure (player gone) triggers
    **compensate** (returns credits to the pool).
  - `reward_sagas` holds the state (`reserved/granted/compensated/rejected`),
    keyed by `reward_id`. Every step is guarded by the current state, so
    redelivery is a no-op.
- **Failures:**
  - Transient errors go to `game.reward-commands-retry.v1` with `attempt+1`
    and a `not-before` header (exponential backoff).
  - After 5 attempts, or for an undecodable command, the record goes to
    `game.reward-commands-dlq.v1` with error and source headers.
  - Offsets are committed only after the record's work and follow-ups are
    durable; a failing consumer restarts from committed offsets.

## Consequences

- Credits are applied once per reward under duplicate delivery (459 → 459 granted after replaying MatchCompleted).
- A dead-lettered reward stays `reserved` (pool credits held) until an operator replays or compensates it.
- The retry topic waits on its partition head; one backoff schedule per attempt makes that acceptable.

## Alternatives considered

- Choreography (each service reacts to events): fewer central components, but the flow is harder to see and compensate.
- Two-phase commit across services: blocking and unavailable here.
