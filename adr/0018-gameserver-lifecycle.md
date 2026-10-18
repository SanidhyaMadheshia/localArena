# ADR-0018: Game servers drain on termination; allocation over autoscaling

- Status: accepted
- Date: 2026-10-19

## Decision

- On SIGTERM the gameserver:
  1. flips `/readyz` to 503, so the Service stops routing new players;
  2. refuses new joins with WebSocket close 1013 "server draining";
  3. waits for connected players to finish (`Manager.Drain`), up to `-drain-timeout`;
  4. then closes matches and flushes the event spool.
- `terminationGracePeriodSeconds` = round length + margin (dev: 45 s for 30 s rounds; default: 150 s for 120 s rounds). The drain timeout is the grace period minus 5 s.
- Rollout strategy: surge 1, unavailable 0. The new server becomes ready before the old one drains.
- No HPA for game servers: scaling is allocation-driven (free slots, empty-server scale-in). See Experiment 007; Agones-style fleet controller as the target design.

## Consequences

- A deploy waits for matches to finish, so a gameserver rollout takes up to a round length.
- Players on a node that is lost (not drained) still lose their match. Matches are not migrated (README scope: optional stretch goal).
