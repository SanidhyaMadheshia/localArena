# Game tick budget (> 50 ms)

**Symptom:** `LocalArenaTickBudgetBurn`: more than 1.44% of ticks exceed 50 ms.

## Check
1. Dashboard "Tick duration p50/p99" and `localarena_tick_start_lag_seconds` (Experiment 001: overload shows as start lag, not tick work).
2. `localarena_players`, `localarena_matches`: load spike?
3. CPU: `docker stats gameserver`; container CPU limit (2).
4. Profile: `go tool pprof http://localhost:6060/debug/pprof/profile?seconds=10` (debug port, loopback only).
5. GC: `go_gc_duration_seconds`; Experiment 001 showed GOGC=5 raising tick p99 25x.

## Mitigate
- Stop admitting new matches (cap capacity), or add a gameserver instance and route new matches to it.
- Slow clients do not slow ticks (latest-wins mailbox); if `snapshots_total{result="skipped_lagging"}` is huge, that is client-side.

## Verify
`slo:ticks_slow:ratio_rate5m` < 0.001.
