# Runbooks

Each alert's `runbook_url` points here. Format: **symptom → check → mitigate → verify → follow up**.
Dashboards: Grafana `http://localhost:3000` → LocalArena overview. Alerts: Alertmanager `:9093`, lab pager `:9095/alerts`.

| Runbook | Alerts |
|---|---|
| [api-availability.md](api-availability.md) | LocalArenaAvailabilityBurnFast/Slow, LocalArenaTargetDown |
| [api-latency.md](api-latency.md) | LocalArenaLatencyBurnFast, LocalArenaDBPoolSaturated |
| [tick-budget.md](tick-budget.md) | LocalArenaTickBudgetBurn |
| [event-pipeline.md](event-pipeline.md) | LocalArenaDurableEventsDropped, LocalArenaEventSpoolGrowing |
| [postgres.md](postgres.md) | LocalArenaPostgresReplicaLag, LocalArenaReplicationSlotRetainingWAL; backup/restore; failover |
