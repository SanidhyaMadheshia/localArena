# PostgreSQL: lag, WAL retention, backup/restore, failover

## Replica lag (`LocalArenaPostgresReplicaLag`)
Check `SELECT * FROM pg_stat_replication;` on the primary. Long queries on the replica (hot_standby_feedback) or replay paused (`SELECT pg_is_wal_replay_paused();` → `SELECT pg_wal_replay_resume();`). Reads with `X-Read-After-LSN` already fall back to the primary.

## WAL retained by a slot (`LocalArenaReplicationSlotRetainingWAL`)
`SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;`
- `debezium_outbox` inactive: restart Connect (`scripts/connect.sh`). It resumes from the slot; nothing is lost.
- Abandoned slot: dropping it (`pg_drop_replication_slot`) frees disk **and loses CDC position**. Only with an owner's decision.

## Backup and restore (drill: `experiments/006-week6-observability/backup-restore.sh`)
- Backup: `docker compose exec -T postgres pg_dump -U localarena -Fc localarena > backup.dump` (consistent snapshot, no downtime).
- Restore into a fresh instance: `pg_restore --clean --if-exists --no-owner -d <db> backup.dump`, then run `bin/migrate status` and the verification queries from the drill (row counts, credit sum, constraint check).
- RPO of nightly logical dumps = up to 24 h. For lower RPO use WAL archiving / CNPG backups (week 7).

## Failover (drill: `experiments/006-week6-observability/failover-rpo.sh`)
Compose: promote the replica (`pg_ctl promote` / `SELECT pg_promote();`), point PgBouncer at it, restart the API. Measured RPO/RTO are in Experiment 006. Kubernetes: CNPG does this automatically (Experiment 002/003).
