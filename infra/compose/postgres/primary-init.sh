#!/bin/sh
# Runs once on the primary's first start (docker-entrypoint-initdb.d).
# Creates the streaming-replication role and a physical slot so the replica
# never loses WAL it has not received yet.
set -eu
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
SELECT pg_create_physical_replication_slot('replica1');
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- Lets the app role pause/resume replay on the replica for the stale-read experiment.
GRANT EXECUTE ON FUNCTION pg_wal_replay_pause(), pg_wal_replay_resume() TO ${POSTGRES_USER};
SQL
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
