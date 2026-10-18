#!/bin/sh
# Hot-standby entrypoint: clone the primary once with pg_basebackup, then run
# postgres. -R writes primary_conninfo + standby.signal so it streams WAL.
set -eu
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h "$PRIMARY_HOST" -U replicator -q; do echo "waiting for primary"; sleep 1; done
  PGPASSWORD="$REPLICATION_PASSWORD" pg_basebackup -h "$PRIMARY_HOST" -U replicator \
    -D "$PGDATA" -X stream -S replica1 -R -P
  chmod 0700 "$PGDATA"
fi
exec postgres -c hot_standby=on -c hot_standby_feedback=on
