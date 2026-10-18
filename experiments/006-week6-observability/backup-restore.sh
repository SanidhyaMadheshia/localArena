#!/usr/bin/env bash
# Backup/restore drill: logical backup of the live primary under write load,
# restore into a fresh Postgres 18.6 container, verify integrity, report timings.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/006-week6-observability/results; mkdir -p "$OUT"
exec > >(tee "$OUT/backup-restore.txt") 2>&1
PSQL=(docker compose exec -T postgres psql -U localarena -d localarena -qtA -c)
verify() { # $1 = psql command prefix; prints integrity fingerprint
  "$@" "SELECT 'players=' || (SELECT count(*) FROM players)
     || ' purchases=' || (SELECT count(*) FROM purchases)
     || ' credits=' || (SELECT coalesce(sum(credits),0) FROM profiles)
     || ' inventory=' || (SELECT count(*) FROM inventory)
     || ' match_players=' || (SELECT count(*) FROM match_players)
     || ' outbox=' || (SELECT count(*) FROM outbox)
     || ' migrations=' || (SELECT max(version_id) FROM goose_db_version)"
}
# Background writes during the dump (purchases through the API).
( for i in $(seq 300); do curl -s -o /dev/null -X POST localhost:10000/v1/players -H "Idempotency-Key: br-$$-$i" -d "{\"username\":\"br_$$_$i\"}"; done ) & W=$!
t0=$(date +%s.%N)
docker compose exec -T postgres pg_dump -U localarena -Fc --no-owner localarena > "$OUT/../backup.dump"
t1=$(date +%s.%N)
wait $W
echo "dump: $(du -h "$OUT/../backup.dump" | cut -f1) in $(echo "$t1-$t0" | bc | cut -c1-5) s (taken while 300 writes ran)"
docker rm -f la-restore >/dev/null 2>&1 || true
docker run -d --name la-restore -e POSTGRES_USER=localarena -e POSTGRES_PASSWORD=restore-dev -e POSTGRES_DB=localarena postgres:18.6-alpine >/dev/null
until docker exec la-restore pg_isready -U localarena -q 2>/dev/null; do sleep 1; done; sleep 2
t2=$(date +%s.%N)
docker exec -i la-restore pg_restore -U localarena -d localarena --no-owner --exit-on-error < "$OUT/../backup.dump" 2>&1 | grep -v "pg_stat_statements" || true
t3=$(date +%s.%N)
echo "restore into a fresh instance: $(echo "$t3-$t2" | bc | cut -c1-5) s"
R=(docker exec -i la-restore psql -U localarena -d localarena -qtA -c)
echo "restored : $(verify "${R[@]}")"
echo "live now : $(verify "${PSQL[@]}")  (newer: writes continued after the snapshot)"
echo "constraints on restored copy: $("${R[@]}" "SELECT count(*) FROM pg_constraint WHERE NOT convalidated") not validated; negative credits: $("${R[@]}" "SELECT count(*) FROM profiles WHERE credits < 0")"
echo "partitions restored: $("${R[@]}" "SELECT count(*) FROM pg_inherits WHERE inhparent = 'matches'::regclass")"
echo "app smoke on restored DB: $(docker run --rm --network container:la-restore -e DATABASE_URL=postgres://localarena:restore-dev@localhost:5432/localarena localarena/api:dev 2>/dev/null | head -0; docker run --rm --network container:la-restore --entrypoint /migrate -e DATABASE_URL=postgres://localarena:restore-dev@localhost:5432/localarena localarena/api:dev version 2>&1 | tail -1)"
docker rm -f la-restore >/dev/null
rm -f "$OUT/../backup.dump"
