#!/usr/bin/env bash
# Failover RPO/RTO drill on the Compose async replica (destroys the replica's
# role; the script rebuilds both containers' state at the end).
#   1. writes run against the primary; each committed id is logged as "acked"
#   2. a replication delay is simulated: the replica stops receiving WAL
#      (network disconnect) for LAG seconds
#   3. the primary "dies" (docker kill) and the replica is promoted
#   4. RPO = acked writes missing on the new primary; RTO = kill -> first
#      successful write on the new primary
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/006-week6-observability/results; mkdir -p "$OUT"
exec > >(tee "$OUT/failover-rpo.txt") 2>&1
LAG=${LAG:-5}
P=(docker compose exec -T postgres psql -U localarena -d localarena -qtA -c)
R=(docker compose exec -T postgres-replica psql -U localarena -d localarena -qtA -c)
"${P[@]}" "CREATE TABLE IF NOT EXISTS drill_writes (id bigint PRIMARY KEY, at timestamptz DEFAULT now())" >/dev/null
"${P[@]}" "TRUNCATE drill_writes" >/dev/null; sleep 2
writer() { # one autocommit insert per 20 ms; prints the id only after COMMIT returned
  docker compose exec -T postgres sh -c 'i=0; while :; do i=$((i+1)); psql -U localarena -d localarena -qtA -c "INSERT INTO drill_writes(id) VALUES ($i)" >/dev/null 2>&1 && echo $i || exit 0; sleep 0.02; done'
}
writer > /tmp/acked.txt & WP=$!
sleep 5
echo "replica sync before fault: $("${P[@]}" "SELECT state || ' replay_lag=' || coalesce(replay_lag::text,'0') FROM pg_stat_replication")"
RNET=$(docker inspect localarena-postgres-replica-1 --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
if [ "$LAG" -gt 0 ]; then
  docker network disconnect "$RNET" localarena-postgres-replica-1   # replica stops receiving WAL
  echo "replica partitioned for ${LAG}s while writes continue"
  sleep "$LAG"
fi
T0=$(date +%s.%N)
docker kill localarena-postgres-1 >/dev/null; kill $WP 2>/dev/null || true
echo "primary killed at +0 s"
[ "$LAG" -gt 0 ] && docker network connect "$RNET" localarena-postgres-replica-1
docker compose exec -T postgres-replica psql -U localarena -d localarena -qtA -c "SELECT pg_promote(wait => true)" >/dev/null
T1=$(date +%s.%N)
until "${R[@]}" "INSERT INTO drill_writes(id) VALUES (-1)" >/dev/null 2>&1; do sleep 0.2; done
T2=$(date +%s.%N)
acked=$(wc -l < /tmp/acked.txt); maxack=$(tail -1 /tmp/acked.txt)
present=$("${R[@]}" "SELECT count(*) FROM drill_writes WHERE id > 0")
maxpresent=$("${R[@]}" "SELECT max(id) FROM drill_writes WHERE id > 0")
echo "acknowledged writes: $acked (last id $maxack); on promoted replica: $present (last id $maxpresent)"
echo "RPO: $((acked - present)) acknowledged writes lost = $(echo "($maxack - $maxpresent) * 0.02" | bc) s of writes (async replication + ${LAG}s partition)"
echo "RTO (DB only): promote $(echo "$T1-$T0" | bc | cut -c1-5) s, first write accepted $(echo "$T2-$T0" | bc | cut -c1-5) s after the kill"
# Restore the original topology: re-seed both from the promoted node's data is
# heavyweight; for the lab, discard the replica and rebuild it from the old primary.
docker compose stop postgres-replica >/dev/null
docker compose start postgres >/dev/null; sleep 5
docker volume rm -f localarena_pg-replica >/dev/null 2>&1 || (docker compose rm -sf postgres-replica >/dev/null && docker volume rm -f localarena_pg-replica >/dev/null)
docker compose up -d postgres-replica >/dev/null 2>&1; sleep 15
echo "topology restored: old primary back as primary (its writes intact), replica re-cloned: $("${P[@]}" "SELECT state FROM pg_stat_replication" | head -1)"
"${P[@]}" "DROP TABLE drill_writes" >/dev/null
