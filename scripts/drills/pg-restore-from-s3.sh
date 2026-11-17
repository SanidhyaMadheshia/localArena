#!/usr/bin/env bash
# Drill 013c: pg-restore-from-s3
# Proves: a CNPG base backup + WAL archive in S3 (written through Pod Identity, no static keys)
#         restores into a NEW cluster (arena-db-restore) with the data intact: RTO measured.
# Expected: Backup phase=completed; restore cluster Ready in a few minutes; the marker row
#           inserted before the backup is present. Needs 20-eks applied (bucket + Pod Identity
#           for service accounts arena-db and arena-db-restore).
# NOTE: patches spec.backup on arena-db (not in the chart yet). Argo CD selfHeal would revert it.
#       Uses the in-tree barmanObjectStore (deprecated since CNPG 1.26, removed in 1.32): on a
#       newer operator switch to the barman-cloud plugin.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=013-data-resilience DRILL=pg-restore-from-s3
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
bucket=$(tofu -chdir="$AWS_INFRA/20-eks" output -raw pg_backup_bucket)
[[ -n $bucket ]] || fail "no pg_backup_bucket output from 20-eks"
C="k -n $APP_NS"
primary=$($C get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.status.currentPrimary}')
image=$($C get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.spec.imageName}')
psql() { $C exec "$1" -c postgres -- psql -d localarena -Atqc "$2"; }

if [[ -z $($C get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.spec.backup.barmanObjectStore.destinationPath}') ]]; then
  say "enabling backup to s3://$bucket/arena-db"
  $C patch cluster.postgresql.cnpg.io arena-db --type merge -p "{\"spec\":{\"backup\":{\"retentionPolicy\":\"7d\",\"barmanObjectStore\":{\"destinationPath\":\"s3://$bucket/\",\"s3Credentials\":{\"inheritFromIAMRole\":true},\"wal\":{\"compression\":\"gzip\"}}}}}"
  archiving() { [[ $($C get cluster.postgresql.cnpg.io arena-db -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].status}') == True ]]; }
  wait_until 600 "WAL archiving working" archiving
fi
marker="drill-$(date +%s)"
psql "$primary" "CREATE TABLE IF NOT EXISTS drill_marker(note text, at timestamptz DEFAULT now()); INSERT INTO drill_marker(note) VALUES ('$marker');"
say "marker $marker written on $primary"
t0=$SECONDS
$C apply -f - <<YAML >/dev/null
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: { name: drill-backup-$(date +%s) }
spec: { cluster: { name: arena-db } }
YAML
bname=$($C get backup -o name --sort-by=.metadata.creationTimestamp | tail -1)
done_() { [[ $($C get "$bname" -o jsonpath='{.status.phase}') == completed ]]; }
wait_until 900 "backup completed" done_
say "T_backup=$((SECONDS - t0))s"
psql "$primary" "SELECT pg_switch_wal();" >/dev/null
sleep 30 # let the last WAL segment reach S3

say "restoring into arena-db-restore"
cleanup_add "$C delete cluster.postgresql.cnpg.io arena-db-restore --ignore-not-found --wait=false"
t1=$SECONDS
$C apply -f - <<YAML >/dev/null
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: { name: arena-db-restore }
spec:
  instances: 1
  imageName: $image
  storage: { size: 5Gi, storageClass: gp3 }
  bootstrap: { recovery: { source: arena-db } }
  externalClusters:
    - name: arena-db
      barmanObjectStore:
        destinationPath: s3://$bucket/
        serverName: arena-db
        s3Credentials: { inheritFromIAMRole: true }
YAML
ready() { [[ $($C get cluster.postgresql.cnpg.io arena-db-restore -o jsonpath='{.status.phase}') == "Cluster in healthy state" ]]; }
wait_until 1800 "restore cluster healthy" ready
say "T_restore_RTO=$((SECONDS - t1))s"
got=$(psql arena-db-restore-1 "SELECT count(*) FROM drill_marker WHERE note='$marker';")
[[ $got == 1 ]] && say "RESULT: PASS (marker present in restored cluster)" || fail "marker missing after restore (count=$got)"
