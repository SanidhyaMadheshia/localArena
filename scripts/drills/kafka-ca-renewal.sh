#!/usr/bin/env bash
# Drill 012c: kafka-ca-renewal
# Proves: Strimzi renews the cluster CA with a rolling restart while clients that re-read the
#         CA secret keep working, and exposes the operational risk: clients that pinned the OLD
#         CA copy (scripts/k8s-kafka-copy-secrets.sh output) fail until the copy is refreshed.
# Expected: CA fingerprint changes; fresh-secret client OK after the roll; stale-copy client
#           FAILS the TLS handshake (PKIX / certificate verify failed) once brokers present
#           certs from the new CA. Pins ca.crt may hold both certs during the overlap: then the
#           stale client still passes and the script says so.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=012-kafka-security DRILL=kafka-ca-renewal
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
kafka_tools_up
fp() { kk get secret "$KAFKA-cluster-ca-cert" -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -fingerprint -sha256 -enddate; }
say "CA before:"; fp
kx "cp /tmp/admin.properties /tmp/stale.properties"   # snapshot of the old CA, pinned inline
load_start 100 900
t0=$SECONDS
kk annotate secret "$KAFKA-cluster-ca-cert" strimzi.io/force-renew=true --overwrite
say "force-renew annotated; waiting for rolling restart"
sleep 30
wait_until 2400 "kafka Ready after CA renewal" kafka_ready
say "T_renewal=$((SECONDS - t0))s"
say "CA after:"; fp
say "client with fresh secret:"
kx "tr '\n' ' ' </s/ca/ca.crt | grep -o 'BEGIN CERTIFICATE' | wc -l" | sed 's/^/    certificates in ca.crt: /'
kcmd kafka-topics.sh --list >/dev/null && say "fresh client: OK" || say "fresh client: FAIL"
say "client with stale (pre-renewal) CA copy:"
if kx "bin/kafka-topics.sh --bootstrap-server $KAFKA_BS --command-config /tmp/stale.properties --list" >/dev/null 2>&1; then
  say "stale client: still OK (old CA still trusted/overlap)"
else
  say "stale client: FAILED handshake as predicted. Refresh copies: scripts/k8s-kafka-copy-secrets.sh and restart app pods"
fi
load_wait 900
load_report
