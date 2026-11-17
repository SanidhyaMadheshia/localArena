#!/usr/bin/env bash
# Drill 012a: kafka-bad-password
# Proves: SCRAM authentication really gates the listener: a wrong password and an unknown user
#         are both rejected, and the broker logs the failure.
# Expected: both attempts fail with SaslAuthenticationException; the control (admin) succeeds;
#           broker logs contain "Authentication failed".
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=012-kafka-security DRILL=kafka-bad-password
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
kafka_tools_up
try() { # props-file label expect(ok|deny)
  local out rc=0
  out=$(kx "bin/kafka-topics.sh --bootstrap-server $KAFKA_BS --command-config $1 --list 2>&1") || rc=$?
  if [[ $3 == ok ]]; then
    ((rc == 0)) && say "$2: OK (allowed)" || fail "$2 was rejected: $(tail -3 <<<"$out")"
  else
    ((rc != 0)) && grep -q 'SaslAuthenticationException\|Authentication failed' <<<"$out" && say "$2: DENIED (as expected)" || fail "$2 was not rejected"
  fi
}
try /tmp/admin.properties "control: admin, right password" ok
kafka_props_pw "$KAFKA-admin" "definitely-wrong" /tmp/bad.properties
try /tmp/bad.properties "wrong password" deny
kafka_props_pw "no-such-user" "x" /tmp/ghost.properties
try /tmp/ghost.properties "unknown user" deny
say "broker log evidence:"
for p in $(kk get pods -l "strimzi.io/cluster=$KAFKA,strimzi.io/kind=Kafka" -o name); do
  kk logs "$p" --since=5m | grep -i 'authentication failed' | tail -2 | sed "s|^|    $p: |"
done
say "RESULT: PASS"
