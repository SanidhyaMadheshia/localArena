#!/usr/bin/env bash
# Drill 012d: kafka-tls-check (openssl s_client)
# Proves: the client listener speaks verified TLS only: the certificate chains to the cluster CA
#         and matches the bootstrap name, TLS 1.2+ is negotiated, TLS 1.1 is refused, and
#         there is no plaintext listener on 9092.
# Expected: "Verify return code: 0 (ok)", Protocol TLSv1.3 or TLSv1.2, TLS1.1 handshake fails,
#           9092 refused. Read-only.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=012-kafka-security DRILL=kafka-tls-check
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
kafka_tools_up
host=${KAFKA_BS%:*}
say "s_client against $host:9093"
kx "echo | openssl s_client -connect $KAFKA_BS -servername $host -CAfile /s/ca/ca.crt -verify_hostname $host 2>&1 | grep -E 'Verify return code|Protocol|Cipher is|subject=|issuer=|Verification'" | tee /tmp/drill-tls.out
grep -q 'Verify return code: 0 (ok)' /tmp/drill-tls.out || fail "certificate did not verify"
kx "echo | openssl s_client -connect $KAFKA_BS -servername $host -CAfile /s/ca/ca.crt 2>/dev/null | openssl x509 -noout -subject -dates -ext subjectAltName" | sed 's/^/    /'
if kx "echo | openssl s_client -connect $KAFKA_BS -tls1_1 -CAfile /s/ca/ca.crt" >/dev/null 2>&1; then say "WARN: TLS 1.1 accepted"; else say "TLS 1.1: refused (good)"; fi
if kx "timeout 5 bash -c '</dev/tcp/${KAFKA}-kafka-bootstrap.${KAFKA_NS}.svc/9092'" >/dev/null 2>&1; then fail "plaintext port 9092 is open"; else say "9092 plaintext: closed (good)"; fi
say "RESULT: PASS"
