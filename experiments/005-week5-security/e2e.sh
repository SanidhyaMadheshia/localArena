#!/usr/bin/env bash
# Experiment 005 end-to-end security checks against the running stack:
#   scripts/secrets-env.sh && scripts/tls-certs.sh
#   docker compose --profile security up -d keycloak opa flagd api gameserver envoy
#   experiments/005-week5-security/e2e.sh
# Prints PASS/FAIL per check and exits non-zero on any failure.
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/005-week5-security/results; mkdir -p "$OUT"
exec > >(tee "$OUT/e2e.txt") 2>&1
KC=http://localhost:8180/realms/localarena/protocol/openid-connect/token
API=https://localhost:8443; CA=(--cacert infra/compose/tls/ca.crt)
fails=0
check() { if [ "$2" = "$3" ]; then echo "PASS $1 ($2)"; else echo "FAIL $1: got $2 want $3"; fails=$((fails+1)); fi; }
token() { curl -s -X POST "$KC" -d grant_type=password -d client_id=localarena-bots -d "username=$1" -d "password=$1-dev-password" | jq -r .access_token; }
code() { curl -s -o /dev/null -w '%{http_code}' "${CA[@]}" "$@"; }

ALICE=$(token alice); BOB=$(token bob); ADMIN=$(token admin1)
check "keycloak issues tokens" "$([ ${#ALICE} -gt 100 ] && echo yes)" yes
check "wrong password refused" "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$KC" -d grant_type=password -d client_id=localarena-bots -d username=alice -d password=nope)" 400 # OAuth2: invalid_grant is 400

# TLS edge
check "TLS 1.2+ with dev CA" "$(code $API/v2/cosmetics)" 200
check "TLS 1.1 refused" "$(curl -s -o /dev/null -w '%{http_code}' "${CA[@]}" --tls-max 1.1 $API/v2/cosmetics 2>/dev/null || echo refused)" "000refused"
check "HSTS on TLS" "$(curl -s -D - -o /dev/null "${CA[@]}" $API/v2/cosmetics | grep -ci strict-transport-security)" 1

# AuthN / AuthZ
check "no token -> 401" "$(code $API/v2/me)" 401
# Tamper with the payload (sub -> attacker) and keep the original signature. (Swapping the
# last base64url character is not a reliable forgery: its low bits may be padding.)
FORGED=$(cut -d. -f1 <<<"$ALICE").$(cut -d. -f2 <<<"$ALICE" | tr '_-' '/+' | base64 -d 2>/dev/null | jq -c '.sub="attacker"' | base64 -w0 | tr '/+' '_-' | tr -d '=').$(cut -d. -f3 <<<"$ALICE")
check "forged token (edited claims) -> 401" "$(code -H "Authorization: Bearer $FORGED" $API/v2/me)" 401
ME=$(curl -s "${CA[@]}" -H "Authorization: Bearer $ALICE" $API/v2/me); ALICE_ID=$(jq -r .profile.player_id <<<"$ME")
check "valid token -> /v2/me" "$(jq -r .roles[0] <<<"$ME")" player
BOB_ID=$(curl -s "${CA[@]}" -H "Authorization: Bearer $BOB" $API/v2/me | jq -r .profile.player_id)
check "player cannot ban" "$(code -X POST -H "Authorization: Bearer $ALICE" -d "{\"player_id\":$BOB_ID,\"reason\":\"x\"}" $API/v2/admin/bans)" 403
check "admin bans bob (1h)" "$(code -X POST -H "Authorization: Bearer $ADMIN" -d "{\"player_id\":$BOB_ID,\"reason\":\"e2e drill\",\"hours\":1}" $API/v2/admin/bans)" 204
check "banned bob, still-valid token: purchase -> 403" "$(code -X POST -H "Authorization: Bearer $BOB" -H "Idempotency-Key: e2e-$RANDOM" -d '{"cosmetic_id":1}' $API/v2/purchases)" 403
check "banned bob: game ticket -> 403" "$(code -X POST -H "Authorization: Bearer $BOB" $API/v2/game-tickets)" 403
docker compose exec -T postgres psql -U localarena -qc "DELETE FROM bans WHERE player_id = $BOB_ID" >/dev/null

# Game ticket -> WebSocket (gameserver checks the HMAC)
TICKET=$(curl -s "${CA[@]}" -X POST -H "Authorization: Bearer $ALICE" $API/v2/game-tickets | jq -r .ticket)
(cd backend && go build -o ../bin/ ./cmd/wscheck)
WS=$(bin/wscheck "ws://localhost:8080/ws?ticket=$TICKET" | cut -d' ' -f1)
check "ticket -> ws welcome as alice" "$WS" "id=$ALICE_ID"
check "forged ticket -> 401" "$(bin/wscheck 'ws://localhost:8080/ws?ticket=AAAA')" status=401

# Edge WAF and global rate limit
check "path traversal rejected (Envoy path normalization, before the WAF)" "$(code "$API/v2/players/..%2f..%2fetc%2fpasswd")" 400
check "WAF: script tag" "$(code "$API/v2/cosmetics?q=%3Cscript%3Ealert(1)%3C/script%3E")" 403
check "WAF: union select" "$(code "$API/v2/cosmetics?id=1%20UNION%20SELECT%20password")" 403
check "oversized headers -> 431 (HTTP/1.1; HTTP/2 resets the stream)" "$(code --http1.1 -H "X-Big: $(head -c 20000 /dev/zero | tr '\0' a)" $API/v2/cosmetics)" 431
# Burst far above the bucket (2000 tokens, +1000/s) using k6 over one listener.
RL=$(k6 run -q --no-color --log-output=none -e URL=http://localhost:10000/healthz - <<'K6' | tail -1
import http from 'k6/http';
export const options = { scenarios: { b: { executor: 'constant-arrival-rate', rate: 4000, timeUnit: '1s', duration: '3s', preAllocatedVUs: 200, maxVUs: 400 } } };
export default function () { http.get(__ENV.URL, { tags: { s: 'x' } }); }
export function handleSummary(d) { return { stdout: JSON.stringify(d.metrics.http_reqs.values.count) + '\n' }; }
K6
)
RL=$(curl -s localhost:9901/stats | awk -F': ' '/edge_rl.http_local_rate_limit.rate_limited/{s+=$2} END{print (s>0)?"limited":"none"}')
check "edge token bucket sheds a 3000-request burst" "$RL" limited

# Flag fallback: stop flagd, matches keep starting as classic
docker compose stop flagd >/dev/null 2>&1
check "flagd down: gameserver still healthy" "$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/healthz)" 200
docker compose start flagd >/dev/null 2>&1

echo "failures: $fails"
exit $fails
