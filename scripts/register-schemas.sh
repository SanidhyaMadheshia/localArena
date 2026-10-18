#!/usr/bin/env bash
# Register event contracts in Apicurio with BACKWARD compatibility (idempotent).
#   scripts/register-schemas.sh            register/update schemas/…/events.proto
#   scripts/register-schemas.sh --check F  dry-run: would proto file F be accepted?
# Exit code 0 = compatible/registered, 1 = rejected. Used by CI (week 7).
set -euo pipefail
cd "$(dirname "$0")/.."
REG=${REGISTRY_URL:-http://localhost:8086/apis/registry/v3}
GROUP=localarena
ARTIFACT=localarena.events.v1.Envelope
FILE=schemas/localarena/events/v1/events.proto
DRY=false
if [ "${1:-}" = --check ]; then DRY=true; FILE=$2; fi

body() { jq -Rs --arg id "$ARTIFACT" '{artifactId:$id, artifactType:"PROTOBUF",
  firstVersion:{content:{content:., contentType:"application/x-protobuf"}}}' < "$FILE"; }

code=$(curl -s -o /dev/null -w '%{http_code}' "$REG/groups/$GROUP/artifacts/$ARTIFACT")
if [ "$code" = 404 ]; then
  $DRY && { echo "no artifact yet: anything is compatible"; exit 0; }
  curl -sf -X POST "$REG/groups/$GROUP/artifacts" -H 'Content-Type: application/json' -d "$(body)" >/dev/null
  # Compatibility is checked against all previous versions, not only the latest.
  curl -sf -X POST "$REG/groups/$GROUP/artifacts/$ARTIFACT/rules" -H 'Content-Type: application/json' \
    -d '{"ruleType":"COMPATIBILITY","config":"BACKWARD_TRANSITIVE"}' >/dev/null
  curl -sf -X POST "$REG/groups/$GROUP/artifacts/$ARTIFACT/rules" -H 'Content-Type: application/json' \
    -d '{"ruleType":"VALIDITY","config":"FULL"}' >/dev/null
  echo "registered $ARTIFACT v1 (BACKWARD_TRANSITIVE, VALIDITY FULL)"
  exit 0
fi

# Idempotent: identical content to the latest version is a no-op.
if ! $DRY && curl -sf "$REG/groups/$GROUP/artifacts/$ARTIFACT/versions/branch=latest/content" | cmp -s - "$FILE"; then
  echo "unchanged: latest version already has this content"; exit 0
fi
resp=$(curl -s -w '\n%{http_code}' -X POST "$REG/groups/$GROUP/artifacts/$ARTIFACT/versions?dryRun=$DRY" \
  -H 'Content-Type: application/json' \
  -d "$(jq -Rs '{content:{content:., contentType:"application/x-protobuf"}}' < "$FILE")")
status=$(tail -1 <<<"$resp"); json=$(sed '$d' <<<"$resp")
if [ "$status" -lt 300 ]; then
  if $DRY; then echo "compatible (dry run, nothing registered)"; else echo "registered version $(jq -r .version <<<"$json")"; fi
else
  echo "REJECTED ($status): $(jq -r '.detail // .title // .' <<<"$json" | head -c 600)"
  exit 1
fi
