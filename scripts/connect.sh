#!/usr/bin/env bash
# Create/update the Debezium outbox connector (idempotent PUT) and show status.
set -euo pipefail
cd "$(dirname "$0")/.."
URL=${CONNECT_URL:-http://localhost:8083}
curl -sf -X PUT "$URL/connectors/outbox/config" -H 'Content-Type: application/json' \
  --data @connect/outbox-connector.json >/dev/null
for _ in $(seq 30); do
  s=$(curl -s "$URL/connectors/outbox/status")
  [ "$(jq -r '.tasks[0].state // empty' <<<"$s")" = RUNNING ] && break
  sleep 1
done
jq -c '{connector: .connector.state, task: .tasks[0].state, trace: (.tasks[0].trace // "" | .[0:300])}' <<<"$s"
