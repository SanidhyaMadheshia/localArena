#!/usr/bin/env bash
# Experiment 007 rollout drills on kind (Helm release "localarena", Argo CD auto-sync paused).
#   experiments/007-week7-delivery/drills.sh [rolling canary bluegreen failed-migration rollback gameserver-drain]
# Every drill runs a continuous in-cluster load (busybox, ~20 req/s through Service api)
# and reports non-2xx/transport failures during the change.
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/007-week7-delivery/results; mkdir -p "$OUT"
NS=localarena
# --force-conflicts: Argo CD (paused) also owns these fields via server-side apply (finding F1).
CH=(helm upgrade localarena infra/helm/localarena -n $NS -f infra/helm/localarena/values-dev.yaml --reuse-values=false --force-conflicts)
log() { echo "$*" | tee -a "$OUT/$D.txt"; }

load_start() { # $1 = seconds
  kubectl -n $NS delete pod drill-load --ignore-not-found --wait=true >/dev/null
  kubectl -n $NS run drill-load --image=busybox:1.37.0 --restart=Never --command -- sh -c "
    end=\$((\$(date +%s)+$1)); while [ \$(date +%s) -lt \$end ]; do
      for i in 1 2 3 4; do
        (if wget -q -T 3 -O /dev/null http://api:8081/v1/cosmetics 2>/dev/null; then echo ok; else echo fail; fi) &
      done; wait; sleep 0.2; done" >/dev/null
  kubectl -n $NS wait --for=condition=Ready pod/drill-load --timeout=60s >/dev/null
}
load_result() {
  kubectl -n $NS wait --for=jsonpath='{.status.phase}'=Succeeded pod/drill-load --timeout=600s >/dev/null 2>&1
  kubectl -n $NS logs drill-load | sort | uniq -c | awk '{printf "%s=%s ", $2, $1} END{print ""}'
  kubectl -n $NS delete pod drill-load >/dev/null
}
images() { kubectl -n $NS get pods -l app=api -o json | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | "\(.metadata.labels.track):\(.spec.containers[0].image)"' | sort | uniq -c | tr -s ' \n' ' '; echo; }

drill_rolling() {
  "${CH[@]}" --set image.api.tag=v1 --wait --timeout 5m >/dev/null
  log "start: $(images)"
  load_start 90; sleep 5
  t0=$(date +%s)
  "${CH[@]}" --set image.api.tag=v2 --wait --timeout 5m >/dev/null
  log "rolled v1 -> v2 in $(( $(date +%s)-t0 )) s (maxSurge 1, maxUnavailable 0, readiness gate + 5 s drain)"
  log "after: $(images)"
  log "requests during rollout: $(load_result)"
}

drill_canary() {
  "${CH[@]}" --set image.api.tag=v1 --set api.canary.enabled=true --set api.canary.tag=v2 --set api.canary.replicas=1 --wait --timeout 5m >/dev/null
  log "canary: $(images)"
  # Traffic split = per-pod deltas of the cosmetics request counter (only that route's counter line).
  cnt() { for n in $(kubectl -n $NS get pods -l app=api -o json | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name'); do
      echo "$n $(kubectl -n $NS get --raw "/api/v1/namespaces/$NS/pods/$n:8081/proxy/metrics" </dev/null | grep '^localarena_http_requests_total' | grep 'route="GET /v1/cosmetics"' | awk '{s+=$NF} END{print s+0}')"; done | sort; }
  cnt > /tmp/c0
  load_start 40
  log "requests during canary: $(load_result)"
  cnt > /tmp/c1
  join /tmp/c0 /tmp/c1 | awk '{d=$3-$2; t+=d; a[$1]=d} END{for (k in a) printf "  %s %d (%.0f%%)\n", k, a[k], 100*a[k]/t}' | sort | tee -a "$OUT/$D.txt"
  # Promote: stable takes the canary tag, canary removed.
  "${CH[@]}" --set image.api.tag=v2 --set api.canary.enabled=false --wait --timeout 5m >/dev/null
  log "promoted: $(images)"
}

drill_bluegreen() {
  # Green = a full second release of the API only, then a Service selector switch.
  "${CH[@]}" --set image.api.tag=v1 --wait --timeout 5m >/dev/null
  kubectl -n $NS get deploy api -o json | jq '.metadata.name="api-green" | .spec.replicas=2 | .spec.selector.matchLabels.track="green"
    | .spec.template.metadata.labels.track="green" | .spec.template.spec.containers[0].image="localarena/api:v2"
    | del(.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp,.metadata.generation,.metadata.managedFields,.status,.metadata.annotations)' \
    | kubectl apply -f - >/dev/null
  kubectl -n $NS rollout status deploy/api-green --timeout=180s >/dev/null
  log "blue (track=stable, v1) and green (track=green, v2) both running: $(images)"
  load_start 40; sleep 8
  t0=$(date +%s.%N)
  kubectl -n $NS patch svc api --type merge -p '{"spec":{"selector":{"app":"api","track":"green"}}}' >/dev/null
  log "switched Service to green in $(echo "$(date +%s.%N)-$t0" | bc | cut -c1-4) s; instant rollback = patch selector back"
  sleep 12
  kubectl -n $NS patch svc api --type merge -p '{"spec":{"selector":{"app":"api","track":"stable"}}}' >/dev/null
  log "switched back to blue (rollback drill)"
  log "requests during cutover + rollback: $(load_result)"
  kubectl -n $NS delete deploy api-green >/dev/null
  kubectl -n $NS patch svc api --type json -p '[{"op":"remove","path":"/spec/selector/track"}]' >/dev/null
}

drill_failed_migration() {
  local rev0; rev0=$(helm -n $NS history localarena -o json | jq '.[-1].revision')
  load_start 45; sleep 5
  t0=$(date +%s)
  out=$("${CH[@]}" --set image.api.tag=v2 --set migrate.failForDrill=true --rollback-on-failure --wait --timeout 3m 2>&1 | tail -2 | tr '\n' ' ')
  log "upgrade with a failing migration: $(echo "$out" | cut -c1-200)"
  log "  took $(( $(date +%s)-t0 )) s; release history: $(helm -n $NS history localarena -o json | jq -r '.[-3:] | map("\(.revision):\(.status)") | join(" ")')"
  log "  schema unchanged: drill_bad column exists = $(kubectl -n $NS exec arena-db-1 -c postgres -- psql -U postgres -d localarena -qtAc "SELECT count(*) FROM information_schema.columns WHERE table_name='profiles' AND column_name='drill_bad'" 2>/dev/null)"
  log "  pods still serving: $(images)"
  log "  requests during the failed upgrade: $(load_result)"
}

drill_rollback() {
  "${CH[@]}" --set image.api.tag=v1 --wait --timeout 5m >/dev/null
  "${CH[@]}" --set image.api.tag=v2 --wait --timeout 5m >/dev/null
  local prev; prev=$(helm -n $NS history localarena -o json | jq '.[-2].revision')
  load_start 60; sleep 3; t0=$(date +%s)
  helm rollback localarena "$prev" -n $NS --wait --timeout 5m >/dev/null
  log "helm rollback to revision $prev in $(( $(date +%s)-t0 )) s: $(images)"
  log "requests during rollback: $(load_result)"
}

drill_gameserver_drain() { # bots connected; roll the gameserver; players must finish, not be cut
  (cd backend && go build -o ../bin/ ./cmd/bots)
  local gs; gs=$(kubectl -n $NS get pod -l app=gameserver -o jsonpath='{.items[0].metadata.name}')
  kubectl -n $NS port-forward "pod/$gs" 18080:8080 >/dev/null 2>&1 & local PF=$!; sleep 2
  (bin/bots -server http://localhost:18080 -bots 20 -ramp 1s -duration 25s -json > "$OUT/drain-bots.json" 2>/dev/null) & local B=$!
  sleep 5; t0=$(date +%s)
  kubectl -n $NS delete pod "$gs" --wait=false >/dev/null
  sleep 2
  log "old pod readiness after SIGTERM: $(kubectl -n $NS get pod "$gs" -o jsonpath='{.status.containerStatuses[0].ready}') (false = out of the Service, still serving its players)"
  wait $B; kill $PF 2>/dev/null
  for _ in $(seq 60); do kubectl -n $NS get pod "$gs" >/dev/null 2>&1 || break; sleep 1; done
  log "old pod exited $(( $(date +%s)-t0 )) s after SIGTERM (after its players left, < grace ${GRACE:-45} s)"
  log "bots: $(jq -c '{connects,connect_failures,server_drops}' "$OUT/drain-bots.json")"
  log "gameserver log: $(kubectl -n $NS logs "$gs" --previous 2>/dev/null | grep -oE '"msg":"(draining|shutting down)"[^}]*' | tr '\n' ' ' | cut -c1-200)"
}

for D in ${@:-rolling canary bluegreen failed-migration rollback gameserver-drain}; do
  : > "$OUT/$D.txt"; echo "== $D"; "drill_${D//-/_}"
done
