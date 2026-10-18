#!/usr/bin/env bash
# Failover drill: write continuously through Service "api" from inside the
# cluster (kube-proxy balances across API replicas), delete the CNPG primary
# pod, and measure the write outage. Requires the k8s stack deployed.
# kubectl port-forward is NOT used: it pins one pod and dies with it.
set -euo pipefail
NS=localarena
DURATION=${DURATION:-60}
OLD=$(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.currentPrimary}')
echo "primary before: $OLD"
kubectl -n $NS delete pod failover-writer --ignore-not-found >/dev/null
# One write every 100 ms; prints "<unix-seconds> ok|fail".
kubectl -n $NS run failover-writer --image=busybox:1.37.0 --restart=Never --command -- sh -c "
  end=\$((\$(date +%s) + $DURATION)); i=0
  while [ \$(date +%s) -lt \$end ]; do
    i=\$((i+1))
    if wget -q -T 2 -O /dev/null --post-data='{\"username\":\"fo_'\$(date +%s)_\$i'\"}' http://api:8081/v1/players; then r=ok; else r=fail; fi
    echo \"\$(date +%s) \$r\"; sleep 0.1
  done" >/dev/null
kubectl -n $NS wait --for=condition=Ready pod/failover-writer --timeout=60s >/dev/null
sleep 10
echo "deleting $OLD at $(date +%s)"
kubectl -n $NS delete pod "$OLD" --wait=false >/dev/null
kubectl -n $NS wait --for=jsonpath='{.status.phase}'=Succeeded pod/failover-writer --timeout=$((DURATION + 60))s >/dev/null
kubectl -n $NS logs failover-writer > /tmp/failover-writer.log
kubectl -n $NS delete pod failover-writer >/dev/null
echo "primary after: $(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.currentPrimary}')"
awk '{n[$2]++} $2=="fail"{if(!f)f=$1; l=$1} END{printf "writes ok=%d failed=%d\n", n["ok"], n["fail"]; if(f) printf "write outage window: %d s (first fail %d, last fail %d)\n", l-f+1, f, l}' /tmp/failover-writer.log
