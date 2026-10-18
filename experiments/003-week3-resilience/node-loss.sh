#!/usr/bin/env bash
# Node-loss drill (Week 3 on the 2-worker kind cluster): stop the worker node
# that hosts the CNPG primary (a whole "machine" disappears: primary, maybe an
# API pod, maybe the pooler/Valkey), while a pod on the *other* worker writes
# through Service "api" every 250 ms (4/s: below the 5/s per-IP account-creation
# rate limit, which otherwise shows up as 429 "failures"). Measures write outage.
#   experiments/003-week3-resilience/node-loss.sh
set -euo pipefail
NS=localarena
DURATION=${DURATION:-150}
OUT=experiments/003-week3-resilience/results/node-loss.txt
exec > >(tee "$OUT") 2>&1
PRIMARY=$(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.currentPrimary}')
NODE=$(kubectl -n $NS get pod "$PRIMARY" -o jsonpath='{.spec.nodeName}')
OTHER=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name | sed 's|node/||' | grep -vx "$NODE" | head -1)
echo "primary $PRIMARY on $NODE; writer pinned to $OTHER"
kubectl -n $NS get pods -o wide --no-headers | awk -v n="$NODE" '$7==n{print "  on victim node: "$1}'
kubectl -n $NS delete pod node-loss-writer --ignore-not-found >/dev/null
kubectl -n $NS run node-loss-writer --image=busybox:1.37.0 --restart=Never \
  --overrides="{\"spec\":{\"nodeName\":\"$OTHER\"}}" --command -- sh -c "
  end=\$((\$(date +%s) + $DURATION)); i=0
  while [ \$(date +%s) -lt \$end ]; do
    i=\$((i+1))
    k=nl_\$(date +%s)_\$i
    if wget -q -T 2 -O /dev/null 2>/dev/null --header \"Idempotency-Key: \$k\" --post-data=\"{\\\"username\\\":\\\"\$k\\\"}\" http://api:8081/v1/players; then r=ok; else r=fail; fi
    echo \"\$(date +%s) \$r\"; sleep 0.25
  done" >/dev/null
kubectl -n $NS wait --for=condition=Ready pod/node-loss-writer --timeout=60s >/dev/null
sleep 10
T0=$(date +%s); echo "stopping node container $NODE at $T0"
docker stop "$NODE" >/dev/null
for _ in $(seq 60); do
  p=$(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.currentPrimary}' 2>/dev/null || true)
  [ -n "$p" ] && [ "$p" != "$PRIMARY" ] && { echo "new primary $p after $(( $(date +%s) - T0 )) s"; break; }
  sleep 2
done
kubectl -n $NS wait --for=jsonpath='{.status.phase}'=Succeeded pod/node-loss-writer --timeout=$((DURATION + 60))s >/dev/null || true
kubectl -n $NS logs node-loss-writer > /tmp/node-loss-writer.log
kubectl -n $NS delete pod node-loss-writer >/dev/null
awk -v t0="$T0" '{n[$2]++} $2=="fail"{if(!f)f=$1; l=$1} END{printf "writes ok=%d failed=%d\n", n["ok"], n["fail"];
  if(f) printf "write outage: %d s (from +%d s to +%d s after node stop)\n", l-f+1, f-t0, l-t0}' /tmp/node-loss-writer.log
echo "restarting $NODE"; docker start "$NODE" >/dev/null
kubectl wait --for=condition=Ready node/"$NODE" --timeout=180s >/dev/null
for _ in $(seq 60); do
  [ "$(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.phase}')" = "Cluster in healthy state" ] && break; sleep 5
done
echo "after recovery: $(kubectl -n $NS get cluster arena-db -o jsonpath='{.status.phase}; primary={.status.currentPrimary}; ready={.status.readyInstances}')"
kubectl -n $NS get pods -o wide --no-headers | awk '{print "  "$1, $3, $7}'
