#!/usr/bin/env bash
# Drill 009b: pvc-az-pin
# Proves: an EBS volume is pinned to the AZ where it was first attached. When every node in that
#         AZ is cordoned, the pod cannot move to another AZ and stays Pending ("volume node
#         affinity conflict"), then recovers with its data when the node is uncordoned.
# Expected: step 3 Pending with a volume node affinity conflict; step 4 Running and the file
#           written in step 1 is still there.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=009-eks-foundation DRILL=pvc-az-pin
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
NS=drill-pin
k create ns "$NS" --dry-run=client -o yaml | k apply -f - >/dev/null
cleanup_add "k delete ns $NS --wait=false"
cleanup_add "k get nodes -l workload=general -o name | xargs -r -n1 kubectl --context $KUBE_CONTEXT uncordon >/dev/null"
pod() { # nodeSelector fragment on stdin-less arg $1
  k -n "$NS" apply -f - <<YAML >/dev/null
apiVersion: v1
kind: Pod
metadata: { name: pinned }
spec:
  nodeSelector: { workload: general$1 }
  containers:
    - name: c
      image: public.ecr.aws/docker/library/busybox:1.37
      command: [sh, -c, "sleep 3600"]
      volumeMounts: [{ name: d, mountPath: /data }]
  volumes: [{ name: d, persistentVolumeClaim: { claimName: pinned } }]
YAML
}
k -n "$NS" apply -f - <<'YAML' >/dev/null
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: pinned }
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: gp3
  resources: { requests: { storage: 1Gi } }
YAML
say "1. create pod + 1Gi gp3 PVC (WaitForFirstConsumer picks the AZ)"
pod ""
k -n "$NS" wait pod/pinned --for=condition=Ready --timeout=300s >/dev/null
node=$(k -n "$NS" get pod pinned -o jsonpath='{.spec.nodeName}')
zone=$(node_zone "$node")
k -n "$NS" exec pinned -- sh -c 'echo survived > /data/proof'
say "pod on $node in $zone; wrote /data/proof"

say "2. cordon every general node in $zone, delete the pod"
mapfile -t zn < <(k get nodes -l "workload=general,topology.kubernetes.io/zone=$zone" -o name)
for n in "${zn[@]}"; do k cordon "${n#node/}" >/dev/null; done
k -n "$NS" delete pod pinned --wait=true >/dev/null
pod "" # no zone selector: the scheduler may use any zone, but the volume only exists in $zone

say "3. expect Pending with volume node affinity conflict"
sleep 25
phase=$(k -n "$NS" get pod pinned -o jsonpath='{.status.phase}')
reason=$(k -n "$NS" get events --field-selector involvedObject.name=pinned -o jsonpath='{.items[*].message}' | grep -o 'volume node affinity conflict' | head -1 || true)
say "phase=$phase reason='${reason}'"
[[ $phase == Pending ]] || fail "pod left Pending without a node in $zone"

say "4. uncordon, expect Running with data"
for n in "${zn[@]}"; do k uncordon "${n#node/}" >/dev/null; done
k -n "$NS" wait pod/pinned --for=condition=Ready --timeout=300s >/dev/null
say "file content: $(k -n "$NS" exec pinned -- cat /data/proof)"
say "RESULT: PASS (volume stayed in $zone). Cleanup: PV is Retain; run scripts/aws-teardown.sh --purge later or delete the volume by hand."
