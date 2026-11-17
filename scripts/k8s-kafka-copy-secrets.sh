#!/usr/bin/env bash
# Copy KafkaUser secrets and the cluster CA cert from namespace `data` (where the
# Strimzi User Operator writes them) into the app namespace. Idempotent.
# Usage: scripts/k8s-kafka-copy-secrets.sh [src-ns=data] [dst-ns=localarena]
# Env: KUBECTL_CONTEXT (optional kubectl context), CLUSTER (default localarena)
set -euo pipefail
SRC=${1:-data}; DST=${2:-localarena}; CLUSTER=${CLUSTER:-localarena}
K=(kubectl); [[ -n ${KUBECTL_CONTEXT:-} ]] && K+=(--context "$KUBECTL_CONTEXT")

"${K[@]}" get ns "$DST" >/dev/null 2>&1 || "${K[@]}" create ns "$DST"

names=$("${K[@]}" -n "$SRC" get secret -l strimzi.io/kind=KafkaUser,strimzi.io/cluster="$CLUSTER" -o name)
names+=$'\n'"secret/${CLUSTER}-cluster-ca-cert"
n=0
for s in $names; do
  [[ -z $s ]] && continue
  "${K[@]}" -n "$SRC" get "$s" -o json \
    | jq --arg ns "$DST" '{apiVersion, kind, type, data,
        metadata: {name: .metadata.name, namespace: $ns,
                   labels: (((.metadata.labels // {}) | del(.["strimzi.io/cluster"], .["strimzi.io/kind"]))
                            + {"app.kubernetes.io/managed-by": "k8s-kafka-copy-secrets"})}}' \
    | "${K[@]}" apply -f - >/dev/null
  n=$((n+1))
done
echo "copied $n secrets from $SRC to $DST"
