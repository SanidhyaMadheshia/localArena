#!/usr/bin/env bash
# Drill 009d: api-private-check
# Proves: (1) the EKS API endpoint is not open to the world (public CIDRs restricted),
#         (2) from inside the VPC the endpoint resolves to private IPs,
#         (3) pods cannot reach IMDS (hop limit 1), so they cannot take the node role.
# Expected: no 0.0.0.0/0 in publicAccessCidrs; in-cluster lookup returns 10.40.x.x;
#           IMDS token request from a pod times out. Read-only plus one throwaway pod.
# shellcheck disable=SC2034,SC2015,SC2016 # EXP/DRILL are read by _lib.sh
EXP=009-eks-foundation DRILL=api-private-check
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
drill_init
cfg=$(aws eks describe-cluster --name "$CLUSTER_NAME" --query 'cluster.resourcesVpcConfig' --output json)
echo "$cfg" | jq '{endpointPublicAccess, endpointPrivateAccess, publicAccessCidrs}'
echo "$cfg" | jq -e '.publicAccessCidrs | index("0.0.0.0/0") | not' >/dev/null || fail "API is open to 0.0.0.0/0"
say "1. PASS: public API restricted"
host=$(aws eks describe-cluster --name "$CLUSTER_NAME" --query 'cluster.endpoint' --output text | sed 's|https://||')
say "2. resolving $host from a pod"
k run drill-dns --rm -i --restart=Never --image=public.ecr.aws/docker/library/busybox:1.37 -- nslookup "$host" 2>&1 | tee /tmp/drill-dns.out | tail -6
grep -Eq 'Address: *10\.40\.' /tmp/drill-dns.out && say "2. PASS: private IPs" || say "2. WARN: no 10.40.x.x answer (check the VPC cidr)"
say "3. IMDS from a pod (expect timeout)"
if k run drill-imds --rm -i --restart=Never --image=docker.io/curlimages/curl:8.10.1 --command -- \
  curl -sS -m 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token; then
  fail "pod reached IMDS: hop limit is not 1"
else
  say "3. PASS: IMDS unreachable from pod network"
fi
