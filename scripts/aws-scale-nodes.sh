#!/usr/bin/env bash
# Scale all managed node groups to N nodes per AZ group (0 = overnight saving).
#   scripts/aws-scale-nodes.sh <N> [--group general|kafka|all]
# The control plane keeps billing at 0 nodes. EBS volumes (PVCs) stay and keep billing too.
# This changes desired size outside OpenTofu; the next `tofu apply` in 20-eks resets it to
# general_nodes_per_az / kafka_nodes_per_az. For a lasting change set those variables.
set -euo pipefail
# shellcheck source=scripts/aws-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/aws-lib.sh"

N=${1:-}
[[ $N =~ ^[0-9]+$ ]] || die "usage: $0 <N> [--group general|kafka|all]"
shift
GROUP=all
while (($#)); do
  case $1 in
    --group) GROUP=${2:?}; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done
need aws

MAX=$((N > 2 ? N : 2))
cluster=${CLUSTER_NAME}
mapfile -t groups < <(aws eks list-nodegroups --cluster-name "$cluster" --query 'nodegroups[]' --output text | tr '\t' '\n')
((${#groups[@]})) || die "no node groups found for cluster $cluster in $AWS_REGION"

for g in "${groups[@]}"; do
  [[ $GROUP == all || $g == "$cluster-$GROUP-"* ]] || continue
  log "$g -> desired=$N"
  aws eks update-nodegroup-config --cluster-name "$cluster" --nodegroup-name "$g" \
    --scaling-config "minSize=0,maxSize=$MAX,desiredSize=$N" --query 'update.id' --output text
done
log "requested; watch with: kubectl get nodes -w   (aws eks describe-nodegroup ... --query nodegroup.status)"
