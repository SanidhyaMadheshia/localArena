#!/usr/bin/env bash
# Tear everything down and print a clean/dirty report.
#   scripts/aws-teardown.sh [--yes] [--purge]
# Order: LoadBalancer Services and Ingresses -> PVCs/PVs -> 20-eks -> 10-network -> leftover scan.
# --purge also deletes leftover tagged EBS volumes (StorageClass reclaimPolicy is Retain, so they
# survive PVC deletion on purpose). Without it they are only listed.
# shellcheck disable=SC2016 # JMESPath backticks
set -uo pipefail
# shellcheck source=scripts/aws-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/aws-lib.sh"

ASSUME_YES=
PURGE=
while (($#)); do
  case $1 in
    --yes | -y) ASSUME_YES=1; shift ;;
    --purge) PURGE=1; shift ;;
    -h | --help) sed -n '2,7p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
export ASSUME_YES
need aws; need tofu

if [[ -z $ASSUME_YES ]]; then
  read -r -p "Destroy EKS cluster '$CLUSTER_NAME' and the VPC in $AWS_REGION? [y/N] " a
  [[ $a == y || $a == Y ]] || exit 1
fi

VPC_ID=$(tf_out "$AWS_INFRA/10-network" vpc_id)
K=(kubectl --context "$CLUSTER_NAME" --request-timeout=20s)

# 1. Kubernetes-created AWS resources (load balancers, volumes) block VPC deletion.
if command -v kubectl >/dev/null && "${K[@]}" get ns >/dev/null 2>&1; then
  log "deleting LoadBalancer Services and Ingresses"
  "${K[@]}" get svc -A -o json |
    jq -r '.items[] | select(.spec.type=="LoadBalancer") | "\(.metadata.namespace) \(.metadata.name)"' |
    while read -r ns name; do "${K[@]}" -n "$ns" delete svc "$name" --wait=false; done
  "${K[@]}" delete ingress -A --all --wait=false 2>/dev/null
  log "deleting PVCs and PVs"
  "${K[@]}" delete pvc -A --all --wait=false 2>/dev/null
  # Strimzi and CNPG finalizers hold PVCs while pods run; remove their workloads first.
  "${K[@]}" delete kafka,kafkanodepool -A --all --wait=false 2>/dev/null
  "${K[@]}" delete cluster.postgresql.cnpg.io -A --all --wait=false 2>/dev/null
  "${K[@]}" delete pvc -A --all --wait=false 2>/dev/null
  "${K[@]}" delete pv --all --wait=false 2>/dev/null
  log "waiting up to 5 min for PVCs and LB services to disappear"
  for _ in $(seq 30); do
    pvcs=$("${K[@]}" get pvc -A --no-headers 2>/dev/null | wc -l)
    lbs=$("${K[@]}" get svc -A -o json | jq '[.items[] | select(.spec.type=="LoadBalancer")] | length')
    ((pvcs == 0 && lbs == 0)) && break
    sleep 10
  done
else
  log "cluster not reachable with context $CLUSTER_NAME; skipping in-cluster cleanup"
fi

# 2. Platform state belongs to the cluster that is about to vanish.
log "destroying 20-eks"
tf_destroy "$AWS_INFRA/20-eks" -var 'admin_cidrs=["203.0.113.1/32"]' -var 'admin_principal_arn=arn:aws:iam::000000000000:user/placeholder' || log "20-eks destroy failed"
rm -f "$AWS_INFRA"/platform/terraform.tfstate "$AWS_INFRA"/platform/terraform.tfstate.backup
log "destroying 10-network"
tf_destroy "$AWS_INFRA/10-network" -var "region=$AWS_REGION" -var "name=$CLUSTER_NAME" || log "10-network destroy failed"

# 3. ECR repos are not in OpenTofu (created by aws-up.sh); delete with --force.
for r in "${ECR_REPOS[@]}"; do
  aws ecr delete-repository --repository-name "$ECR_PREFIX/$r" --force >/dev/null 2>&1 && log "deleted ECR $ECR_PREFIX/$r"
done

# 4. Leftover scan.
dirty=0
report() { # label count detail
  if (($2 > 0)); then printf 'DIRTY  %-22s %s\n%s\n' "$1" "$2" "$3"; dirty=1; else printf 'clean  %-22s 0\n' "$1"; fi
}
echo
echo "== leftover report ($AWS_REGION)"

vols=$(aws ec2 describe-volumes --filters "Name=tag-key,Values=kubernetes.io/cluster/$CLUSTER_NAME,ebs.csi.aws.com/cluster" \
  --query 'Volumes[].[VolumeId,State,Size]' --output text)
if [[ -n $PURGE && -n $vols ]]; then
  while read -r id state _; do
    [[ $state == available ]] && aws ec2 delete-volume --volume-id "$id" && log "deleted volume $id"
  done <<<"$vols"
  vols=$(aws ec2 describe-volumes --filters "Name=tag-key,Values=kubernetes.io/cluster/$CLUSTER_NAME,ebs.csi.aws.com/cluster" \
    --query 'Volumes[].[VolumeId,State,Size]' --output text)
fi
report "EBS volumes" "$(grep -c . <<<"$vols")" "$vols"

lbs=$(aws elbv2 describe-load-balancers --query "LoadBalancers[?VpcId=='${VPC_ID:-none}'].[LoadBalancerArn]" --output text 2>/dev/null)
lbs+=$(aws elb describe-load-balancers --query "LoadBalancerDescriptions[?VPCId=='${VPC_ID:-none}'].[LoadBalancerName]" --output text 2>/dev/null)
report "load balancers" "$(grep -c . <<<"$lbs")" "$lbs"

if [[ -n $VPC_ID ]]; then
  enis=$(aws ec2 describe-network-interfaces --filters "Name=vpc-id,Values=$VPC_ID" --query 'NetworkInterfaces[].[NetworkInterfaceId,Description]' --output text)
  nats=$(aws ec2 describe-nat-gateways --filter "Name=vpc-id,Values=$VPC_ID" --query 'NatGateways[?State!=`deleted`].[NatGatewayId,State]' --output text)
else
  enis="" nats=""
  log "no VPC id from 10-network state; ENI and NAT scan skipped"
fi
report "ENIs" "$(grep -c . <<<"$enis")" "$enis"
report "NAT gateways" "$(grep -c . <<<"$nats")" "$nats"

eips=$(aws ec2 describe-addresses --filters "Name=tag:project,Values=localarena" --query 'Addresses[].[AllocationId,PublicIp]' --output text)
report "Elastic IPs" "$(grep -c . <<<"$eips")" "$eips"

ecr=$(aws ecr describe-repositories --query "repositories[?starts_with(repositoryName, '$ECR_PREFIX/')].repositoryName" --output text)
report "ECR repos" "$(tr '\t' '\n' <<<"$ecr" | grep -c .)" "$ecr"

clusters=$(aws eks list-clusters --query "clusters[?@=='$CLUSTER_NAME']" --output text)
report "EKS cluster" "$(grep -c . <<<"$clusters")" "$clusters"

echo
if ((dirty)); then echo "RESULT: DIRTY (see above; --purge deletes available tagged EBS volumes)"; exit 1; fi
echo "RESULT: CLEAN"
