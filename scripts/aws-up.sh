#!/usr/bin/env bash
# Day-1 bring-up: preflight -> network -> eks -> kubeconfig + gp3 -> platform -> ECR.
#   scripts/aws-up.sh [--step network|eks|platform|ecr|all] [--yes]
# Env: AWS_REGION (default us-east-1), ADMIN_CIDR (default: your IP /32 via ifconfig.me),
#      ADMIN_PRINCIPAL_ARN (default: derived from sts get-caller-identity),
#      TF_EXTRA_EKS (extra tofu args for 20-eks, e.g. "-var=instance_type=m6i.large"),
#      SKIP_PREFLIGHT=1.
set -euo pipefail
# shellcheck source=scripts/aws-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/aws-lib.sh"

STEP=all
ASSUME_YES=
while (($#)); do
  case $1 in
    --step) STEP=${2:?--step needs a value}; shift 2 ;;
    --yes | -y) ASSUME_YES=1; shift ;;
    -h | --help) sed -n '2,8p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
case $STEP in network | eks | platform | ecr | all) ;; *) die "bad --step: $STEP" ;; esac
want() { [[ $STEP == all || $STEP == "$1" ]]; }

need aws; need tofu; need kubectl; need curl
export ASSUME_YES

preflight() {
  [[ -n ${SKIP_PREFLIGHT:-} ]] && return 0
  log "preflight"
  "$AWS_INFRA/00-preflight/preflight.sh"
}

admin_cidr() {
  if [[ -n ${ADMIN_CIDR:-} ]]; then echo "$ADMIN_CIDR"; return; fi
  local ip
  ip=$(curl -fsS --max-time 10 https://ifconfig.me) || die "cannot detect public IP; set ADMIN_CIDR=x.x.x.x/32"
  [[ $ip =~ ^[0-9.]+$ ]] || die "ifconfig.me returned '$ip'; set ADMIN_CIDR"
  echo "$ip/32"
}

# EKS access entries need an IAM user or role ARN, not an assumed-role session ARN.
admin_principal() {
  if [[ -n ${ADMIN_PRINCIPAL_ARN:-} ]]; then echo "$ADMIN_PRINCIPAL_ARN"; return; fi
  local arn acct
  arn=$(aws sts get-caller-identity --query Arn --output text)
  acct=$(aws sts get-caller-identity --query Account --output text)
  case $arn in
    *:root) die "root identity cannot be an EKS access entry. Create an IAM user with AdministratorAccess, or set ADMIN_PRINCIPAL_ARN" ;;
    *:assumed-role/*)
      local role=${arn#*:assumed-role/}
      echo "arn:aws:iam::${acct}:role/${role%%/*}" ;;
    *) echo "$arn" ;;
  esac
}

do_network() {
  log "10-network"
  tf_apply "$AWS_INFRA/10-network" -var "region=$AWS_REGION" -var "name=$CLUSTER_NAME"
}

do_eks() {
  log "20-eks"
  local cidr principal
  cidr=$(admin_cidr)
  principal=$(admin_principal)
  log "admin_cidrs=$cidr admin_principal_arn=$principal"
  # shellcheck disable=SC2086 # TF_EXTRA_EKS is intentionally word-split
  tf_apply "$AWS_INFRA/20-eks" -var "admin_cidrs=[\"$cidr\"]" -var "admin_principal_arn=$principal" ${TF_EXTRA_EKS:-}
  kubeconfig
  log "gp3 StorageClass"
  kubectl --context "$CLUSTER_NAME" apply -f "$AWS_INFRA/manifests/gp3-storageclass.yaml"
  if kubectl --context "$CLUSTER_NAME" get storageclass gp2 >/dev/null 2>&1; then
    kubectl --context "$CLUSTER_NAME" annotate storageclass gp2 storageclass.kubernetes.io/is-default-class- --overwrite
  fi
  kubectl --context "$CLUSTER_NAME" get nodes -L topology.kubernetes.io/zone,workload
}

kubeconfig() {
  local name
  name=$(tf_out "$AWS_INFRA/20-eks" cluster_name)
  [[ -n $name ]] || die "20-eks has no state; run --step eks first"
  aws eks update-kubeconfig --region "$AWS_REGION" --name "$name" --alias "$name"
}

do_platform() {
  log "platform (infra/opentofu via infra/aws/platform; own state, no imports.tf)"
  kubeconfig
  # manage_app=false: Argo CD or helm owns the app. insecure_kubelet_tls=false: EKS kubelet certs are valid.
  tf_apply "$AWS_INFRA/platform" -var "kube_context=$CLUSTER_NAME" -var manage_app=false -var insecure_kubelet_tls=false
}

do_ecr() {
  log "ECR repositories"
  local r
  for r in "${ECR_REPOS[@]}"; do
    aws ecr describe-repositories --repository-names "$ECR_PREFIX/$r" >/dev/null 2>&1 ||
      aws ecr create-repository --repository-name "$ECR_PREFIX/$r" \
        --image-scanning-configuration scanOnPush=true --encryption-configuration encryptionType=AES256 >/dev/null
    log "ready: $ECR_PREFIX/$r"
  done
}

next_steps() {
  local acct
  acct=$(aws sts get-caller-identity --query Account --output text)
  cat <<EOF

Next steps
  1. Push images:   aws ecr get-login-password | docker login --username AWS --password-stdin $acct.dkr.ecr.$AWS_REGION.amazonaws.com
                    (tag as $acct.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_PREFIX/{api,gameserver,bots,streams}:<tag>)
  2. Kafka:         kubectl --context $CLUSTER_NAME apply -k infra/kafka/overlays/eks
  3. App:           helm upgrade --install localarena infra/helm/localarena -n localarena \\
                      -f infra/helm/localarena/values-eks.yaml --set image.registry=$acct.dkr.ecr.$AWS_REGION.amazonaws.com
  4. Drills:        DRILL_CONFIRM=1 scripts/drills/<name>.sh   (see docs/building-blocks/aws-drills.md)
  5. Overnight:     scripts/aws-scale-nodes.sh 0     # back: scripts/aws-scale-nodes.sh 1
  6. Done:          scripts/aws-teardown.sh --yes
EOF
}

preflight
want network && do_network
want eks && do_eks
want platform && do_platform
want ecr && do_ecr
[[ $STEP == all ]] && next_steps
log "done ($STEP)"
