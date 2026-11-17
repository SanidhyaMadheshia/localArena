#!/usr/bin/env bash
# Day-1 limits check for the burner AWS account. Read-only: only describe/get/list calls.
# Prints a PASS / WARN / BLOCKED table. Exit code 1 if anything is BLOCKED.
#   AWS_REGION=us-east-1 infra/aws/00-preflight/preflight.sh
# shellcheck disable=SC2016 # JMESPath backticks
set -uo pipefail

REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$REGION" AWS_PAGER=""
PRIMARY_TYPE="${INSTANCE_TYPE:-m7i.large}"
FALLBACKS=(m6i.large m5.large)
MIN_VCPU=12 GOOD_VCPU=24

rows=()
blocked=0
# add <status> <check> <detail> [optional]: an optional BLOCKED row is shown but does not fail the run.
add() {
  rows+=("$1|$2|$3${4:+ (optional)}")
  [[ $1 == BLOCKED && -z ${4:-} ]] && blocked=1
  return 0
}
denied() { grep -qiE 'AccessDenied|UnauthorizedOperation|not authorized|OptInRequired|SubscriptionRequired|explicit deny' <<<"$1"; }

if ! command -v aws >/dev/null; then
  echo "aws CLI not found. Install v2 (https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)." >&2
  exit 2
fi

# 1. identity
if out=$(aws sts get-caller-identity --output json 2>&1); then
  arn=$(jq -r .Arn <<<"$out" 2>/dev/null || sed -n 's/.*"Arn": "\(.*\)".*/\1/p' <<<"$out")
  acct=$(jq -r .Account <<<"$out" 2>/dev/null || sed -n 's/.*"Account": "\(.*\)".*/\1/p' <<<"$out")
  if [[ $arn == *":root" ]]; then
    add WARN "identity" "$arn (root: EKS access entries do not accept root; create an IAM user with AdministratorAccess)"
  else
    add PASS "identity" "$arn (account $acct)"
  fi
else
  add BLOCKED "identity" "sts get-caller-identity failed: ${out//$'\n'/ }"
  printf '%s\n' "${rows[@]}" | column -t -s'|'
  exit 1
fi

# 2. region enabled
if out=$(aws ec2 describe-availability-zones --query 'AvailabilityZones[?State==`available`].ZoneName' --output text 2>&1); then
  add PASS "region $REGION" "usable, AZs: ${out//$'\t'/ }"
  mapfile -t AZS < <(tr '\t' '\n' <<<"$out")
else
  if denied "$out"; then add BLOCKED "region $REGION" "denied (region disabled or SCP): ${out//$'\n'/ }"; else add BLOCKED "region $REGION" "${out//$'\n'/ }"; fi
  AZS=()
fi

# 3. vCPU quota L-1216C47A (Running On-Demand Standard A,C,D,H,I,M,R,T,Z)
if out=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query 'Quota.Value' --output text 2>&1); then
  v=${out%.*}
  if ((v >= GOOD_VCPU)); then add PASS "vCPU quota L-1216C47A" "$v vCPUs (>= $GOOD_VCPU)"
  elif ((v >= MIN_VCPU)); then add WARN "vCPU quota L-1216C47A" "$v vCPUs: enough for 6 x 2 vCPU nodes, not for upgrade surge or add-broker drills. Request an increase to $GOOD_VCPU"
  else add BLOCKED "vCPU quota L-1216C47A" "$v vCPUs < $MIN_VCPU. Request an increase before applying 20-eks"; fi
else
  if denied "$out"; then add WARN "vCPU quota L-1216C47A" "cannot read quotas (denied); check Service Quotas console"; else add WARN "vCPU quota L-1216C47A" "${out//$'\n'/ }"; fi
fi

# 4. instance type offering per AZ, with fallback suggestions
offered_in() { # type -> AZ names offering it
  aws ec2 describe-instance-type-offerings --location-type availability-zone \
    --filters "Name=instance-type,Values=$1" --query 'InstanceTypeOfferings[].Location' --output text 2>&1
}
if out=$(offered_in "$PRIMARY_TYPE"); then
  missing=()
  for az in "${AZS[@]:0:3}"; do grep -qw "$az" <<<"$out" || missing+=("$az"); done
  if ((${#AZS[@]} == 0)); then
    add WARN "$PRIMARY_TYPE offering" "AZ list unavailable"
  elif ((${#missing[@]} == 0)); then
    add PASS "$PRIMARY_TYPE offering" "offered in ${AZS[*]:0:3}"
  else
    sugg=""
    for t in "${FALLBACKS[@]}"; do
      o=$(offered_in "$t") || continue
      ok=1
      for az in "${AZS[@]:0:3}"; do grep -qw "$az" <<<"$o" || ok=0; done
      ((ok)) && { sugg="$t"; break; }
    done
    if [[ -n $sugg ]]; then add WARN "$PRIMARY_TYPE offering" "missing in ${missing[*]}; use instance_type=$sugg"
    else add BLOCKED "$PRIMARY_TYPE offering" "missing in ${missing[*]} and no fallback (${FALLBACKS[*]}) in all 3 AZs"; fi
  fi
else
  if denied "$out"; then add BLOCKED "$PRIMARY_TYPE offering" "denied: ${out//$'\n'/ }"; else add WARN "$PRIMARY_TYPE offering" "${out//$'\n'/ }"; fi
fi

# 5. service permissions via cheap describe/list calls
probe() { # label cmd...   (MSK is only needed for experiment 015, so it is optional)
  local label=$1 out opt=""
  [[ $label == MSK* ]] && opt=1
  shift
  if out=$("$@" 2>&1); then add PASS "$label" "allowed"
  elif denied "$out"; then add BLOCKED "$label" "AccessDenied: $(head -c 160 <<<"${out//$'\n'/ }")" "$opt"
  else add WARN "$label" "$(head -c 160 <<<"${out//$'\n'/ }")"; fi
}
probe "EKS (list-clusters)" aws eks list-clusters
probe "NAT (describe-nat-gateways)" aws ec2 describe-nat-gateways --max-items 1
probe "MSK (list-clusters-v2)" aws kafka list-clusters-v2 --max-results 1
probe "ECR (describe-repositories)" aws ecr describe-repositories --max-results 1
probe "IAM (list-roles)" aws iam list-roles --max-items 1
probe "S3 (list-buckets)" aws s3api list-buckets
probe "Secrets Manager (list)" aws secretsmanager list-secrets --max-results 1
probe "KMS (list-keys)" aws kms list-keys --limit 1

# 6. Elastic IP headroom (NAT needs one; default quota is 5)
if out=$(aws ec2 describe-addresses --query 'length(Addresses)' --output text 2>&1); then
  if ((out >= 4)); then add WARN "Elastic IPs" "$out in use (default limit 5)"; else add PASS "Elastic IPs" "$out in use"; fi
fi

echo
echo "Preflight for region $REGION"
{
  echo "STATUS|CHECK|DETAIL"
  printf '%s\n' "${rows[@]}"
} | column -t -s'|'
echo
if ((blocked)); then echo "RESULT: BLOCKED. Fix the BLOCKED rows before scripts/aws-up.sh."; exit 1; fi
echo "RESULT: OK (review WARN rows)."
