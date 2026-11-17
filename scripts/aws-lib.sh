#!/usr/bin/env bash
# Shared helpers for scripts/aws-*.sh. Source it, do not run it.
# shellcheck shell=bash disable=SC2034

AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION" AWS_PAGER=""
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AWS_INFRA="$ROOT/infra/aws"
CLUSTER_NAME="${CLUSTER_NAME:-localarena}" # must match `name` in 10-network
ECR_REPOS=(api gameserver bots streams)
ECR_PREFIX="${ECR_PREFIX:-localarena}" # repos: localarena/api, ... (matches values-eks.yaml)

log() { printf '%s [%s] %s\n' "$(date +%H:%M:%S)" "${0##*/}" "$*"; }
die() { log "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

# tf_apply <dir> [extra tofu args...]. Honors $ASSUME_YES.
tf_apply() {
  local dir=$1
  shift
  tofu -chdir="$dir" init -input=false >/dev/null
  tofu -chdir="$dir" apply -input=false ${ASSUME_YES:+-auto-approve} "$@"
}

tf_destroy() {
  local dir=$1
  shift
  tofu -chdir="$dir" init -input=false >/dev/null
  tofu -chdir="$dir" destroy -input=false ${ASSUME_YES:+-auto-approve} "$@"
}

# tf_out <dir> <output>; prints nothing when the stack has no state.
tf_out() { tofu -chdir="$1" output -raw "$2" 2>/dev/null || true; }
