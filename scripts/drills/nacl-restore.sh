#!/usr/bin/env bash
# Emergency restore after an interrupted NACL isolation drill.
#   scripts/drills/nacl-restore.sh experiments/011-kafka-resilience/results/nacl-restore-<ts>.state
# Safe to run any time: it only puts each subnet back on its original network ACL.
set -euo pipefail
# shellcheck source=scripts/drills/_lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
NACL_STATE=${1:?usage: $0 <state-file>}
[[ -f $NACL_STATE ]] || fail "no such state file: $NACL_STATE"
nacl_restore
