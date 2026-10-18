#!/usr/bin/env bash
# Decrypt config/secrets.dev.enc.yaml into .secrets.env (gitignored, mode 600)
# for docker compose `env_file`. Requires the age key in ~/.config/sops/age/keys.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
export SOPS_AGE_KEY_FILE=${SOPS_AGE_KEY_FILE:-$HOME/.config/sops/age/keys.txt}
sops --decrypt --output-type dotenv config/secrets.dev.enc.yaml > .secrets.env
echo "wrote .secrets.env ($(wc -l < .secrets.env) keys)"
