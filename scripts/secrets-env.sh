#!/usr/bin/env bash
# Decrypt config/secrets.dev.enc.yaml into .secrets.env (gitignored, mode 600)
# for docker compose `env_file`. Requires the age key in ~/.config/sops/age/keys.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
export SOPS_AGE_KEY_FILE=${SOPS_AGE_KEY_FILE:-$HOME/.config/sops/age/keys.txt}
if [[ -f $SOPS_AGE_KEY_FILE ]] && command -v sops >/dev/null; then
  sops --decrypt --output-type dotenv config/secrets.dev.enc.yaml > .secrets.env
elif [[ -f .secrets.env ]]; then
  echo "keeping existing .secrets.env (no age key to decrypt the repo secrets)"; exit 0
else
  # New contributor without the maintainer's age key: generate fresh random
  # dev secrets. Tickets/webhooks only need both sides to share them.
  printf 'GAME_TICKET_SECRET=%s\nWEBHOOK_SIGNING_SECRET=%s\n' "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" > .secrets.env
  echo "generated random .secrets.env (no age key found at $SOPS_AGE_KEY_FILE)"
fi
echo "wrote .secrets.env ($(wc -l < .secrets.env) keys)"
