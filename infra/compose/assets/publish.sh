#!/usr/bin/env bash
# Publish game assets to Garage with content-hashed (immutable) names and a manifest.
#   infra/compose/assets/publish.sh [asset-dir]   (default: web/assets)
# Idempotent: unchanged files keep their hash and URL; changed files get a new URL.
set -euo pipefail
cd "$(dirname "$0")/../../.."
SRC=${1:-web/assets}
G=(docker compose exec -T -e RUST_LOG=error garage /garage)

# One-time: bucket, key, website access (all idempotent).
"${G[@]}" bucket create assets >/dev/null 2>&1 || true
"${G[@]}" bucket website --allow assets >/dev/null
if ! "${G[@]}" key info assets-publisher >/dev/null 2>&1; then "${G[@]}" key create assets-publisher >/dev/null; fi
"${G[@]}" bucket allow --read --write assets --key assets-publisher >/dev/null
KEY=$("${G[@]}" key info assets-publisher --show-secret)
AK=$(awk '/Key ID:/{print $3}' <<<"$KEY")
SK=$(awk '/Secret key:/{print $3}' <<<"$KEY")

aws() { # pinned AWS CLI container on the Compose network; credentials via env only
  docker run --rm -i --network localarena_default -v "$PWD/$SRC:/src:ro" \
    -e AWS_ACCESS_KEY_ID="$AK" -e AWS_SECRET_ACCESS_KEY="$SK" -e AWS_DEFAULT_REGION=garage \
    amazon/aws-cli:2.31.18 --endpoint-url http://garage:3900 "$@"
}

# A 2 MB sample replay is generated deterministically rather than committed.
[ -f "$SRC/replay-sample.bin" ] || head -c 2000000 /dev/zero | openssl enc -aes-128-ctr -K 00 -iv 00 -nosalt 2>/dev/null > "$SRC/replay-sample.bin"

manifest="{"
sep=""
for f in "$SRC"/*; do
  [ -f "$f" ] || continue
  base=$(basename "$f"); name=${base%.*}; ext=${base##*.}
  hash=$(sha256sum "$f" | cut -c1-12)
  key="assets/$name.$hash.$ext"
  ctype=$(file -b --mime-type "$f"); [ "$ext" = json ] && ctype=application/json; [ "$ext" = svg ] && ctype=image/svg+xml
  if ! aws s3api head-object --bucket assets --key "$key" >/dev/null 2>&1; then
    aws s3 cp "/src/$base" "s3://assets/$key" --content-type "$ctype" --only-show-errors
    echo "uploaded $key"
  fi
  manifest+="$sep\"$base\":\"/$key\""; sep=","
done
manifest+="}"
echo "$manifest" | aws s3 cp - s3://assets/assets/manifest.json --content-type application/json --only-show-errors
echo "manifest: $manifest"
