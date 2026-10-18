#!/usr/bin/env bash
# Generate a local dev CA and a server certificate for the Envoy TLS edge
# (localhost, envoy, api). Idempotent: keeps existing files. Keys are gitignored.
# Trust the CA for curl with: curl --cacert infra/compose/tls/ca.crt https://localhost:8443/...
set -euo pipefail
cd "$(dirname "$0")/../infra/compose/tls"
[ -f server.crt ] && { echo "certs exist"; exit 0; }
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 825 -subj "/CN=LocalArena Dev CA" \
  -keyout ca.key -out ca.crt -addext basicConstraints=critical,CA:TRUE -addext keyUsage=critical,keyCertSign,cRLSign 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 397 -out server.crt \
  -extfile <(printf 'subjectAltName=DNS:localhost,DNS:envoy,DNS:api,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature') 2>/dev/null
rm -f server.csr ca.srl
chmod 600 ca.key
chmod 644 server.key # read by the envoy user inside the container; lab-only key, gitignored
echo "generated CA and server certificate"
