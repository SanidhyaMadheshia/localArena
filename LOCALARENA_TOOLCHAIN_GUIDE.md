# LocalArena Toolchain Setup and Daily Usage Guide

This guide documents the local toolchain installed for the **LocalArena multiplayer backend and event-streaming project**. It explains how the machine was configured, how to reproduce the setup, and the commands most often used during development.

- Host: Ubuntu 24.04.5 LTS, x86-64
- Toolchain snapshot: 2026-10-18
- Project plan: `~/projects/Learning/LOCALARENA_8_WEEK_PLAN.md`
- Detailed version report: `~/.config/localarena/toolchain-versions.md`
- No cloud account is required.

## 1. Architecture and language policy

LocalArena is **Go-first**:

- Use Go for game servers, game loops, HTTP/gRPC APIs, matchmaking, bots, Kafka producers and ordinary consumers, workers, tests, and utilities.
- Use Java/Kotlin only for processors that specifically require the **Kafka Streams JVM library**.
- Run Kafka, PostgreSQL, Valkey, MinIO, Envoy, Keycloak, and observability software as project-scoped containers or Helm releases—not global host services.

```text
Host tools
├── Go toolchain and quality/security tools
├── Java 21 + Gradle for Kafka Streams only
├── Docker Desktop and Docker Compose
├── kubectl + kind + Helm
├── OpenTofu
├── k6
├── SOPS + age
└── protoc + grpcurl

Project services
├── Go game/API processes
├── Kafka and Kafka Streams processors
├── PostgreSQL, Valkey, and MinIO
└── OpenTelemetry and observability stack
```

## 2. Installed versions

| Tool | Installed version/configuration |
|---|---|
| Docker Desktop | 4.95.0-242831 |
| Docker client / engine | 29.9.1 / 29.8.2 |
| Docker Compose | 5.6.0 |
| Docker resources | 16 CPUs, 8192 MiB RAM, 2048 MiB swap |
| kubectl | 1.37.1 |
| kind | 0.33.0 |
| Kubernetes | 1.37.0 |
| Helm | 4.3.1 |
| Go | 1.27.2 |
| Java/Javac | 21.0.12.1 |
| Gradle | 9.8.1 |
| Node.js / npm | 24.13.0 / 11.6.2 |
| OpenTofu | 1.13.1 |
| k6 | 2.3.0 |
| SOPS | 3.13.3 |
| age | 1.3.2 |
| protoc | 36.2 |
| grpcurl | 1.9.4 |
| Trivy | 0.76.0 |
| golangci-lint | 2.15.0 |
| gosec | 2.29.0 |
| govulncheck | 1.9.0 |
| protoc-gen-go | 1.36.12 |
| protoc-gen-go-grpc | 1.6.2 |

User-installed executables are under `~/.local/bin`. The active Go installation is `~/.local/share/go-toolchains/go1.27.2`.

## 3. Daily start and health check

Docker Desktop normally starts with the graphical session. If it is not running:

```bash
docker desktop start
docker desktop status
```

Select the LocalArena Kubernetes context and inspect the cluster:

```bash
kubectl config use-context kind-localarena
kubectl get nodes
kubectl get pods -A
```

Expected nodes:

```text
localarena-control-plane   Ready
localarena-worker          Ready
localarena-worker2         Ready
```

Check important versions:

```bash
docker version
docker compose version
kubectl version
kind version
helm version
go version
java -version
gradle --version
tofu version
k6 version
```

A normal future project startup will look like:

```bash
cd ~/projects/localarena
docker compose up -d
docker compose ps
docker compose logs -f
```

Only use the last commands after the LocalArena repository and its Compose file exist.

---

# Part I — Reproducing the setup

## 4. Host prerequisites

Verify architecture, virtualization, memory, and disk:

```bash
uname -m
grep -Eoc '(vmx|svm)' /proc/cpuinfo
ls -l /dev/kvm
free -h
df -h / /home
```

The installed machine has approximately 15 GiB host RAM and hardware virtualization. Docker Desktop requires KVM on Linux.

Install baseline packages:

```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates curl jq git make unzip \
  openjdk-21-jdk
```

Create the user-local binary path if needed:

```bash
mkdir -p ~/.local/bin
case ":$PATH:" in
  *":$HOME/.local/bin:"*) ;;
  *) echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc ;;
esac
source ~/.bashrc
```

## 5. Docker Desktop setup

The downloaded installer used on this machine is:

```bash
bash ~/Downloads/install-docker-desktop.sh
```

That script:

1. Adds Docker's Ubuntu apt repository and signing key.
2. Installs `~/Downloads/docker-desktop-amd64.deb`.
3. Adds the current user to the `kvm` group.
4. Installs GNOME tray integration when available.

After installation, log out and back in so group membership takes effect. Open Docker Desktop once and accept the Docker Subscription Service Agreement. Check its licensing terms for your usage; Docker Desktop is not itself a fully open-source product.

Enable and start the user service:

```bash
systemctl --user enable --now docker-desktop
systemctl --user is-enabled docker-desktop
systemctl --user is-active docker-desktop
```

In Docker Desktop, open **Settings → Resources → Advanced**, set:

```text
Memory: 8192 MiB
Swap:   2048 MiB
```

Apply and restart. Verify the engine-visible allocation:

```bash
docker info --format 'CPUs={{.NCPU}} MemoryBytes={{.MemTotal}}'
```

The prior resource values are backed up at:

```text
~/.config/localarena/docker-desktop-resources.before.json
```

Run the basic test:

```bash
docker run --rm hello-world
```

## 6. Go setup

Go 1.27.2 was installed user-locally to avoid the outdated system copy:

```bash
GO_VERSION=1.27.2
ARCHIVE="$HOME/Downloads/go${GO_VERSION}.linux-amd64.tar.gz"
DEST="$HOME/.local/share/go-toolchains/go${GO_VERSION}"

mkdir -p "$DEST" ~/.local/bin
tar -xzf "$ARCHIVE" -C "$DEST" --strip-components=1
ln -sfn "$DEST/bin/go" ~/.local/bin/go
ln -sfn "$DEST/bin/gofmt" ~/.local/bin/gofmt
hash -r
go version
go env GOROOT GOVERSION
```

For a fresh download, obtain the archive from `https://go.dev/dl/` and verify its published SHA-256 before extraction.

Pinned Go helper tools were installed with:

```bash
export GOBIN="$HOME/.local/bin"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
```

## 7. kubectl and kind setup

Install kubectl v1.37.1 with checksum verification:

```bash
VERSION=v1.37.1
curl -fLO "https://dl.k8s.io/release/${VERSION}/bin/linux/amd64/kubectl"
curl -fLO "https://dl.k8s.io/release/${VERSION}/bin/linux/amd64/kubectl.sha256"
echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
install -m 0755 kubectl ~/.local/bin/kubectl
rm kubectl kubectl.sha256
```

Install kind v0.33.0:

```bash
VERSION=v0.33.0
curl -fLo kind "https://github.com/kubernetes-sigs/kind/releases/download/${VERSION}/kind-linux-amd64"
curl -fLo kind.sha256sum "https://github.com/kubernetes-sigs/kind/releases/download/${VERSION}/kind-linux-amd64.sha256sum"
EXPECTED="$(awk '{print $1}' kind.sha256sum)"
echo "$EXPECTED  kind" | sha256sum --check
install -m 0755 kind ~/.local/bin/kind
rm kind kind.sha256sum
```

The cluster configuration is `~/.config/kind/localarena.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: localarena
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

The project copy is `infra/kubernetes/kind/cluster.yaml` (two workers, so database
instances and API replicas can be spread across nodes). `make k8s-cluster`
recreates the cluster from it (destructive).

Create the cluster:

```bash
kind create cluster \
  --name localarena \
  --config ~/.config/kind/localarena.yaml \
  --wait 5m

kubectl config use-context kind-localarena
kubectl wait --for=condition=Ready nodes --all --timeout=180s
```

## 8. Other user-local tools

These tools were downloaded from their official release pages, their SHA-256 checksum or GitHub asset digest was verified, and their binaries were installed to `~/.local/bin`:

```text
Helm 4.3.1
OpenTofu 1.13.1
k6 2.3.0
SOPS 3.13.3
age 1.3.2
protoc 36.2
grpcurl 1.9.4
Trivy 0.76.0
golangci-lint 2.15.0
gosec 2.29.0
```

General safe installation pattern:

```bash
# 1. Pin an exact release; do not blindly use an unversioned URL.
VERSION='<exact-version>'

# 2. Download the Linux amd64 asset and the publisher's checksum file.
curl --retry 3 -fLo tool.tar.gz '<official-release-asset-url>'
curl --retry 3 -fLo checksums.txt '<official-checksum-url>'

# 3. Verify before extracting.
grep 'tool.tar.gz$' checksums.txt | sha256sum --check

# 4. Extract and install for the current user.
tar -xzf tool.tar.gz
install -m 0755 ./tool ~/.local/bin/tool
```

Archive layouts differ, so consult each official release page before applying the generic pattern. Upgrade one tool at a time and rerun its smoke test.

## 9. Gradle setup

Gradle 9.8.1 was installed under `~/.local/share/gradle/gradle-9.8.1` and linked into `~/.local/bin`:

```bash
GRADLE_VERSION=9.8.1
mkdir -p ~/.local/share/gradle ~/.local/bin
unzip "gradle-${GRADLE_VERSION}-bin.zip" -d ~/.local/share/gradle
ln -sfn \
  "$HOME/.local/share/gradle/gradle-${GRADLE_VERSION}/bin/gradle" \
  ~/.local/bin/gradle
gradle --version
```

Verify the SHA-256 published by `services.gradle.org` before extracting. Use this host installation to create a wrapper; thereafter use `./gradlew` inside the project.

---

# Part II — Everyday command guide

## 10. Docker Desktop

Purpose: manages the Linux VM and Docker Engine used by containers and kind.

```bash
# Status and lifecycle
docker desktop status
docker desktop start
docker desktop stop
docker desktop restart

# User service
systemctl --user status docker-desktop
systemctl --user restart docker-desktop

# Diagnostics/logs
docker desktop diagnose check
docker desktop logs --help
```

Stopping Docker Desktop also stops access to the kind node containers. They normally recover when Desktop starts again.

## 11. Docker Engine

Purpose: builds images and runs individual containers.

```bash
# Inspect
docker info
docker ps
docker ps -a
docker images
docker inspect <container>
docker stats

# Images
docker pull postgres:18
docker build -t localarena/api:dev .
docker image history localarena/api:dev
docker image rm <image>

# Containers
docker run --rm hello-world
docker run --name demo -d -p 8080:80 nginx:alpine
docker logs -f demo
docker exec -it demo sh
docker stop demo
docker rm demo

# Networks and volumes
docker network ls
docker volume ls
docker volume inspect <volume>
```

Resource-limited example:

```bash
docker run --rm \
  --cpus 0.5 \
  --memory 256m \
  alpine:latest \
  sh -c 'echo constrained-container'
```

Cleanup commands are destructive to unused Docker objects; inspect before running:

```bash
docker system df
docker container prune
docker image prune
docker volume prune       # Be especially careful: data may be lost.
docker system prune       # Does not remove volumes unless --volumes is added.
```

## 12. Docker Compose

Purpose: runs the local multi-service application stack.

Minimal `compose.yaml` example:

```yaml
services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: localarena
      POSTGRES_PASSWORD: local-only-password
      POSTGRES_DB: localarena
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U localarena"]
      interval: 5s
      timeout: 3s
      retries: 10

volumes:
  postgres-data:
```

Common commands:

```bash
docker compose config
docker compose pull
docker compose build
docker compose up -d
docker compose ps
docker compose logs -f
docker compose logs -f postgres
docker compose exec postgres psql -U localarena
docker compose restart postgres
docker compose stop
docker compose down
docker compose down -v    # Destructive: removes declared volumes/data.
```

Use profiles to avoid running Kafka and observability continuously:

```yaml
services:
  kafka-ui:
    profiles: ["streaming"]
    image: <pinned-kafka-ui-image>
```

```bash
docker compose --profile streaming up -d
```

## 13. kind

Purpose: runs Kubernetes nodes as Docker containers.

```bash
kind version
kind get clusters
kind create cluster --name localarena \
  --config ~/.config/kind/localarena.yaml \
  --wait 5m
kind export kubeconfig --name localarena
kind load docker-image localarena/api:dev --name localarena
kind delete cluster --name localarena   # Destructive: deletes the cluster.
```

Local image workflow:

```bash
docker build -t localarena/api:dev .
kind load docker-image localarena/api:dev --name localarena
```

For locally loaded images, use `imagePullPolicy: IfNotPresent` or `Never` in development manifests.

## 14. kubectl

Purpose: deploys, inspects, and troubleshoots Kubernetes resources.

```bash
# Context and cluster
kubectl config get-contexts
kubectl config current-context
kubectl config use-context kind-localarena
kubectl cluster-info
kubectl get nodes -o wide

# Resources
kubectl get namespaces
kubectl get pods -A
kubectl get deploy,svc,ingress -n localarena
kubectl describe pod <pod> -n localarena
kubectl get events -n localarena --sort-by=.metadata.creationTimestamp

# Apply and remove
kubectl apply -f infra/kubernetes/
kubectl diff -f infra/kubernetes/
kubectl delete -f infra/kubernetes/

# Logs and process access
kubectl logs <pod> -n localarena
kubectl logs -f <pod> -n localarena
kubectl logs <pod> -c <container> --previous -n localarena
kubectl exec -it <pod> -n localarena -- sh

# Port forwarding
kubectl port-forward service/api 8080:8080 -n localarena

# Scaling and rollout
kubectl scale deployment/api --replicas=3 -n localarena
kubectl rollout status deployment/api -n localarena
kubectl rollout history deployment/api -n localarena
kubectl rollout undo deployment/api -n localarena

# Wait for health
kubectl wait --for=condition=Ready pod -l app=api \
  -n localarena --timeout=120s
```

Quick scheduling/DNS test:

```bash
kubectl run dns-smoke \
  --image=busybox:1.37.0 \
  --restart=Never \
  --command -- sh -c \
  'nslookup kubernetes.default.svc.cluster.local && echo PASS'

kubectl wait pod/dns-smoke \
  --for=jsonpath='{.status.phase}'=Succeeded \
  --timeout=120s
kubectl logs dns-smoke
kubectl delete pod dns-smoke
```

## 15. Helm

Purpose: templates, installs, upgrades, and rolls back Kubernetes packages.

```bash
helm version
helm repo add <name> <repository-url>
helm repo update
helm search repo <keyword>
helm list -A

# Local chart development
helm create infra/helm/localarena
helm lint infra/helm/localarena
helm template localarena infra/helm/localarena \
  --namespace localarena \
  --values infra/helm/localarena/values-dev.yaml

# Install/upgrade
helm upgrade --install localarena infra/helm/localarena \
  --namespace localarena \
  --create-namespace \
  --values infra/helm/localarena/values-dev.yaml \
  --wait --timeout 5m

# History and rollback
helm history localarena -n localarena
helm rollback localarena <revision> -n localarena --wait
helm uninstall localarena -n localarena
```

Always run `helm lint` and `helm template` before applying a chart.

## 16. Go

Purpose: primary LocalArena implementation language.

```bash
# Module lifecycle
mkdir localarena && cd localarena
go mod init example.com/localarena
go get <module>@<exact-version>
go mod tidy
go mod verify
go list -m all

# Format, build, and run
gofmt -w .
go vet ./...
go build ./...
go run ./cmd/api

# Tests
go test ./...
go test -race ./...
go test -count=1 ./...
go test -cover ./...
go test -run TestMatchTick ./internal/engine
go test -bench=. -benchmem ./internal/engine

# Detect dependencies requiring updates
go list -m -u all
```

Profiles:

```bash
# If the service exposes net/http/pprof on localhost:6060
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
go tool pprof http://localhost:6060/debug/pprof/heap
```

Useful concurrency checks:

```bash
go test -race ./...
go test -run TestName -count=100 ./internal/engine
```

Pin dependencies in `go.mod`; do not use unreviewed `@latest` updates in the project.

## 17. Go quality and security tools

```bash
# Aggregated linting
golangci-lint run ./...
golangci-lint run --fix ./...   # Review all automatic changes.

# Go security patterns; generated protobuf code is excluded.
gosec -exclude-generated ./...

# Reachable known vulnerabilities
govulncheck ./...
```

Recommended CI order:

```bash
gofmt -w .
go vet ./...
go test -race ./...
golangci-lint run ./...
gosec -quiet -exclude-generated ./...
govulncheck ./...
```

A vulnerability finding should be fixed by updating the affected Go patch version or dependency, then rerunning tests and `govulncheck`—not by suppressing it without analysis.

## 18. Protocol Buffers and gRPC

Purpose: defines typed internal APIs and events.

Example `api/echo.proto`:

```proto
syntax = "proto3";
package localarena.echo.v1;
option go_package = "example.com/localarena/gen/echo/v1;echov1";

message PingRequest { string value = 1; }
message PingReply { string value = 1; }

service EchoService {
  rpc Ping(PingRequest) returns (PingReply);
}
```

Generate Go code:

```bash
mkdir -p gen
protoc \
  -I . \
  -I "$HOME/.local/include" \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  api/echo.proto
```

Inspect a reflection-enabled local gRPC server:

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe localarena.echo.v1.EchoService
grpcurl -plaintext \
  -d '{"value":"hello"}' \
  localhost:50051 \
  localarena.echo.v1.EchoService/Ping
```

Without server reflection:

```bash
grpcurl -plaintext \
  -import-path . \
  -proto api/echo.proto \
  -d '{"value":"hello"}' \
  localhost:50051 \
  localarena.echo.v1.EchoService/Ping
```

Treat `.proto` changes as API changes. Reserve removed field numbers and enforce backward compatibility.

## 19. Java and Gradle

Purpose: builds Kafka Streams applications that cannot use Go's runtime libraries.

```bash
java -version
javac -version
gradle --version
```

Create and use a project wrapper:

```bash
cd streams/leaderboard
gradle wrapper --gradle-version 9.8.1
./gradlew tasks
./gradlew clean test
./gradlew build
./gradlew dependencies
```

Run one test:

```bash
./gradlew test --tests '*LeaderboardTopologyTest'
```

Use Java 21 toolchains in `build.gradle.kts`:

```kotlin
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
```

Commit `gradlew`, `gradlew.bat`, and `gradle/wrapper/`. Do not commit `.gradle/` or `build/`.

## 20. Node.js and npm

Purpose: minimal Canvas/TypeScript browser client—not the main backend.

```bash
node --version
npm --version
npm init -y
npm install --save-exact <package>@<version>
npm install
npm ci
npm run dev
npm test
npm run build
npm audit
```

Commit `package-lock.json`. Use `npm ci` in CI for deterministic installation.

## 21. GNU Make

Purpose: provides memorable aliases for multi-tool workflows.

Example `Makefile`:

```makefile
.PHONY: test lint up down cluster-status

test:
	go test -race ./...

lint:
	golangci-lint run ./...

up:
	docker compose up -d

down:
	docker compose down

cluster-status:
	kubectl get nodes
	kubectl get pods -A
```

Commands:

```bash
make test
make lint
make up
make down
make -n up       # Preview commands without executing them.
```

Make recipe indentation must use tabs.

## 22. OpenTofu

Purpose: open-source Terraform-compatible infrastructure as code.

Standard workflow:

```bash
cd infra/opentofu
tofu fmt -recursive
tofu init
tofu validate
tofu plan -out=localarena.tfplan
tofu show localarena.tfplan
tofu apply localarena.tfplan
tofu output
```

Destroy is intentionally destructive and requires review:

```bash
tofu plan -destroy
tofu destroy
```

Example:

```hcl
terraform {
  required_version = ">= 1.13.0"
}

variable "environment" {
  type    = string
  default = "local"
}

output "environment" {
  value = var.environment
}
```

Do not commit `.terraform/`, plan files, crash logs, or state containing secrets. For this local learning project, explicitly document where state is stored and backed up.

## 23. k6

Purpose: generates HTTP and WebSocket traffic and enforces performance thresholds.

Example `load-tests/smoke.js`:

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
    checks: ['rate>0.99'],
  },
};

export default function () {
  const response = http.get('http://localhost:8080/healthz');
  check(response, { 'status is 200': (r) => r.status === 200 });
  sleep(0.1);
}
```

Run it:

```bash
k6 run load-tests/smoke.js
k6 run --vus 50 --duration 1m load-tests/smoke.js
k6 run --out json=results.json load-tests/smoke.js
```

Use the custom Go bot swarm for realistic game-tick/WebSocket traffic; use k6 for login, profile, matchmaking, store, and basic WebSocket scenarios. Only load-test systems you own.

## 24. SOPS and age

Purpose: encrypts project configuration and Kubernetes secret source files before Git storage.

Generate a personal age key once:

```bash
mkdir -p ~/.config/sops/age
chmod 700 ~/.config/sops/age
age-keygen -o ~/.config/sops/age/keys.txt
chmod 600 ~/.config/sops/age/keys.txt
age-keygen -y ~/.config/sops/age/keys.txt
```

The `age1...` line is the public recipient. The `AGE-SECRET-KEY-...` content is private: never commit or paste it into chat.

Encrypt/decrypt:

```bash
RECIPIENT='age1replace-with-your-public-recipient'

sops --encrypt --age "$RECIPIENT" \
  config/secrets.dev.yaml > config/secrets.dev.enc.yaml

SOPS_AGE_KEY_FILE="$HOME/.config/sops/age/keys.txt" \
  sops --decrypt config/secrets.dev.enc.yaml

SOPS_AGE_KEY_FILE="$HOME/.config/sops/age/keys.txt" \
  sops config/secrets.dev.enc.yaml
```

Example `.sops.yaml`:

```yaml
creation_rules:
  - path_regex: config/.*\.enc\.ya?ml$
    age: age1replace-with-your-public-recipient
```

Then:

```bash
sops --encrypt config/secrets.dev.yaml > config/secrets.dev.enc.yaml
```

Keep unencrypted secret files in `.gitignore`.

## 25. Trivy

Purpose: scans container images, filesystems, IaC, and dependency metadata.

```bash
trivy --version

# Image vulnerabilities
trivy image localarena/api:dev
trivy image --severity HIGH,CRITICAL localarena/api:dev

# Repository filesystem and dependencies
trivy fs .

# Kubernetes/Compose/Terraform misconfiguration
trivy config infra/

# Generate machine-readable CI output
trivy image --format json --output trivy.json localarena/api:dev
```

The first vulnerability scan downloads Trivy's advisory database. Review findings and pin patched base images/dependencies rather than ignoring severity blindly.

## 26. Git, curl, and jq

Purpose: source control, HTTP diagnostics, and JSON processing.

```bash
# Git
git status
git diff
git switch -c feature/game-loop
git add <specific-file>
git commit -m 'Implement fixed-tick game loop'
git log --oneline --decorate -10

# curl
curl -i http://localhost:8080/healthz
curl -sS http://localhost:8080/metrics | head
curl -sS -X POST \
  -H 'Content-Type: application/json' \
  -d '{"player_id":"player-1"}' \
  http://localhost:8080/lobbies

# jq
curl -sS http://localhost:8080/status | jq .
jq '.items[] | select(.status == "active")' response.json
kubectl get pods -n localarena -o json \
  | jq -r '.items[] | [.metadata.name, .status.phase] | @tsv'
```

Do not commit credentials, private keys, `.env` files, kubeconfigs, or generated secrets.

---

# Part III — Operations and troubleshooting

## 27. Cluster lifecycle

Normal stop/start:

```bash
docker desktop stop
docker desktop start
kubectl get nodes
```

Kind nodes persist as Docker containers and should recover after Docker Desktop restarts. Wait for recovery:

```bash
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl wait --for=condition=Ready pods --all \
  -n kube-system --timeout=180s
```

Intentional clean reset:

```bash
kind delete cluster --name localarena
kind create cluster --name localarena \
  --config ~/.config/kind/localarena.yaml \
  --wait 5m
kubectl config use-context kind-localarena
```

Deleting the cluster removes workloads and cluster-local persistent volumes. Back up anything important first.

## 28. Common troubleshooting

### Docker command cannot connect

```bash
docker desktop status
systemctl --user status docker-desktop
docker context ls
docker context use desktop-linux
docker desktop start
docker info
```

### Cluster is unavailable after Docker restart

```bash
docker ps -a --filter name=localarena
kind get clusters
kubectl config use-context kind-localarena
kubectl get nodes
```

If the kind containers exist but are stopped:

```bash
docker start localarena-control-plane localarena-worker localarena-worker2
kubectl wait --for=condition=Ready nodes --all --timeout=180s
```

### Pod is not starting

```bash
kubectl get pod <pod> -n localarena -o wide
kubectl describe pod <pod> -n localarena
kubectl logs <pod> -n localarena --all-containers
kubectl get events -n localarena --sort-by=.metadata.creationTimestamp
```

Typical causes include an unloaded local image, failed readiness probe, insufficient memory, invalid configuration, or unavailable dependency.

### Docker/Kafka stack runs out of memory

```bash
docker stats
kubectl top pods -A   # Requires metrics-server; unavailable until installed.
free -h
docker system df
```

Use Compose profiles and run only the current learning phase. Stop optional Grafana/Loki/Tempo/Keycloak components when studying the game loop or Kafka alone.

### Go command resolves to the old installation

```bash
command -v go
go version
go env GOROOT
echo "$PATH" | tr ':' '\n'
hash -r
```

Expected active command:

```text
/home/sandyops/.local/bin/go
go version go1.27.2 linux/amd64
```

### Go security scan reports a vulnerability

```bash
go version
go list -m all
govulncheck ./...
go get <affected-module>@<fixed-version>
go mod tidy
go test -race ./...
govulncheck ./...
```

Do not use `replace` or suppression directives merely to hide findings.

## 29. Suggested daily command sequence

```bash
# 1. Check platform
docker desktop status
kubectl config use-context kind-localarena
kubectl get nodes

# 2. Start dependencies
cd ~/projects/localarena
docker compose up -d
docker compose ps

# 3. Develop and validate Go
gofmt -w .
go test -race ./...
golangci-lint run ./...
gosec -quiet -exclude-generated ./...
govulncheck ./...

# 4. Build/load/deploy when working on Kubernetes
docker build -t localarena/api:dev .
kind load docker-image localarena/api:dev --name localarena
helm lint infra/helm/localarena
helm upgrade --install localarena infra/helm/localarena \
  -n localarena --create-namespace --wait

# 5. Observe
kubectl get pods -n localarena
kubectl logs -f deployment/api -n localarena

# 6. Load test
k6 run load-tests/smoke.js

# 7. Stop project services when finished
docker compose stop
```

Do not run every expensive validation on every file save. Run fast unit tests continuously, race/lint checks before commits, and vulnerability/image scans before milestone releases.

## 30. What is deliberately not installed globally

The following will be introduced later as pinned Compose services or Helm releases:

- Apache Kafka in KRaft mode
- Kafka UI
- Apicurio Registry
- Debezium and Kafka Connect
- PostgreSQL and PgBouncer
- Valkey
- MinIO
- Envoy
- Keycloak and Open Policy Agent
- OpenTelemetry Collector
- Prometheus, Grafana, Loki, Tempo, and Alertmanager
- Toxiproxy and other failure-injection services

Keeping these project-scoped makes versions reproducible, prevents host pollution, and allows the complete lab to be reset.

## 31. Final health checklist

```bash
# Docker
docker desktop status
docker run --rm hello-world

# Kubernetes
kubectl config use-context kind-localarena
kubectl wait --for=condition=Ready nodes --all --timeout=180s
kubectl get pods -n kube-system

# Languages
go version
java -version
gradle --version
node --version

# Platform tools
helm version
tofu version
k6 version

# Protocol/security tools
protoc --version
protoc-gen-go --version
protoc-gen-go-grpc --version
grpcurl --version
sops --version
age --version
trivy --version
golangci-lint version
gosec -version
govulncheck -version
```

A setup is healthy when Docker responds, all three kind nodes are Ready, kube-system pods are Running, and every required command resolves from the shell.
