GO_IMAGE := golang:1.27.2-alpine
BACKEND  := backend

.PHONY: build web test itest race lint vuln bench run bots up down logs db-up k8s-cluster k8s-up k8s-status experiment-002

build: web
	cd $(BACKEND) && go build -o ../bin/ ./cmd/...

web:
	cd web && npm ci --silent && npm run build

test:
	cd $(BACKEND) && go vet ./... && go test -count=1 ./...

# Integration tests against the Compose databases (make db-up first).
PG := postgres://localarena:localarena-dev
TEST_ENV := LOCALARENA_TEST_DATABASE_URL=$(PG)@localhost:5432/localarena \
	LOCALARENA_TEST_REPLICA_URL=$(PG)@localhost:5433/localarena \
	LOCALARENA_TEST_PGBOUNCER_URL=$(PG)@localhost:6432/localarena \
	LOCALARENA_TEST_VALKEY_ADDR=localhost:6380 \
	LOCALARENA_TEST_OPA_URL=http://localhost:8181 \
	LOCALARENA_TEST_FLAGD_URL=http://localhost:8016 \
	LOCALARENA_TEST_SHARD_URLS=$(PG)@localhost:5440/localarena,$(PG)@localhost:5441/localarena

db-up:
	docker compose --profile shards --profile security up -d --wait postgres postgres-replica pgbouncer valkey pg-shard0 pg-shard1 opa flagd

itest:
	cd $(BACKEND) && $(TEST_ENV) go test -count=1 -p 1 ./...

# Experiment 002 tables (slow): purchase strategies + EXPLAIN ANALYZE.
experiment-002:
	cd $(BACKEND) && $(TEST_ENV) LOCALARENA_EXPERIMENT=1 go test -count=1 -v -run Experiment ./internal/store

# The host has no C toolchain, so -race runs inside the Go image. It joins the
# Compose network so integration tests run too when the databases are up.
RACE_PG := postgres://localarena:localarena-dev
race:
	docker run --rm -v $(CURDIR)/$(BACKEND):/src -w /src \
		-v localarena-gomod:/go/pkg/mod -v localarena-gobuild:/root/.cache/go-build \
		--network localarena_default \
		-e LOCALARENA_TEST_DATABASE_URL=$(RACE_PG)@postgres:5432/localarena \
		-e LOCALARENA_TEST_REPLICA_URL=$(RACE_PG)@postgres-replica:5432/localarena \
		-e LOCALARENA_TEST_PGBOUNCER_URL=$(RACE_PG)@pgbouncer:5432/localarena \
		-e LOCALARENA_TEST_VALKEY_ADDR=valkey:6379 \
		-e CGO_ENABLED=1 $(GO_IMAGE) sh -c 'apk add -q gcc musl-dev && go test -race -count=1 -p 1 ./...'

# Kubernetes (kind-localarena): CloudNativePG operator, cluster + pooler, Valkey, migrate Job, API.
CNPG_CHART := 0.29.1

# DESTRUCTIVE: deletes and recreates the kind cluster (1 control plane + 2 workers).
k8s-cluster:
	kind delete cluster --name localarena
	kind create cluster --name localarena --config infra/kubernetes/kind/cluster.yaml --wait 5m

k8s-up:
	docker compose build api
	kind load docker-image localarena/api:dev --name localarena
	helm repo add cnpg https://cloudnative-pg.github.io/charts --force-update >/dev/null
	helm upgrade --install cnpg cnpg/cloudnative-pg --version $(CNPG_CHART) -n cnpg-system --create-namespace --wait -f infra/kubernetes/helm/cnpg-values.yaml
	kubectl apply -f infra/kubernetes/00-namespace.yaml -f infra/kubernetes/10-postgres.yaml -f infra/kubernetes/15-valkey.yaml
	kubectl -n localarena wait --for=condition=Ready cluster/arena-db --timeout=5m
	kubectl -n localarena delete job migrate --ignore-not-found
	kubectl apply -f infra/kubernetes/20-migrate-job.yaml
	kubectl -n localarena wait --for=condition=complete job/migrate --timeout=3m
	kubectl apply -f infra/kubernetes/30-api.yaml
	kubectl -n localarena rollout restart deploy/api
	kubectl -n localarena rollout status deploy/api --timeout=3m

k8s-status:
	kubectl get nodes
	kubectl -n localarena get cluster,pooler,pdb,pods,svc -o wide

# gosec 2.29.0 as released bundles an x/tools that cannot read Go 1.27 export
# data ("export data version 5"), so it silently skips type-aware rules. It is
# rebuilt locally against golang.org/x/tools v0.52.0.
lint:
	cd $(BACKEND) && gofmt -l . | (! grep .) && golangci-lint run ./... && gosec -quiet -exclude-generated -exclude=G404 ./...  # G404: seeded math/rand is required for deterministic replay (adr/0001)

vuln:
	cd $(BACKEND) && govulncheck ./...

bench:
	cd $(BACKEND) && go test -run '^$$' -bench . -benchmem ./internal/engine

run: build
	./bin/gameserver -web web

# Usage: make bots N=200 ARGS="-latency 200ms"
N ?= 100
bots:
	cd $(BACKEND) && go run ./cmd/bots -bots $(N) $(ARGS)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f gameserver

# ---- Week 4 streaming (Kafka, Apicurio, Debezium, Streams, rewards) ----
.PHONY: stream-up stream-down proto streams-test experiment-004
stream-up:
	docker compose --profile streaming up -d --wait kafka-1 kafka-2 kafka-3
	scripts/kafka-topics.sh
	docker compose --profile streaming up -d
	sleep 20
	scripts/register-schemas.sh
	scripts/connect.sh

stream-down:
	docker compose --profile streaming stop

proto:
	protoc -I schemas -I $(HOME)/.local/include --go_out=backend/gen \
		--go_opt=module=github.com/SanidhyaMadheshia/localArena/backend/gen schemas/localarena/events/v1/events.proto

streams-test:
	cd streams && ./gradlew test --no-daemon -q

experiment-004:
	experiments/004-week4-streaming/run.sh

# ---- Week 5 security ----
.PHONY: secrets certs sec-up opa-test experiment-005 scan
secrets:
	scripts/secrets-env.sh
certs:
	scripts/tls-certs.sh
sec-up: secrets certs
	docker compose --profile security up -d keycloak opa flagd api gameserver envoy
opa-test:
	docker run --rm -v $(CURDIR)/policies:/p:ro openpolicyagent/opa:1.21.2-static test /p -v
experiment-005:
	experiments/005-week5-security/e2e.sh
scan:
	trivy image --severity HIGH,CRITICAL localarena/api:dev localarena/gameserver:dev localarena/streams:dev
	trivy config --severity HIGH,CRITICAL --skip-dirs experiments .
	cd $(BACKEND) && gosec -quiet -exclude-generated -exclude=G404 ./...

# ---- Week 6 observability / SRE ----
.PHONY: obs-up alert-test drill-backup drill-failover incident
obs-up:
	docker compose --profile observability up -d
alert-test:
	docker run --rm --entrypoint promtool -v $(CURDIR)/infra/observability:/o -w /o/rules-tests prom/prometheus:v3.13.4 test rules slo_test.yml
	docker run --rm --entrypoint promtool -v $(CURDIR)/infra/observability:/o prom/prometheus:v3.13.4 check rules /o/rules/slo.yml
drill-backup:
	experiments/006-week6-observability/backup-restore.sh
drill-failover:
	experiments/006-week6-observability/failover-rpo.sh
incident:
	experiments/006-week6-observability/incident.sh start

# ---- Week 7 Kubernetes delivery ----
.PHONY: helm-lint k8s-platform k8s-app drills-007
helm-lint:
	helm lint infra/helm/localarena -f infra/helm/localarena/values-dev.yaml
	helm template la infra/helm/localarena -f infra/helm/localarena/values-dev.yaml --set api.canary.enabled=true > /dev/null
k8s-platform:
	cd infra/opentofu && tofu init -input=false && tofu apply -input=false
k8s-app:
	kubectl apply -f infra/argocd/localarena-app.yaml
drills-007:
	kubectl -n argocd patch app localarena --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}'
	experiments/007-week7-delivery/drills.sh
	kubectl -n argocd patch app localarena --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
