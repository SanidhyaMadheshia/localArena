GO_IMAGE := golang:1.27.2-alpine
BACKEND  := backend

.PHONY: build web test itest race lint vuln bench run bots up down logs db-up k8s-up k8s-status experiment-002

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
	LOCALARENA_TEST_SHARD_URLS=$(PG)@localhost:5440/localarena,$(PG)@localhost:5441/localarena

db-up:
	docker compose --profile shards up -d --wait postgres postgres-replica pgbouncer pg-shard0 pg-shard1

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
		-e CGO_ENABLED=1 $(GO_IMAGE) sh -c 'apk add -q gcc musl-dev && go test -race -count=1 -p 1 ./...'

# Kubernetes (kind-localarena): CloudNativePG operator, cluster + pooler, migrate Job, API.
CNPG_CHART := 0.29.1
k8s-up:
	docker compose build api
	kind load docker-image localarena/api:dev --name localarena
	helm repo add cnpg https://cloudnative-pg.github.io/charts --force-update >/dev/null
	helm upgrade --install cnpg cnpg/cloudnative-pg --version $(CNPG_CHART) -n cnpg-system --create-namespace --wait
	kubectl apply -f infra/kubernetes/00-namespace.yaml -f infra/kubernetes/10-postgres.yaml
	kubectl -n localarena wait --for=condition=Ready cluster/arena-db --timeout=5m
	kubectl -n localarena delete job migrate --ignore-not-found
	kubectl apply -f infra/kubernetes/20-migrate-job.yaml
	kubectl -n localarena wait --for=condition=complete job/migrate --timeout=3m
	kubectl apply -f infra/kubernetes/30-api.yaml
	kubectl -n localarena rollout restart deploy/api
	kubectl -n localarena rollout status deploy/api --timeout=3m

k8s-status:
	kubectl -n localarena get cluster,pooler,pods,svc

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
