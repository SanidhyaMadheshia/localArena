GO_IMAGE := golang:1.27.2-alpine
BACKEND  := backend

.PHONY: build web test race lint vuln bench run bots up down logs

build: web
	cd $(BACKEND) && go build -o ../bin/ ./cmd/...

web:
	cd web && npm ci --silent && npm run build

test:
	cd $(BACKEND) && go vet ./... && go test -count=1 ./...

# The host has no C toolchain, so -race runs inside the Go image.
race:
	docker run --rm -v $(CURDIR)/$(BACKEND):/src -w /src \
		-v localarena-gomod:/go/pkg/mod -v localarena-gobuild:/root/.cache/go-build \
		-e CGO_ENABLED=1 $(GO_IMAGE) sh -c 'apk add -q gcc musl-dev && go test -race -count=3 ./...'

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
