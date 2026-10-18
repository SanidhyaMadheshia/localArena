# syntax=docker/dockerfile:1
# Build context: repository root (needs backend/ and web/).

FROM node:24.13.0-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/tsconfig.json web/index.html ./
COPY web/src ./src
RUN npm run build

FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY backend/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/gameserver ./cmd/bots ./cmd/api ./cmd/migrate

# Distroless static: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot AS gameserver
COPY --from=build /out/gameserver /gameserver
COPY --from=web /web/index.html /web/
COPY --from=web /web/dist /web/dist
ENV WEB_DIR=/web ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/gameserver"]

FROM gcr.io/distroless/static-debian12:nonroot AS bots
COPY --from=build /out/bots /bots
ENTRYPOINT ["/bots"]

# API image also carries the migrate binary (used by the Compose "migrate" service and the k8s Job).
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build /out/api /api
COPY --from=build /out/migrate /migrate
ENV ADDR=:8081
EXPOSE 8081
ENTRYPOINT ["/api"]
