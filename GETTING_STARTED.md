# Getting started: run, test, improve and launch LocalArena

This guide takes you from a fresh clone to playing the 3D game in a browser with accounts, a shop and a live leaderboard. It then covers testing, development and what to change before a public launch.

- The design itself is documented in [`docs/`](docs/README.md).
- Measured results are in [`FINAL_REPORT.md`](FINAL_REPORT.md).

---

## 1. Prerequisites

| Tool | Version used | Needed for |
|---|---|---|
| Docker Engine/Desktop + Compose v2 | Compose 2.40+, **≥ 8 GiB RAM** for Docker | everything |
| GNU make, bash, curl, jq, openssl | any recent | scripts |
| Go | 1.27.2 | building and running Go tools/tests on the host (`bin/bots`, `make test`) |
| Node.js + npm | 24.x | frontend development (`web/`) |
| Java 21 | optional | Kafka Streams tests on the host (`cd streams && ./gradlew test`) |
| kind, kubectl, helm, tofu | optional | the Kubernetes path (section 8) |
| k6, golangci-lint, gosec, trivy, sops + age | optional | load tests, lint, scans, encrypted secrets |
| Chrome/Chromium | optional | the headless browser smoke test |

Linux and macOS are supported. On Windows, use WSL2. If something on the host already uses port 6379, that's fine: Compose's Valkey is published on **6380**.

```bash
git clone https://github.com/SanidhyaMadheshia/localArena.git
cd localArena
```

---

## 2. Quick start: play in 5 minutes

### Option A: full game (accounts, shop, leaderboard, rewards)

```bash
make play          # first run builds images: ~5-10 min; later runs ~1-2 min
```

Open **http://localhost:8080**.

- You start as a guest and can play immediately.
- Click **Sign in** and use `alice` / `alice-dev-password` (or `bob` / `bob-dev-password`; the admin is `admin1` / `admin1-dev-password`).
- After sign-in, the client reconnects to the game as your account.
- Buy a cosmetic in the shop. The Basic Hat appears on your character in 3D. Credits from match rewards arrive a few seconds after each round ends.

`make play` does the following:

1. Creates `.secrets.env`:
   - It decrypts the repo secrets if you hold the maintainer's age key.
   - Otherwise it generates random dev secrets.
2. Creates dev TLS certificates.
3. Starts 3 Kafka brokers and creates the topics.
4. Starts the core, `security` and `streaming` profiles, waits until they are healthy, registers the event schemas and starts the CDC connector.

Stop with `make down`. To stop everything from every profile, run `make play-down`. Data lives in named volumes. `docker compose down -v` deletes them (destructive).

### Option B: minimal (gameplay only, about 1.5 GB RAM)

```bash
docker compose up -d --build gameserver
```

Open http://localhost:8080 and play as a guest. Accounts, shop and leaderboard show "unavailable" messages. That is the designed degradation, not a bug.

### Add bots so the arena is busy

```bash
cd backend && go build -o ../bin/ ./cmd/... && cd ..
bin/bots -server http://localhost:8080 -bots 30 -duration 10m
```

Bots are guests by default. `-accounts 2` makes them play as accounts 2..N, so they earn rewards and appear on the leaderboard. That works only with the lab flag `TRUST_PLAYER_PARAM=1`, which is the Compose default; see section 10.

---

## 3. What runs where

```text
Browser ──► :8080 gameserver (serves the web client + /ws game socket)
       ──► :10000 Envoy ──► api ×3 ──► PgBouncer ──► Postgres (+ replica)
       ──► :8180 Keycloak (login)              └──► Valkey (cache)
gameserver ─events─► Kafka ×3 ─► Kafka Streams (leaderboard) ─► /v2/leaderboard
Postgres outbox ─Debezium─► Kafka ─► rewards saga ─► credits
```

| URL / port | Service | Profile |
|---|---|---|
| http://localhost:8080 | **game + web client**; `/status`, `/metrics` | core |
| http://localhost:10000 | public API via Envoy (least-request); `:10001` is round-robin | core |
| https://localhost:8443 | TLS edge (CA: `infra/compose/tls/ca.crt`) | core |
| http://localhost:8180 | Keycloak (admin `kcadmin` / `kcadmin-dev`) | security |
| localhost:8181 | OPA policy engine | security |
| localhost:7071, 7072 | Kafka Streams instances (`/top`, `/healthz`) | streaming |
| localhost:8085 / 8086 / 8083 | Kafka UI / Apicurio schema registry / Kafka Connect | streaming |
| localhost:5432 / 5433 / 6432 / 6380 | Postgres primary / replica / PgBouncer / Valkey | core |
| http://localhost:3000 | Grafana (`admin` / `grafana-dev`) | observability |
| localhost:9090 / 9093 / 9095 | Prometheus / Alertmanager / lab pager | observability |
| localhost:9901 | Envoy admin | core |
| 21080-22081 | simulated regions A/B | regions |

Admin UIs listen only on loopback and have no authentication. Never expose them.

### Compose profiles

| Profile | Adds | Start with |
|---|---|---|
| (none) | gameserver, api ×3, Envoy, Postgres + replica, PgBouncer, Valkey, flagd, asset edge | `docker compose up -d` |
| `security` | Keycloak, OPA | `make sec-up` |
| `streaming` | Kafka ×3, Streams ×2, rewards, Debezium/Connect, Apicurio, Kafka UI | `make stream-up` |
| `observability` | Prometheus, Alertmanager, Grafana, Tempo, Loki, Alloy, OTel collector, exporters | `make obs-up` |
| `regions` | Toxiproxy WAN, CoreDNS, region B game/API | see `experiments/008-week8-regions` |
| `shards` / `load` | two shard databases / k6 | experiments 002/003 |

With 8 GiB, run at most two of the heavy profiles (streaming, observability, security) at a time. Use `docker compose --profile X stop` to free memory.

---

## 4. How to play

| Action | Control |
|---|---|
| Move | WASD / arrow keys; on-screen joystick on touch screens |
| Zoom | mouse wheel |
| Toggle side panel | `P` or the ☰ button |
| Score | touch the golden gems |

- Rounds last 120 s by default (`ROUND_SECONDS`). When a round ends, the rewards saga grants credits to signed-in players.
- The `frenzy` game mode (more gems, faster movement) is rolled out by the `game-mode` feature flag in `infra/compose/flagd/flags.json`.

What the client does:

- **Remote players** are rendered from server snapshots, interpolated 2 ticks (100 ms) behind.
- **Your own character** uses client-side prediction with server reconciliation. You move instantly, but the server's position always wins (`web/src/net.ts`).
- **Purchases** send an `Idempotency-Key`. A retried click can never charge twice.

---

## 5. Verify it works

```bash
curl -s localhost:8080/status | jq .               # matches and players
curl -s localhost:10000/v2/cosmetics | jq .        # API through Envoy
curl -s 'localhost:10000/v2/leaderboard?n=5' | jq . # Streams leaderboard (streaming profile)
make web-smoke                                     # headless browser: guest play, login, purchase
```

`make web-smoke` needs Chrome; set `CHROME=/path/to/chrome` if it is not at `/usr/bin/google-chrome`. It prints the HUD state and fails if the page logs any error. To also take screenshots, run `node e2e/smoke.mjs /tmp` from `web/`.

---

## 6. Tests

| Command | What | Needs |
|---|---|---|
| `make test` | Go unit tests (`go vet` + tests; DB tests skip) | Go |
| `make db-up && make itest` | all Go tests incl. Postgres, PgBouncer, replica, shards, OPA | Docker |
| `make race` | race detector (runs in a container) | `make db-up` |
| `make lint` | gofmt, golangci-lint, gosec | linters |
| `make streams-test` | Kafka Streams topology tests (Java) | Java 21 |
| `make opa-test` | Rego policy tests | Docker |
| `make alert-test` | promtool tests for every SLO/alert rule | Docker |
| `make helm-lint` | Helm chart lint + render | helm |
| `cd web && npm run build` | TypeScript typecheck + production build | Node |
| `make web-smoke` | end-to-end browser test of the running stack | stack + Chrome |
| `make scan` | Trivy image scan | trivy |

CI (`.github/workflows/ci.yml`) runs the Go, Streams, web, policy/config and image jobs on every push.

---

## 7. Experiments and drills

Each week has a reproducible experiment with a README of the measured results:

| Experiment | Run | Shows |
|---|---|---|
| 001 baseline | `make bench`, `bin/bots -bots 1000` | tick p99, saturation point |
| 002 persistence | `make experiment-002` | lost updates, deadlocks, idempotency, failover |
| 003 resilience | `experiments/003-week3-resilience/*.sh` | replica kill/hang, cache outage, retry storm |
| 004 streaming | `make experiment-004` | Kafka down during play, duplicates, CDC crash, schema break |
| 005 security | `make experiment-005` | JWT, OPA, webhook replay, XSS/SSRF, WAF |
| 006 observability | `make drill-backup`, `make drill-failover`, `make incident` | restore, RPO/RTO, hidden incident |
| 007 delivery | `make drills-007` (k8s) | rolling, canary, blue-green, rollback |
| 008 regions | `experiments/008-week8-regions/regions.sh`, `gameday.sh start` | stale reads, partitions, DNS TTL, game day |

Runbooks for every alert are in [`runbooks/`](runbooks/README.md).

---

## 8. Kubernetes (kind)

```bash
make k8s-cluster    # kind cluster: 1 control plane + 2 workers (deletes an existing "localarena" cluster)
make k8s-platform   # OpenTofu: CloudNativePG operator, metrics-server, Argo CD
make k8s-app        # Argo CD Application: syncs infra/helm/localarena from Git (main)
make k8s-status
```

- Argo CD deploys from the Git repo. Your changes reach the cluster only after you push.
- To test a local chart without Git, pause auto-sync (see `drills-007` in the Makefile) and use `helm upgrade --install`. Use one deployer at a time: Helm and Argo CD fight over field ownership.
- Images are loaded into kind with `kind load docker-image localarena/api:dev --name localarena`.

---

## 9. Developing

### Frontend (`web/`)

Stack: **Vite 8 + TypeScript + three.js 0.186 + oidc-client-ts** (pinned in `web/package.json`).

```bash
make play            # or at least the core + security profiles
make web-dev         # http://localhost:5173 with hot reload
```

The dev server proxies `/ws` and `/status` to the gameserver on :8080. The API (:10000) and Keycloak (:8180) already allow the `localhost:5173` origin.

| File | Responsibility |
|---|---|
| `src/config.ts` | endpoints (`VITE_API_URL`, `VITE_OIDC_AUTHORITY`, `VITE_OIDC_CLIENT_ID`, `VITE_GAME_WS`), interpolation delay |
| `src/net.ts` | WebSocket protocol v1, reconnect with jittered backoff, snapshot interpolation, prediction and reconciliation |
| `src/api.ts` | OIDC login (auth code + PKCE), `/v2` client, safe retries |
| `src/scene.ts` | three.js scene: arena, avatars, gems, shadows, particles, labels, camera follow, cosmetics |
| `src/main.ts` | wiring: HUD, input (keyboard/touch), account, shop, leaderboard, toasts |
| `e2e/smoke.mjs` | Playwright smoke test |

The production build (`npm run build` → `web/dist`) is baked into the gameserver image by the `Dockerfile` and served at `/`. After frontend changes, run `docker compose up -d --build gameserver`.

Rules that keep the game fair and robust:

- The client sends only `{dx, dy}` intent (-1/0/1). Never send positions or scores; the server would ignore them (ADR-0001).
- New server messages must be additive (protocol v1). Bump `PROTOCOL_VERSION` only together with the server; the client shows "please reload" on a mismatch.
- Every new write call carries an `Idempotency-Key` if it is retried.
- Visual effects come from diffs between authoritative snapshots (`onSnapshot`), never from local guesses about game rules.

Ideas for next steps:

- 3D models (glTF via `GLTFLoader`) for skins.
- Sound (Web Audio).
- A minimap.
- Spectator mode.
- A post-match results screen fed by `/v1/players/{id}/matches`.
- Delta-encoded snapshots to cut bandwidth. Bandwidth is the dominant cost (`experiments/008-week8-regions/cost.md`).

### Backend (`backend/`)

```bash
cd backend
go build -o ../bin/ ./cmd/...    # gameserver, api, migrate, bots, rewards, eventcat, ...
make -C .. run                    # gameserver on the host with the web client (web/dist)
```

- Add a migration as `backend/migrations/000NN_name.sql` (goose). Follow expand/contract (ADR-0005): it must work with the previous API version still running.
- Add an endpoint in `internal/api` (`v2.go` for authenticated routes). The acting player comes from the token, and authorization comes from OPA (`policies/`, plus a Rego test).
- Event changes go into `schemas/localarena/events/v1/events.proto`. Changes must be backward compatible; `scripts/register-schemas.sh --check <file>` checks this against Apicurio.
- Rebuild a service with `docker compose up -d --build api` (or `gameserver`).

### Kafka Streams (`streams/`, Java 21)

Run `cd streams && ./gradlew test`, then `docker compose --profile streaming up -d --build streams-1 streams-2`.

---

## 10. Configuration switches

| Variable | Default (Compose) | Meaning | Production |
|---|---|---|---|
| `ALLOW_GUESTS` | `1` | anonymous play without a ticket | product decision |
| `TRUST_PLAYER_PARAM` | `1` | **lab:** `?player=<id>` accepted without a ticket (account bots) | **must be `0`** |
| `LEGACY_V1` | `1` | unauthenticated `/v1` API for bots/k6 | **must be `0`** |
| `ROUND_SECONDS` | `120` | round length | tune |
| `ALLOWED_ORIGINS` | `localhost:5173` | extra WebSocket origins | your web domain |
| `PROFILE_TTL` | `30s` | profile cache TTL | tune |
| `STREAMS_URL` | `http://streams-1:7070` | leaderboard source for the API | internal URL |
| `.secrets.env` | generated | `GAME_TICKET_SECRET`, `WEBHOOK_SIGNING_SECRET` | from a secret manager |

---

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Page shows "Sign-in is unavailable" | The `security` profile isn't running: `make sec-up`. |
| Login redirects back but you remain a guest | Keycloak's issuer must be `http://localhost:8180`; open the game via `localhost`, not `127.0.0.1`. |
| "Leaderboard is catching up" | Streams is down or rebalancing: `curl localhost:7071/healthz`. The API degrades to 503 on purpose. |
| Credits never increase after rounds | The rewards worker isn't running (`docker compose --profile streaming ps rewards`), or you played as a guest. See `runbooks/event-pipeline.md` and PM-002. |
| Shop says "temporarily unavailable" | Postgres/PgBouncer is unhealthy or OPA is down (fail closed). Nothing was charged. |
| `disconnected (1006)` loop | The gameserver isn't running: `docker compose ps gameserver`. |
| Black canvas / "WebGL 2 disabled" | Enable hardware acceleration in the browser. |
| Containers OOM-killed or very slow | Stop unused profiles; Docker needs ≥ 8 GiB. |
| `bind: address already in use` | Another service owns the port (often 5432 or 8080). Stop it or change the mapping in `compose.yaml`. |
| Envoy exits: missing TLS files | Run `make certs`. |
| `make secrets` fails with sops errors | Delete `.secrets.env` and re-run; without the age key, random dev secrets are generated. |

---

## 12. Before a public launch

LocalArena runs as a lab. Things that are fine on a laptop must change before real users arrive:

**Security**
- [ ] `TRUST_PLAYER_PARAM=0` and `LEGACY_V1=0`. Decide `ALLOW_GUESTS`.
- [ ] Real secrets from a secret manager. Rotate `GAME_TICKET_SECRET` and the webhook secret. Never reuse the dev values.
- [ ] Keycloak in production mode (`start`, external DB, HTTPS hostname). Remove the dev users and set password and brute-force policies.
- [ ] Public TLS certificates (e.g. Let's Encrypt) at Envoy; HSTS on.
- [ ] Update the CORS (`AllowedOrigins` in `backend/internal/api/v2.go`), WebSocket `ALLOWED_ORIGINS` and Keycloak redirect URIs to the real domain.
- [ ] Put admin UIs (Grafana, Kafka UI, Envoy admin, Keycloak admin) behind SSO/VPN, or remove them.
- [ ] Run `make scan`, gosec and ZAP (see Experiment 005) on the release candidate.

**Reliability**
- [ ] Managed or operator-run Postgres with synchronous replication if zero data loss is required (ADR-0004), automated backups and a tested restore (`make drill-backup`).
- [ ] Kafka with RF 3 and `min.insync.replicas=2` across failure domains, plus monitored consumer lag (`LocalArenaConsumerLagGrowing`).
- [ ] Alertmanager routed to a real pager. Every alert links a runbook (`runbooks/`).
- [ ] Load test at 2× the expected peak (`bin/bots`, `load-tests/`). Keep tick p99 under 50 ms (`LocalArenaTickBudgetBurn`).
- [ ] Game-server allocation by free slots (Agones-style), not CPU HPA (ADR-0018).

**Product and cost**
- [ ] Delta snapshots or binary encoding before scaling. Egress is ~15 KB/s per player.
- [ ] Region selection with automatic re-selection on failure (PM-002, action 5).
- [ ] Add an end-to-end "reward granted after a match" check to CI (PM-002, action 6).
- [ ] Privacy policy, terms, account deletion (GDPR), age requirements.

**Release process**
- [ ] Deploy through Argo CD only. Canary first (ADR-0019). Migrations follow expand/contract with a rollback plan.
- [ ] Run a game day before launch (`experiments/008-week8-regions/gameday.sh`).
