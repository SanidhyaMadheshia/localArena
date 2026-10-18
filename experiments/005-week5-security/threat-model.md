# LocalArena threat model (Week 5)

STRIDE review of the lab architecture, 2026-10-18. Scope: everything reachable
on the host (Compose stack). Kubernetes adds the same components behind
ClusterIP services. "Status" is what is in place now, not what is planned.

## Assets

| Asset | Why it matters |
|---|---|
| Player credits, inventory, purchases | economic integrity (fictional currency, but the invariants are real) |
| Match outcome / scores / leaderboard | fairness; rewards are paid from it |
| Account identity (Keycloak), tokens, game tickets | impersonation enables every other attack |
| Secrets (ticket HMAC, webhook HMAC, DB password, age key) | forge tickets/webhooks, read all data |
| Event streams (Kafka) and outbox | replay or injection changes leaderboards and rewards |
| Availability of matches | the product |

## Trust boundaries

1. Browser / bot → Envoy (`:8443` TLS, `:10000/:10001` plain, lab), `:8080` gameserver.
2. Envoy → API replicas (cluster network).
3. API → Postgres / PgBouncer / Valkey / OPA / Keycloak JWKS / internet (avatar import).
4. Gameserver → Kafka; Kafka → Streams / rewards worker → Postgres.
5. Tournament server → API webhook (shared HMAC secret).
6. Operator → admin UIs (Kafka UI, Connect REST, Envoy admin, Keycloak admin): **loopback only, no auth**.

## Threats and controls

| # | STRIDE | Threat | Control | Status / evidence |
|---|---|---|---|---|
| 1 | S | Act as another player by sending their `player_id` | v2 takes the player from the verified token's `sub`; OPA requires `resource == subject` | done; `TestV2AuthFlow` (body `player_id` → 400) |
| 2 | S | Forged / expired / `alg=none` / foreign-key JWT | go-oidc: RS256 only, issuer, audience, expiry, JWKS keys | done; `TestVerifyClaimsAndRejections`, e2e forged → 401 |
| 3 | S | Old signing key keeps working after rotation | JWKS refetch on unknown `kid`; retired keys drop out | done; `TestKeyRotation` |
| 4 | S | Join a match as someone else | 60 s HMAC game ticket from the API; `?player=` ignored when tickets are on | done; `TestWebSocketTickets`, e2e |
| 5 | T | Client sends authoritative positions/scores | server-authoritative engine, inputs clamped (ADR-0001) | done (week 1) |
| 6 | T | Forged or replayed match-result webhook | HMAC over `t.delivery.body`, 5 min window, delivery id claimed once in the same tx | done; `TestMatchResultWebhook` |
| 7 | T | Duplicate / replayed purchase or reward | idempotency keys, deterministic reward ids, state-guarded saga | done (weeks 2, 4) |
| 8 | T | Poison / incompatible events | Apicurio BACKWARD_TRANSITIVE; Streams DLQ | done (week 4) |
| 9 | T | SQL injection | all queries parameterised (pgx); ids parsed as integers | done; e2e/unit `1 OR 1=1` → 400 |
| 10 | T | Stored XSS via display name / avatar | names stored as data, JSON-encoded, client renders with `textContent`; `nosniff`, CSP `default-src 'none'`; avatars sniffed to png/jpeg/webp and re-checked on serve | done; `TestV2AuthFlow`, `TestAvatarContentValidation` |
| 11 | I | SSRF through avatar import (metadata service, internal ports) | https only, no userinfo, dial-time IP check (blocks private, loopback, link-local, CGNAT, mapped v6), ports 80/443, redirect checks, 3 s, 512 KiB | done; `TestSSRFGuard` |
| 12 | I | Secrets in Git | SOPS + age (`config/secrets.dev.enc.yaml`), plaintext `.secrets.env` gitignored, TLS keys gitignored | done; Trivy secret scan finds only the gitignored dev keys |
| 13 | I | Traffic sniffing | TLS 1.2+ at the edge with HSTS | done for the edge; **internal hops are plaintext** (accepted for the lab) |
| 14 | I | Error messages leak internals | 500s return "internal error"; details only in logs | done |
| 15 | D | Request floods | Envoy global token bucket (1000/s, burst 2000), per-player/IP Valkey limits, timeouts, 4 KiB bodies, 16 KB headers | done; e2e burst → 429s |
| 16 | D | Kafka/Valkey/OPA/flagd outage takes the game down | non-blocking producer + spool; cache breaker; OPA fails closed (503) only for v2 writes; flags fall back to defaults | done (weeks 3-5) |
| 17 | E | Player bans others or escalates | OPA policy: only `admin` bans, no self-ban, banned can do nothing; ban checked per request | done; 7 OPA tests, e2e |
| 18 | E | Container breakout impact | distroless non-root images, `USER 65532`, read-only rootfs and dropped caps in k8s | done |
| 19 | R | Admin denies a ban | ban rows record `banned_by` (admin sub) + structured audit log | partial: logs are not tamper-evident |

## Accepted risks / open items

- **Unauthenticated admin surfaces** (Kafka UI, Connect REST, Envoy admin, Keycloak admin console, Streams IQ): bound to `127.0.0.1` in Compose, ClusterIP in k8s. Must be authenticated before any shared deployment.
- **`LEGACY_V1=1` by default** in Compose so weeks 1-4 experiments and bots keep working; it reintroduces threat #1 for v1. Secure stack: `LEGACY_V1=0 ALLOW_GUESTS=0`.
- **Bot client uses the password grant** (`localarena-bots`), lab only; the browser client is authorization code + PKCE.
- **Plaintext inside the cluster** (API ↔ DB, Kafka): mTLS/service mesh is a week 7 option.
- **Keycloak dev mode** (H2 database, HTTP): realm is re-imported on restart; production would need a real DB and TLS.
- **WAF rules are pattern blocklists**; they reduce noise, they do not replace the input handling above.
