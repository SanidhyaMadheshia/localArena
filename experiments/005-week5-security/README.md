# Experiment 005: Week 5 security, identity, and API evolution

## Hypothesis

With token-based identity, policy-based authorization and edge controls,
a client cannot act as another player, escalate privileges, replay a signed
webhook, inject SQL/HTML, or reach internal services through the avatar
importer. Floods are shed at the edge. Outages of OPA or flagd fail safe
(deny writes / default flag), not open. Scans show no unaddressed high
findings.

## System version and configuration

- 2026-10-18. Keycloak 26.8.1 (dev mode, realm import), OPA 1.21.2, flagd
  0.18.0, Envoy 1.40 (TLS on `:8443`), SOPS 3.13.3 + age 1.3.2, go-oidc 3.21.0.
- Setup:
  ```bash
  scripts/secrets-env.sh   # decrypt SOPS secrets -> .secrets.env (needs ~/.config/sops/age/keys.txt)
  scripts/tls-certs.sh     # dev CA + server cert
  docker compose --profile security up -d keycloak opa flagd api gameserver envoy
  experiments/005-week5-security/e2e.sh
  ```
- Lab users (realm file): `alice`, `bob` (player), `admin1` (player, admin).

## Results

### End-to-end checks (`results/e2e.txt`): 20/20 pass

| Area | Check | Result |
|---|---|---|
| IdP | password grant issues RS256 token with `aud=localarena-api`, roles | pass |
| TLS | TLS 1.2+ with the dev CA; TLS 1.1 refused; HSTS | pass |
| AuthN | no token → 401; edited claims with the original signature → 401 | pass |
| AuthZ | player banning someone → 403; admin ban → 204 | pass |
| Bans | banned player's **still-valid** token: purchase 403, game ticket 403 | pass (ban checked per request, not baked into tokens) |
| Game | ticket from `/v2/game-tickets` → WebSocket welcome as the same player; forged ticket → 401 | pass |
| Edge | `..%2f` traversal 400 (Envoy normalization); `<script>` and `UNION SELECT` in query → 403 (WAF route); 20 KB header → 431 | pass |
| DoS | 4000 req/s burst against a 1000/s bucket → `local_rate_limit.rate_limited` > 0 (429s) | pass |
| Flags | flagd stopped → gameserver healthy, rounds start as `classic` | pass |

### Unit / integration tests (new this week)

| Test | Proves |
|---|---|
| `TestVerifyClaimsAndRejections` | expired, wrong issuer, wrong audience, `alg=none`, foreign key with the same `kid` all rejected |
| `TestKeyRotation` | new `kid` accepted without restart; retired key rejected after JWKS refresh |
| `TestTickets` | HMAC tickets: expiry, tampering, other secret rejected |
| `TestFailClosed` (authz) | OPA 500 / undefined decision / garbage / >200 ms / unreachable → deny (`ErrUnavailable` → HTTP 503) |
| OPA `opa test` (7) | self-only player actions, admin-only bans, no self-ban, banned denied, unknown action denied |
| `TestV2AuthFlow` | sub→player mapping is stable; body `player_id` rejected; XSS payload never appears as raw HTML; bidi override rejected; `1 OR 1=1` → 400; 8 KB body rejected; CORS allow-list |
| `TestSSRFGuard` | 11 bad URLs refused: http, file, gopher, userinfo, 127.0.0.1, localhost, 169.254.169.254, `[::1]`, `[::ffff:10.0.0.1]`, 10/8, port 6379 |
| `TestAvatarContentValidation` | declared type ignored, bytes sniffed; HTML served as `image/png` refused; >512 KiB refused |
| `TestMatchResultWebhook` | valid → 201; same delivery → 200 `replayed`, recorded once; tampered body or stale timestamp → 401 |
| `TestWebSocketTickets` | no ticket, forged ticket, `?player=` → 401 when tickets are required |
| `TestFlagdRollout` / `TestFallbacks` | 20% fractional rollout (105/500 frenzy), sticky per key; every flagd failure → default |

### Scans (`results/`)

| Scanner | Target | Before | After |
|---|---|---|---|
| Trivy image (HIGH/CRITICAL) | `localarena/api`, `gameserver` | 0 | 0 |
| Trivy image | `localarena/streams` | **4 HIGH**: lz4-java 1.11.2 (via kafka-clients); Go stdlib in `/usr/bin/pebble` from the Ubuntu base | **0**: pinned lz4-java 1.11.4, removed unused `pebble` |
| Trivy config | Dockerfiles, compose, k8s | 1 HIGH (DS-0002 no `USER`; base was already `nonroot`) | 0 (explicit `USER 65532`) |
| Trivy secrets | repository | 2 private keys | both are gitignored dev TLS keys (`infra/compose/tls/*.key`), not committed |
| gosec | backend (42 files) | 1 (G705 XSS taint on avatar bytes) | 0 (type re-check on serve; nosniff + CSP) |
| ZAP baseline | `https://localhost:8443/v2/cosmetics` | 0 FAIL, 62 PASS, 5 low/informational WARN | Spectre isolation fixed with CORP; remaining: cache/storable informational on `no-store` JSON, a Unix timestamp in data, "ZAP out of date" |

## Root explanation

- **Identity is separated from authorization.** Keycloak proves who; OPA
  decides what; the database holds facts that must take effect at once
  (bans). A ban therefore does not wait for tokens to expire.
- **The gameserver stays database-free.** Instead of verifying JWTs and
  checking bans itself, it trusts a 60 s HMAC ticket that the API mints
  after doing both.
- **Fail-safe direction is chosen per dependency:**
  - OPA unavailable → deny (security decision).
  - flagd unavailable → default variant (product decision).
  - Valkey unavailable → degrade (week 3).

## Surprises

1. The e2e "forged token" test passed a token through once. A swapped
   *last* base64url character can decode to the same signature bytes
   (padding bits), so it was not a forgery at all. The test now edits the
   claims instead.
2. Envoy's route `safe_regex` matches only the path, not the query string,
   so the first WAF rule missed `?q=<script>`. Matching the `:path` header
   fixed it.
3. Keycloak returns **400** `invalid_grant` for a wrong password (per
   OAuth2), not 401.
4. The vulnerable Go stdlib in the Streams image came from a base-image
   utility, not from our code. Image scans must cover base layers.

## Remaining risks

See `threat-model.md`, "Accepted risks": unauthenticated admin UIs (loopback
only), `LEGACY_V1=1` in the lab, plaintext internal hops, Keycloak dev mode,
and the bot password grant.
