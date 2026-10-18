# ADR-0015: API versioning, signed webhooks, input/edge security, feature flags

- Status: accepted
- Date: 2026-10-18

## Decision

- **API versioning:**
  - `/v2` is authenticated, with the actor taken from the token.
  - `/v1` is frozen. It is served only when `LEGACY_V1=1` and then carries `Deprecation`, `Sunset: 2027-01-31` and `Link: </v2/>; rel="successor-version"`.
  - Changes inside a version are additive only. Protocol and event schemas follow the same rule: new optional fields (`Welcome.mode`, `Input.st`); a breaking change gets a new version, enforced by Apicurio for events.
- **Signed webhooks** (`POST /v2/webhooks/match-results`):
  - Header `t=<unix>,v1=<HMAC-SHA256(secret, t.delivery.body)>` plus `X-LocalArena-Delivery`.
  - The receiver verifies over the raw body with a constant-time compare and a ±5 min window, then claims the delivery ID once (`webhook_deliveries`) in the same transaction that records the match.
  - Several `v1` values and two secrets are accepted during rotation.
- **Input security:**
  - Parameterised SQL everywhere.
  - Display names are data: Unicode-validated (no control or bidi characters), JSON-encoded, rendered with `textContent`.
  - Avatar import uses a dial-time SSRF guard plus sniffed image types and size limits.
  - `nosniff`, CSP `default-src 'none'`, CORP, `no-store`.
  - CSRF is not applicable (bearer header, no cookies), and CORS is an allow-list.
- **Edge (Envoy):**
  - TLS 1.2+ on `:8443` with HSTS.
  - A WAF-style deny route on `:path` (traversal, script, UNION SELECT, JNDI).
  - 16 KB header cap.
  - A global local token bucket (1000/s, burst 2000).
  - Per-identity limits stay in the API (Valkey, week 3).
- **Feature flags:**
  - flagd evaluated through OFREP (`internal/flags`), 100 ms budget, default on any error.
  - `game-mode` is a 20% fractional rollout of `frenzy`, keyed by round ID and evaluated at round start on the match goroutine, so a round never changes mode midway.

## Consequences

- Old clients keep working through the sunset date, and the headers make the deadline visible.
- Webhook senders must reuse the delivery ID on retry, or the receiver cannot dedupe.
- WAF patterns are a noise filter; correctness relies on the application controls.
- flagd outages freeze rollouts at the default and never break matches.
