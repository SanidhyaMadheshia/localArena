# ADR-0013: Identity with Keycloak OIDC, short-lived JWTs, game tickets, SOPS secrets

- Status: accepted
- Date: 2026-10-18

## Context

Through week 4 the API trusted `player_id` in requests and the gameserver
trusted `?player=`: anyone could act as anyone. Secrets were plain env defaults.

## Decision

- **Keycloak 26.8** realm `localarena`:
  - Access tokens: 5 min RS256 JWTs with audience `localarena-api` (audience mapper) and realm roles `player`/`admin`.
  - Refresh tokens rotate (`revokeRefreshToken`, no reuse).
  - Brute-force protection on.
  - Clients: `localarena-web` (public, authorization code + PKCE S256), `localarena-bots` (password grant, lab only), `localarena-api` (bearer-only audience).
- **API verification** (`internal/auth`, go-oidc): RS256 only, issuer, audience, expiry.
  - Keys come from the JWKS URL over the internal network, while the issuer stays the browser-facing URL.
  - An unknown `kid` triggers a JWKS refetch, so signing-key rotation needs no restart.
- **Account mapping by `sub`** (stable), not username. First login provisions the player (`players.idp_sub`).
- **Bans live in Postgres** (`bans`) and are checked on every request, so they apply before tokens expire.
- **Game tickets:** `POST /v2/game-tickets` returns `base64url(player | expiry | HMAC-SHA256)`, valid 60 s.
  - The gameserver verifies only the MAC (it has no DB) and ignores `?player=` when tickets are configured.
- **Secrets:** SOPS + age.
  - `.sops.yaml` holds only the public recipient; `config/secrets.dev.enc.yaml` holds the ticket and webhook secrets, encrypted.
  - `scripts/secrets-env.sh` decrypts them to a gitignored, mode-600 `.secrets.env` used as Compose `env_file`.
  - The private age key never leaves `~/.config/sops/age/`.

## Consequences

- Losing the age key means the secrets must be regenerated. Back up `keys.txt`.
- A compromised ticket secret lets an attacker mint tickets for any player for as long as it is valid. Rotate it via SOPS and restart both services. Tickets live 60 s, so the overlap is short.
- Keycloak runs in dev mode (H2, HTTP) in the lab; production needs a database and TLS.

## Alternatives considered

- Verifying JWTs in the gameserver: possible, but bans would then need a DB or cache lookup on the hot path. Tickets keep it stateless.
- Opaque tokens with introspection: a network call per request and a dependency on Keycloak availability.
- Vault / external secret managers: right for production; SOPS keeps the lab offline and Git-native.
