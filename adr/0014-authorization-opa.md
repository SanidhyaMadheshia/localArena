# ADR-0014: Authorization decisions in Open Policy Agent, failing closed

- Status: accepted
- Date: 2026-10-18

## Context

Rules such as "players act only on their own account", "only admins ban, and
not themselves" and "banned users can do nothing" were about to spread through
handlers. Rules must be testable and reviewable on their own.

## Decision

- Policies in `policies/localarena.rego` (Rego v1). The default is deny, and the result is a decision object `{allow, reason}`. Unit tests use `opa test`.
- The API (`internal/authz`) sends `{action, subject: {player_id, roles, banned}, resource: {player_id}}` to OPA's data API. The API supplies facts (identity from the token, bans from the DB); OPA supplies the rule.
- **Fail closed:** an unreachable OPA, a non-200 response, an undefined decision (policy not loaded), malformed JSON, or more than 200 ms all count as "unavailable". Writes then get 503 + `Retry-After`, never allow. Public reads do not consult OPA.

## Consequences

- An OPA outage stops purchases, bans and tickets but not reading or playing existing matches. Runs as a sidecar or near the API in k8s.
- Each decision costs one local HTTP call (sub-ms in the lab). A cache is unnecessary at this scale.
- Policy changes do not need a Go deploy, but they do need the same review and tests.

## Alternatives considered

- Hard-coded checks in Go: fast and simple; rules get scattered and are harder to audit.
- Keycloak authorization services: couples policy to the IdP and its UI.
