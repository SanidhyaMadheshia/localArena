# ADR-0019: Helm packaging and rollout strategies (rolling, canary, blue-green, migration hooks)

- Status: accepted
- Date: 2026-10-19

## Decision

- **One Helm chart** (`infra/helm/localarena`) for the app and its dependencies (CNPG Cluster/Pooler, Valkey, OPA, flagd).
  - The database `Cluster` carries `helm.sh/resource-policy: keep`.
  - `values.yaml` holds production-shaped defaults; `values-dev.yaml` holds kind overrides.
- **API rolling update:** `maxSurge 1, maxUnavailable 0`, startup, readiness and liveness probes, 5 s readiness-off drain, and a PDB `minAvailable 1`. The HPA owns replicas (2–5 dev), so Argo CD ignores `spec.replicas`.
- **Canary:** `api.canary.enabled` adds a second Deployment (`track=canary`) with its own tag behind the same Service. The share equals replica weight. Promote by setting the stable tag and disabling the canary.
- **Blue-green:** run a second full Deployment (`track=green`), switch the Service selector, and switch back to roll back.
- **Migrations:** a `pre-install,pre-upgrade` hook Job (Argo CD `PreSync`) runs `migrate up`.
  - A failing migration fails the release before any new pod starts; with `--rollback-on-failure` / Argo CD the old revision keeps serving.
  - Migrations stay expand-only (ADR-0005), so the old code works on the new schema in between.

## Consequences

- Zero failed requests in every drill (Experiment 007).
- A canary needs at least 1 of N replicas, so the share is coarse. Precise splits need traffic-level weights.
- A first install with automatic rollback can delete non-`keep` resources (Pooler); avoid it.
