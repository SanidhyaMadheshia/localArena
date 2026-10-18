# ADR-0020: OpenTofu for the platform, Argo CD for the app, GitHub Actions CI

- Status: accepted
- Date: 2026-10-19

## Decision

- **OpenTofu** (`infra/opentofu`) owns cluster-level platform pieces: the app namespace and the CNPG operator, metrics-server and Argo CD Helm releases.
  - Pinned chart and provider versions.
  - Existing releases are adopted with `import` blocks, never recreated.
  - State is local and gitignored (it contains no secrets). A shared setup needs a remote backend with locking.
  - `manage_app=false` by default: Tofu does not deploy the app.
- **Argo CD** owns the application: an `Application` renders the chart from `main` with automated sync, prune and self-heal, plus retry.
  - Exactly one controller deploys the app. Manual `helm upgrade` against an Argo-managed release fails on server-side-apply field ownership (Experiment 007 F1); drills pause auto-sync explicitly.
- **CI** (`.github/workflows/ci.yml`):
  - Go: gofmt, vet, golangci-lint, `-race` integration tests against Postgres, Valkey and OPA service containers, govulncheck, gosec.
  - Kafka Streams Gradle tests.
  - `opa test`, promtool rule tests, `helm lint`/`template`, `tofu fmt`/`validate`.
  - Apicurio BACKWARD_TRANSITIVE check of the PR's schema against `main`.
  - Trivy config and secret scans.
  - Images built with Buildx and the GitHub Actions layer cache, Trivy-scanned (HIGH/CRITICAL fail), and pushed to GHCR by commit SHA on `main`.
  - Pinned tool versions; least-privilege `permissions`. Validated with actionlint.

## Consequences

- Reproducible environment: `tofu apply` → `kubectl apply` of the Application → Argo CD converges.
- Image promotion is not automated yet: the cluster uses locally loaded `:dev` images. The next step is an image-updater or a values commit by CI.
