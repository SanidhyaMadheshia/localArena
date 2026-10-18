# Experiment 007: Week 7 Kubernetes delivery, autoscaling, GitOps, rollout strategies

## Hypothesis

Packaged as a Helm chart, deployed by Argo CD and provisioned by OpenTofu, the API can be rolled, canaried, switched blue-green and rolled back without a single failed request. A failing migration cannot reach production pods. Game servers drain instead of cutting players. The API scales out on CPU and back in.

## System version and configuration

- kind 1.37 (1 control plane + 2 workers), Helm 4.3.1, chart `infra/helm/localarena` 0.7.0, CloudNativePG 0.29.1, metrics-server 3.14.0 (0.9.0), Argo CD chart 10.10.5 (v3.5.5), OpenTofu 1.13.1 (helm/kubernetes providers 3.3.0).
- Reproduce:
  ```bash
  cd infra/opentofu && tofu init && tofu apply          # cnpg, metrics-server, Argo CD
  kubectl apply -f infra/argocd/localarena-app.yaml      # GitOps: Argo CD deploys the chart from main
  kubectl -n argocd patch app localarena --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}'  # pause GitOps for drills (F1)
  experiments/007-week7-delivery/drills.sh
  ```
- During each drill, a pod inside the cluster sends ~20 req/s through Service `api` (`/v1/cosmetics`) and counts failures.

## Results

| Drill | Change | Result |
|---|---|---|
| **Rolling update** | v1 → v2 (maxSurge 1, maxUnavailable 0, readiness gate, 5 s API drain) | 12 s, **1672 ok / 0 failed** |
| **Canary** | 1 canary (v2) + 2 stable (v1) behind one Service | 744 ok / 0 failed; traffic share **31% canary** (228/744, by per-pod request counters) ≈ 1/3; promoted to v2 |
| **Blue-green** | full green Deployment (v2) next to blue (v1); Service selector switched, then switched back | switch 0.08 s; **740 ok / 0 failed** across cutover and rollback |
| **Failed migration** | upgrade with a migration that fails (`NOT NULL` column without default) | pre-upgrade hook failed after 16 s, release marked failed, previous revision kept (`--rollback-on-failure`); **schema unchanged**; **836 ok / 0 failed** |
| **Helm rollback** | v2 → previous revision (v1) | 6 s, **1108 ok / 0 failed** |
| **Gameserver drain** | delete the gameserver pod with 20 bots playing | readiness false immediately (no new players routed), **0 of 20 bots dropped**, pod exited 22 s after SIGTERM, well within the 45 s grace |
| **HPA** | 600 req/s of history queries for 150 s | 2 → 5 replicas (max) within ~30 s of load; **90,000 requests, 0 failed**, p95 9.8 ms; back to 2 within 150 s of the load ending |
| **GitOps self-heal** | `kubectl scale deploy flagd --replicas=0` | Argo CD restored 1 replica in **2 s** |
| **OpenTofu** | adopt the existing cnpg and metrics-server releases, add Argo CD | `3 imported, 1 added, 3 changed, 0 destroyed`; the second plan shows **No changes** |

## Findings

1. **F1: two controllers, one app.** Helm 4 uses server-side apply. After Argo CD synced the app, a manual `helm upgrade` failed with field-ownership conflicts (`conflict with "argocd-controller"`). That is correct: an app must have exactly one deployer. The drills paused Argo auto-sync and used `--force-conflicts`; re-enabling auto-sync moved the cluster straight back to the Git state (`api:dev`, Synced/Healthy). In normal operation, changes go through Git only.
2. **F2: a failed first install deletes the pooler.** `--rollback-on-failure` on a first install *uninstalls* the release. The CNPG `Cluster` survived because of `helm.sh/resource-policy: keep`; the `Pooler` did not. Stateful resources need `keep`, and first installs should not run with automatic rollback.
3. **F3: OPA and ConfigMap directories.** Mounting the policy ConfigMap and loading the directory made OPA load the file three times (through Kubernetes' `..data` symlinks): "multiple default rules". Load the file path, not the directory.
4. **F4: rendering quirks.** An unquoted `?` inside a YAML flow map (`/health?bundles`), and `include | nindent` inside flow mappings, both produced invalid YAML. The helpers now emit block style.
5. **Measurement mistake fixed:** the first canary share read 0% for every pod. The grep also matched histogram lines, and `kubectl` inside a `while read`-style loop consumed stdin. The final numbers come from the per-pod `requests_total` counter only.

## Game-server scaling (why not an HPA)

The API is stateless, so CPU-based HPA is correct for it. A game server holds live matches in memory, so scaling it is an **allocation** problem:

- Scale-in must pick an *empty* server, or drain one for a full round. The HPA picks pods arbitrarily, and its scale-down would cut matches.
- Scale-out should follow *free player slots* (players / capacity), not CPU. CPU stays low until the tick budget is nearly gone (Experiment 001).
- Matchmaking must place a new match on a server with room and stop sending to a draining one (readiness already does the second part).

The production pattern is a fleet controller (e.g. Agones `Fleet` + `FleetAutoscaler` with a buffer of ready servers, `GameServerAllocation` from matchmaking). This lab provides the building blocks: drain on SIGTERM, readiness off while draining, a status endpoint with players per match, and `terminationGracePeriodSeconds` > round length.

## Remaining risks

- CI has not run on GitHub yet; the workflow is validated by actionlint only. GHCR publishing needs package permissions.
- The canary is replica-weighted (31% at 1/3). Precise percentages or header-based routing need a mesh or Gateway API weights (Envoy, week 8).
- Blue-green was done by hand with `kubectl` (Service selector). A chart-native version would template both colours.
- Argo CD tracks `main` with no image-tag promotion flow. Images are `:dev`, loaded into kind locally.
