# ADR-0007: Envoy edge, timeout budget, retries, and graceful drain

- Status: accepted
- Date: 2026-10-18

## Context

The API now runs as 3 stateless replicas. One replica failing (crash, hang,
CPU starvation, deploy) must not cause an API outage (Week 3 gate), and
retries must neither duplicate writes nor amplify a shared-dependency outage
into a retry storm.

## Decision

- **Envoy v1.40 edge** (`infra/compose/envoy/envoy.yaml`) with two listeners
  over the same 3 replicas, kept for comparison: `:10000` round robin and
  `:10001` least request (power of two choices). Replicas are found with
  `STRICT_DNS` on the Compose service name.
- **Health:** active `/readyz` check every 2 s (2 failures to eject), passive
  outlier detection (5 consecutive 5xx / 3 gateway failures, at most 34% of
  hosts ejected), and a panic threshold so traffic is never sent to zero hosts.
- **Timeout budget, innermost shortest:**

  | Layer | Timeout |
  |---|---|
  | Postgres `statement_timeout` (migration 00007) | 2 s |
  | Envoy per-try timeout (GET/PUT/keyed writes) | 2.5 s |
  | API request context | 3 s |
  | Envoy route timeout (all tries) | 4 s |
  | Valkey operation | 100 ms |

  The per-try timeout is deliberately **longer** than the DB timeout. A slow
  shared database then answers with an API 503 instead of an Envoy timeout,
  and only a genuinely hung replica gets cut off and retried elsewhere.
  Experiment 003 measured the wrong order (1.5 s per-try < 2 s DB): 7,482
  proxy retries during a DB brownout.
- **Retries:**
  - Any request: only when it never reached an upstream (connect failure,
    refused stream, reset before request).
  - GET, PUT, or a write with an `Idempotency-Key` header: also 5xx, gateway
    errors, and per-try timeouts, on a different host.
  - All retries are capped by a retry budget (20% of active requests,
    minimum 10 concurrent).
  - Retries use Envoy's jittered exponential backoff (25–250 ms).
- **Shared-dependency failures are not retried by the proxy.** The API sets
  `x-envoy-ratelimited` on 503s caused by Postgres or Valkey being
  unavailable. Envoy does not retry those, because every replica shares the
  same dependency. Clients still get `Retry-After`.
- **Graceful drain:** on SIGTERM the API fails `/readyz` for 5 s (2.5 health
  intervals), keeps serving, then calls `http.Server.Shutdown`.
  `stop_grace_period` is 20 s.

## Consequences

- Killing a replica: 0 failed requests out of 9,000 at 300 rps (5 connect
  failures, all retried). A graceful stop gives 0 failures with no retries
  needed.
- Hanging a replica (`docker pause`): 0 failures, but round robin keeps
  sending about 1/3 of requests to the hung host until health checks eject
  it, so p99 equals the per-try timeout (2.5 s). Least request routes away
  almost immediately: p99 12 ms vs 2,518 ms. **Least request is the default
  for the API.**
- Retries on PUT assume PUT handlers are idempotent. Presence heartbeats are;
  any future PUT must be as well.
- Envoy's admin port (9901) has no auth and is published on loopback only.

## Alternatives considered

- NGINX or HAProxy as the API gateway: both are capable. Envoy's retry
  budget, outlier detection, and stats are richer, and Envoy returns in
  later weeks (service mesh, gRPC).
- Client-side load balancing in each caller: removes a hop, but every client
  language has to implement health, retry, and budget logic.
