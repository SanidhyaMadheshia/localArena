# ADR-0008: Valkey caching, rate limiting, circuit breaker, and edge asset caching

- Status: accepted
- Date: 2026-10-18

## Context

Profile and lobby reads dominate traffic. Rate limits must hold across
replicas. Valkey is acceleration, not a source of truth (README runtime
invariants): losing it must not lose durable data or take down the API.
Game assets need CDN-like delivery with safe invalidation.

## Decision

### Cache-aside (`internal/cache`)
- Profiles (TTL 30 s) and lobbies (10 s) are cached as JSON, keyed
  `profile:v1:<id>` / `lobby:v1:<id>`. The version segment allows a schema
  change without flushing the whole cache.
- Fills read the **primary**, never the replica, so replication lag cannot
  be copied into the cache. A request carrying a read-your-writes LSN
  bypasses the cache.
- **Invalidate after commit** (delete, not update), for profile on
  purchase and lobby on join/leave.
- **Lease-protected fills:** a miss takes `SET key:lease token NX PX 2000`,
  loads, then stores with a Lua compare-and-set on the lease. `Invalidate`
  deletes the lease too, so a fill that read the old value before a write
  committed cannot overwrite the invalidation
  (`TestInvalidationBeatsInFlightFill`).
- **Stampede guard:** in-process `singleflight` collapses concurrent misses
  per key. A hot key with a 1 s TTL at 1,000 rps produced 62 DB loads with
  singleflight and 650 without (Experiment 003).

### Rate limiting (`internal/ratelimit`)
- Token bucket in one Lua script. Bucket state lives in a Valkey hash, and
  the clock is Valkey `TIME`, so replicas with skewed clocks agree. Rules:
  purchases 2/s burst 5 per player; account creation 5/s burst 20 per
  client IP (rightmost `X-Forwarded-For`, appended by Envoy).
- 429 + `Retry-After`. `X-RateLimit-Backend` tells whether the global or
  the local limiter decided.
- **Valkey down → per-replica `x/time/rate` fallback.** The effective limit
  becomes N × rule. This is deliberately neither fail-closed (blocking
  everyone) nor fail-open (no protection).

### Circuit breaker
- `sony/gobreaker/v2` around every Valkey call: opens after 5 consecutive
  failures, stays open 5 s, then admits 1 half-open probe. A miss (`nil`
  reply) or a caller cancellation does not count as a failure. While open,
  calls fail in microseconds, so a dead cache adds no latency.
- Degradation per feature:

  | Feature | Valkey down |
  |---|---|
  | Profile/lobby reads | served from Postgres |
  | Rate limits | per-replica |
  | Presence (ephemeral, Valkey-only) | heartbeats 503 + Retry-After, reads "unknown" |

  Presence does not fall back to Postgres on purpose: writing heartbeats to
  the source of truth during a cache outage moves the load spike onto the
  database.

### Edge asset cache
- **Garage v2.4.1** (S3-compatible) replaces MinIO, which was archived
  upstream in April 2026 and no longer ships images. Assets are served from
  Garage's website endpoint behind **two NGINX caching proxies** (`edge-a`
  on `:8090`, `edge-b` on `:8091`) with independent caches, simulating two
  PoPs.
- **Immutable, content-hashed URLs** (`/assets/skin-gold.8a5d0cbba780.svg`)
  are cached for 1 year (`Cache-Control: immutable`). "Invalidation" means
  publishing a new hash and a new `manifest.json` (5 s TTL). Old URLs stay
  valid for clients that still reference them, and nothing ever needs a
  purge.
- `proxy_cache_lock` collapses concurrent misses (200 concurrent requests
  for a cold 2 MB object produced 2 origin fetches).
  `proxy_cache_use_stale error timeout` keeps cached assets available while
  the origin is down. The cache key ignores query strings, so cache-busting
  parameters cannot fragment the cache.

## Consequences

- With the 30 s TTL plus invalidation, a missed invalidation (Valkey blip
  during a write) can serve a stale profile for at most 30 s. The warning is
  logged. Credits and inventory are always checked in Postgres at purchase
  time, so stale reads never allow a bad write.
- Singleflight is per process: N replicas can still issue N concurrent
  loads per key. The lease limits stores, not loads. A distributed "only the
  lease holder loads, others wait" scheme is possible but not needed at
  this scale.
- Garage runs single-node with `replication_factor = 1`, so there is no
  durability. The bucket holds re-publishable build artifacts, not user data.
  Replays (week 4) will need replication 3 or backups.

## Alternatives considered

- Write-through caching: simpler reads, but every write path must update
  the cache correctly, and a failed cache write blocks the request.
- Probabilistic early expiration (XFetch): avoids synchronized expiry
  without locks. A good follow-up if lease contention ever shows up.
- Purging edge caches via an API on every deploy: slow, error-prone across
  PoPs, and races with in-flight fills. Hashed URLs avoid it.
