# ADR-0016: Observability with OpenTelemetry traces, Prometheus metrics, Loki logs

- Status: accepted
- Date: 2026-10-18

## Decision

- **Traces:** the OpenTelemetry Go SDK (`internal/obs`) exports OTLP/gRPC to an OTel Collector (memory limiter, batching), which sends to Tempo.
  - W3C `traceparent` propagation.
  - Parent-based sampling with `TRACE_SAMPLE_RATIO`; 1.0 in the lab.
  - otelhttp gives a server span per request, renamed to the matched route pattern; otelpgx gives a span per SQL statement; the gameserver adds a `ws.join` span.
  - Tracing is a no-op when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset.
- **Metrics:** Prometheus scrapes services directly (pull), not through the collector.
  - API RED metrics are labelled by route *pattern* (bounded cardinality).
  - Existing game, event, cache and pool metrics stay; exporters cover Postgres and PgBouncer.
- **Logs:** JSON `slog` with `trace_id`/`span_id` added from the context (`obs.LogHandler`). Alloy tails Docker logs and ships them to Loki with only `service` and `level` labels; `trace_id` stays in the line and Grafana links it to Tempo.
- **Grafana** is provisioned from files: datasources with cross-links, plus the "LocalArena overview" dashboard (SLOs, RED, game, events/dependencies, database).

## Consequences

- One request can be followed from metric spike → exemplar/trace → SQL spans → logs (used in PM-001).
- Admin UIs (Grafana anonymous viewer, Prometheus, Alertmanager) are loopback-only and unauthenticated in the lab.
- High-cardinality data (player IDs, raw paths) never goes into metric labels or Loki labels; it goes into span attributes and log lines.

## Alternatives considered

- Metrics through the collector (OTLP to Prometheus remote write): one pipeline, but loses pull-based `up` health and adds a hop.
- ELK for logs: heavier; Loki's label model fits Prometheus habits.
