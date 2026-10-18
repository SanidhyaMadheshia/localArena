# ADR-0010: Critical economy events via transactional outbox and Debezium

- Status: accepted
- Date: 2026-10-18

## Context

A purchase or reward must never be committed without its event, nor the
event published without the commit. A dual write (DB, then Kafka) loses events
when the service crashes between the two.

## Decision

- Economy writes insert an `outbox` row (protobuf `Envelope` in `envelope
  bytea`, JSON copy in `payload`, deterministic `id`) in the same
  transaction as the state change.
- Debezium 3.7 Postgres connector (`pgoutput`, slot `debezium_outbox`,
  filtered publication on `public.outbox`, `snapshot.mode=no_data`) with the
  EventRouter SMT. It publishes the envelope bytes verbatim
  (`BinaryDataConverter`) to `game.economy-events.v1`, keyed by `aggregateid`
  (player). The `id` and `type` columns become headers.
- Heartbeats update `debezium_heartbeat` so the slot advances while the
  outbox is idle. The DB password is resolved in the worker
  (`${env:POSTGRES_PASSWORD}`), never stored in the connector config.

## Consequences

- Connect down → events wait in WAL behind the slot (674 kB for 200
  purchases) and are published on recovery. A crash mid-backlog lost
  nothing and duplicated nothing in the test. Debezium is at-least-once, so
  consumers still dedupe by `event_id`.
- **A slot whose consumer is gone retains WAL without bound.** That needs
  alerting on retained WAL (week 6) and `max_slot_wal_keep_size` as a cap.
- `wal_level=logical` on the primary.
- Outbox rows are not deleted yet; a periodic purge (Debezium has already read
  them from WAL) is needed.

## Alternatives considered

- Polling the outbox table: simpler, but it adds DB load and latency, and ordering needs care.
- Listen/notify: not durable.
