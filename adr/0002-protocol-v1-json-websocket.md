# ADR-0002: Game protocol v1 is versioned JSON over WebSocket

- Status: accepted (to be revisited in week 5 for API/schema evolution)
- Date: 2026-10-18

## Context

Browsers need a bidirectional, low-overhead channel. Native UDP is not
available to browsers, and Week 1 values easy debugging over byte efficiency.

## Decision

- Transport: WebSocket at `GET /ws` (gorilla/websocket, using the
  readPump/writePump pattern from its chat example: one reader goroutine and
  one writer goroutine per connection, with ping/pong deadlines).
- Encoding: JSON text frames, fully specified in `internal/protocol`.
  - `welcome {v, id, match, tickHz, w, h}`: `v` is the protocol version. A
    client that sees an unknown `v` must disconnect instead of guessing.
  - `input {seq, dx, dy, ct, st}`: `seq` is strictly increasing per session.
    `st` (optional) is the last snapshot tick the client processed. The
    server uses it for flow control (adr/0003).
    `ct` (client clock) is diagnostic only and never used for ordering, since
    client clocks are skewed and untrusted.
  - `snap {tick, ack, players, items}`: full state every tick. `ack` is the
    highest applied `seq`, so clients can measure input RTT without
    synchronized clocks.
- Inbound frames are capped at 256 bytes (`SetReadLimit`). An invalid
  message closes the session with code 1003.
- The `Origin` header must match the host or an explicit allowlist, which
  prevents cross-site WebSocket hijacking once auth cookies exist.
- Status uses plain HTTP instead of the socket: JSON, long poll
  (`?since=V&wait=`), and SSE (`/status/stream`). All three are compared in
  Experiment 001.

## Consequences

- At 20 players, a snapshot is about 725 B (measured ~14.5 KB/s/player at
  20 Hz, Experiment 001). Size grows with players, so per-match bandwidth is O(n²).
  Delta compression or a binary encoding (protobuf) is the obvious
  optimization if bandwidth/player becomes the bottleneck.
- TCP head-of-line blocking: one lost segment delays every later snapshot.
  The UDP comparison is a separate experiment.
- Changing the meaning of a field requires bumping `v`. Adding optional
  fields does not.

## Alternatives considered

- gRPC streaming: browsers need grpc-web plus a proxy, so it is reserved for
  internal RPC.
- WebTransport/QUIC: avoids head-of-line blocking, but browser and Go library
  maturity is lower and the scope is too large for Week 1.
