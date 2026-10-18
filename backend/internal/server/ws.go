package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/engine"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/match"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/telemetry"
)

// Timeouts follow the gorilla/websocket chat example.
const (
	writeWait  = 5 * time.Second // a client that cannot absorb a snapshot in 5 s is dropped
	pongWait   = 30 * time.Second
	pingPeriod = pongWait * 9 / 10
)

// session bridges one WebSocket connection and one match client.
// readPump is the only reader and writePump the only writer of conn.
type session struct {
	conn *websocket.Conn
	mgr  *match.Manager
	m    *match.Match
	c    *match.Client
	log  *slog.Logger

	writeErr atomic.Value  // string reason set by writePump so readPump reports the real cause
	seenTick atomic.Uint64 // last snapshot tick the client reported processing; 0 = never reported
}

// A client reporting a snapshot older than maxLagSeconds is throttled to one
// snapshot per laggingInterval until it catches up. It is never cut off
// entirely: the newest snapshot is what lets it catch up (cutting it off
// livelocks, see Experiment 001).
const (
	maxLagSeconds   = 1
	laggingInterval = time.Second
)

var tracer = otel.Tracer("localarena/gameserver")

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracer.Start(ctx, "ws.join", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	// Authenticate before upgrading, so a refused client gets a plain HTTP 401.
	var account engine.PlayerID
	switch q := r.URL.Query(); {
	case s.cfg.Tickets != nil && q.Get("ticket") != "":
		id, err := s.cfg.Tickets.Verify(q.Get("ticket"), time.Now())
		if err != nil || id <= 0 || id >= int64(match.GuestIDBase) {
			http.Error(w, "invalid game ticket", http.StatusUnauthorized)
			return
		}
		account = engine.PlayerID(id) // #nosec G115 -- range checked above
	case s.cfg.Tickets != nil && !s.cfg.AllowGuests:
		http.Error(w, "game ticket required", http.StatusUnauthorized)
		return
	case s.cfg.Tickets == nil:
		// Pre-auth mode (weeks 1-4): ?player= is trusted. Lab only.
		if v, err := strconv.ParseUint(q.Get("player"), 10, 32); err == nil {
			account = engine.PlayerID(v)
		}
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already replied with an HTTP error
	}
	m, c, err := s.mgr.Join(account) // one live session per account
	if err == nil {
		span.SetAttributes(attribute.String("match.id", m.ID), attribute.Int64("player.id", int64(c.ID)))
	} else {
		span.SetStatus(codes.Error, err.Error())
	}
	if errors.Is(err, match.ErrAlreadyPlaying) {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "already connected"), time.Now().Add(writeWait))
		_ = conn.Close()
		return
	}
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "server shutting down"), time.Now().Add(writeWait))
		_ = conn.Close()
		return
	}
	telemetry.Connections.Inc()
	ss := &session{conn: conn, mgr: s.mgr, m: m, c: c,
		log: s.log.With("player", c.ID, "match", m.ID, "remote", r.RemoteAddr)}
	ss.log.InfoContext(ctx, "session open")
	go ss.writePump()
	go ss.readPump()
}

// readPump forwards inputs to the match. On any read error it leaves the
// match, which closes c.Done and in turn stops writePump.
func (ss *session) readPump() {
	reason := "client_close"
	defer func() {
		ss.mgr.Leave(ss.m, ss.c)
		_ = ss.conn.Close()
		telemetry.Connections.Dec()
		telemetry.Disconnects.WithLabelValues(reason).Inc()
		ss.log.Debug("session closed", "reason", reason)
	}()
	ss.conn.SetReadLimit(protocol.MaxClientMessage)
	_ = ss.conn.SetReadDeadline(time.Now().Add(pongWait))
	ss.conn.SetPongHandler(func(string) error { return ss.conn.SetReadDeadline(time.Now().Add(pongWait)) })
	for {
		_, b, err := ss.conn.ReadMessage()
		if err != nil {
			reason = readErrReason(err)
			if r, ok := ss.writeErr.Load().(string); ok {
				reason = r // writePump failed first and closed the conn
			}
			return
		}
		telemetry.BytesReceived.Add(float64(len(b)))
		in, err := protocol.DecodeInput(b)
		if err != nil {
			telemetry.InputInvalid.Inc()
			reason = "protocol_error"
			_ = ss.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "invalid input"), time.Now().Add(writeWait))
			return
		}
		if in.ST > ss.seenTick.Load() {
			ss.seenTick.Store(in.ST) // only readPump writes; monotonic
		}
		ss.m.Submit(ss.c.ID, in)
	}
}

// writeErrReason separates a client too slow to drain (deadline hit) from a
// peer that already went away (broken pipe/reset).
func writeErrReason(err error) string {
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) && ne.Timeout() {
		return "write_timeout"
	}
	return "write_error"
}

func readErrReason(err error) string {
	var ne interface{ Timeout() bool }
	switch {
	case websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway):
		return "client_close"
	case errors.As(err, &ne) && ne.Timeout():
		return "read_timeout"
	case errors.Is(err, websocket.ErrReadLimit):
		return "oversized"
	default:
		return "conn_error"
	}
}

// writePump sends the welcome, then the newest snapshot whenever one is ready.
func (ss *session) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = ss.conn.Close() // unblocks readPump on write failure
	}()
	cfg := ss.m.Config()
	welcome, _ := json.Marshal(protocol.Welcome{T: protocol.TypeWelcome, V: protocol.Version,
		ID: uint32(ss.c.ID), Match: ss.m.ID, TickHz: ss.m.TickHz, W: cfg.Width, H: cfg.Height, Mode: ss.m.Mode()})
	if ss.write(welcome) != nil {
		return
	}
	var (
		lastWrite time.Time
		sentTick  uint64 // approx. tick of the last snapshot written (match tick at write time)
	)
	for {
		select {
		case b := <-ss.c.Out:
			// Lag is measured against what we last *sent*, not the current tick:
			// a throttled client can only ever report old ticks, so comparing to
			// the current tick would keep it throttled forever.
			seen := ss.seenTick.Load()
			lagging := seen > 0 && sentTick > seen+uint64(maxLagSeconds*ss.m.TickHz) // #nosec G115 -- positive constant times positive tick rate
			if lagging && time.Since(lastWrite) < laggingInterval {
				telemetry.SnapshotLagged.Inc()
				continue
			}
			if err := ss.write(b); err != nil {
				ss.writeErr.Store(writeErrReason(err))
				return
			}
			lastWrite, sentTick = time.Now(), ss.m.Tick()
		case <-ss.c.Done: // removed from match (leave or shutdown)
			_ = ss.conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = ss.conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, "match ended"))
			return
		case <-ticker.C:
			_ = ss.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := ss.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (ss *session) write(b []byte) error {
	_ = ss.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := ss.conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return err
	}
	telemetry.BytesSent.Add(float64(len(b)))
	return nil
}
