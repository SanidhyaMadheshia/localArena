// Package store is LocalArena's PostgreSQL data layer (pgx v5).
//
// Correctness is pushed into the database wherever possible: CHECK
// constraints (no negative credits/stock), primary keys (one active lobby per
// player, an item owned once), and unique idempotency keys. Application code
// adds locking strategies on top, and every transaction is retried on
// serialization failures and deadlocks (the whole closure re-runs, so it must
// not have side effects outside the transaction).
package store

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Domain errors returned by the store.
var (
	ErrNotFound            = errors.New("not found")
	ErrUsernameTaken       = errors.New("username taken")
	ErrInsufficientCredits = errors.New("insufficient credits")
	ErrOutOfStock          = errors.New("out of stock")
	ErrAlreadyOwned        = errors.New("already owned")
	ErrAlreadyInLobby      = errors.New("player already in a lobby")
	ErrLobbyFull           = errors.New("lobby full")
	ErrLobbyClosed         = errors.New("lobby closed")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with different request")
	ErrConflict            = errors.New("concurrent modification; retries exhausted")
)

// SQLSTATE codes we act on (https://www.postgresql.org/docs/current/errcodes-appendix.html).
const (
	codeUniqueViolation      = "23505"
	codeCheckViolation       = "23514"
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
)

var (
	// TxRetries counts transaction re-runs by reason.
	TxRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "localarena", Subsystem: "db", Name: "tx_retries_total",
		Help: "Transactions re-run after a deadlock, serialization failure, or optimistic-lock conflict.",
	}, []string{"reason"})
	queries = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "localarena", Subsystem: "db", Name: "queries_total",
		Help: "SQL statements sent (N+1 detector).",
	})
	// QueryCount mirrors queries_total for tests and experiments.
	QueryCount atomic.Int64
)

// errRetry signals an optimistic-lock conflict: roll back and re-run.
var errRetry = errors.New("optimistic lock conflict")

// Store holds the primary pool and an optional read replica pool.
type Store struct {
	Primary    *pgxpool.Pool
	Replica    *pgxpool.Pool // equals Primary when no replica is configured
	MaxRetries int
}

// New returns a Store. replica may be nil.
func New(primary, replica *pgxpool.Pool) *Store {
	if replica == nil {
		replica = primary
	}
	return &Store{Primary: primary, Replica: replica, MaxRetries: 10}
}

// queryTracer counts statements; enough to make N+1 visible in metrics.
// queryTracer counts statements (N+1 detector) and forwards to the
// OpenTelemetry pgx tracer, which creates one span per query (adr/0016).
type queryTracer struct{ otel *otelpgx.Tracer }

func (t queryTracer) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	queries.Inc()
	QueryCount.Add(1)
	return t.otel.TraceQueryStart(ctx, c, d)
}
func (t queryTracer) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	t.otel.TraceQueryEnd(ctx, c, d)
}

// NewPool opens a pool with explicit, small limits. Acquire waits are bounded
// by the caller's context, so pool exhaustion surfaces as a timeout instead of
// an unbounded queue.
func NewPool(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.ConnConfig.Tracer = queryTracer{otel: otelpgx.NewTracer()}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	return pgxpool.NewWithConfig(ctx, cfg)
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func constraintOf(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.ConstraintName
	}
	return ""
}

// inTx runs fn in a transaction on the primary, re-running it on deadlock,
// serialization failure, or errRetry with jittered exponential backoff.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return s.inTxOpts(ctx, pgx.TxOptions{}, fn)
}

func (s *Store) inTxOpts(ctx context.Context, opts pgx.TxOptions, fn func(pgx.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := pgx.BeginTxFunc(ctx, s.Primary, opts, fn)
		var reason string
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errRetry):
			reason = "optimistic_conflict"
		case pgCode(err) == codeDeadlockDetected:
			reason = "deadlock"
		case pgCode(err) == codeSerializationFailure:
			reason = "serialization_failure"
		default:
			return err
		}
		TxRetries.WithLabelValues(reason).Inc()
		if attempt >= s.MaxRetries {
			if reason == "optimistic_conflict" {
				return ErrConflict
			}
			return err
		}
		backoff := time.Duration(1<<min(attempt, 6)) * time.Millisecond
		select {
		case <-time.After(backoff + rand.N(backoff)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// IsUnavailable reports whether err means the database is temporarily
// unreachable (connect failure, connection-class SQLSTATE, shutdown in
// progress, or writing to a just-demoted primary). Callers should answer
// 503 + Retry-After rather than 500: the request may succeed after failover.
func IsUnavailable(err error) bool {
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		return true
	}
	// A pooled connection dying mid-query (network partition, failover) surfaces
	// as EOF / connection reset, not as a SQLSTATE (Experiment 008).
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) || pgconn.SafeToRetry(err) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	switch c := pgCode(err); {
	case len(c) == 5 && c[:2] == "08": // connection exception (incl. pgbouncer login retry)
		return true
	case c == "57P01", c == "57P02", c == "57P03", c == "25006": // shutdown / cannot connect now / read-only tx
		return true
	}
	return false
}
