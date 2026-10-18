// Package obs wires OpenTelemetry tracing and trace-correlated logging (adr/0016).
//
// Traces go over OTLP/gRPC to the collector (OTEL_EXPORTER_OTLP_ENDPOINT, the
// standard env var). Without it, tracing is a no-op and costs nothing. Sampling
// is parent-based with a ratio for new roots (OTEL_TRACES_SAMPLER_ARG-like
// TRACE_SAMPLE_RATIO, default 1.0 in the lab) so a sampled request stays
// sampled across services.
package obs

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs the global tracer provider and W3C propagator. The returned
// function flushes spans; call it on shutdown.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx) // endpoint/insecure from OTEL_EXPORTER_OTLP_* env
	if err != nil {
		return nil, err
	}
	ratio := 1.0
	if v, err := strconv.ParseFloat(os.Getenv("TRACE_SAMPLE_RATIO"), 64); err == nil && v >= 0 && v <= 1 {
		ratio = v
	}
	host, _ := os.Hostname()
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName(service), semconv.ServiceInstanceID(host)))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second), sdktrace.WithMaxQueueSize(4096)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// LogHandler adds trace_id/span_id to every record logged with a context that
// carries a span, so logs (Loki) and traces (Tempo) link both ways.
type LogHandler struct{ slog.Handler }

// Handle implements slog.Handler.
func (h LogHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h LogHandler) WithAttrs(a []slog.Attr) slog.Handler { return LogHandler{h.Handler.WithAttrs(a)} }

// WithGroup implements slog.Handler.
func (h LogHandler) WithGroup(n string) slog.Handler { return LogHandler{h.Handler.WithGroup(n)} }

// NewLogger returns a JSON logger with trace correlation and a service field.
func NewLogger(service string, level slog.Level) *slog.Logger {
	return slog.New(LogHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})}).With("service", service)
}
