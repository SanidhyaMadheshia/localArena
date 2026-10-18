package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestLogsCarryTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(LogHandler{slog.NewJSONHandler(&buf, nil)})
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	log.InfoContext(ctx, "hello")
	span.End()
	var m map[string]any
	_ = json.Unmarshal(buf.Bytes(), &m)
	if m["trace_id"] != span.SpanContext().TraceID().String() || m["span_id"] == nil {
		t.Fatalf("log line = %s", buf.String())
	}
	buf.Reset()
	log.Info("no span")
	if bytes.Contains(buf.Bytes(), []byte("trace_id")) {
		t.Fatal("trace_id without a span")
	}
}
