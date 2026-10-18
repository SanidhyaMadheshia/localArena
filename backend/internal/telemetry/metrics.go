// Package telemetry holds the Prometheus metrics for the game server.
// Names follow https://prometheus.io/docs/practices/naming/ (base units, _total suffix).
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ns = "localarena"

var (
	TickDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "tick_duration_seconds",
		Help:    "Time to simulate one tick and fan out snapshots.",
		Buckets: []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
	})
	TickLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "tick_start_lag_seconds",
		Help:    "Delay between a tick's scheduled time and when the match goroutine started it (scheduler starvation).",
		Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25},
	})
	Ticks         = promauto.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ticks_total", Help: "Ticks simulated."})
	TickOverruns  = promauto.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "tick_overruns_total", Help: "Ticks whose work exceeded the tick period."})
	TicksMissed   = promauto.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ticks_missed_total", Help: "Scheduled ticks skipped because the loop fell behind."})
	Matches       = promauto.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "matches", Help: "Running match actors."})
	Players       = promauto.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "players", Help: "Players currently in matches."})
	Connections   = promauto.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "ws_connections", Help: "Open WebSocket connections."})
	BytesSent     = promauto.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ws_sent_bytes_total", Help: "WebSocket payload bytes written."})
	BytesReceived = promauto.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: "ws_received_bytes_total", Help: "WebSocket payload bytes read."})
	Inputs        = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "inputs_total", Help: "Client inputs by result."}, []string{"result"})
	Snapshots     = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "snapshots_total", Help: "Snapshots offered to client mailboxes by result."}, []string{"result"})
	Disconnects   = promauto.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "disconnects_total", Help: "Session terminations by reason."}, []string{"reason"})
)

// Pre-resolved label children keep the hot path allocation-free.
var (
	InputApplied    = Inputs.WithLabelValues("applied")
	InputStale      = Inputs.WithLabelValues("stale")
	InputQueueFull  = Inputs.WithLabelValues("queue_full")
	InputInvalid    = Inputs.WithLabelValues("invalid")
	SnapshotQueued  = Snapshots.WithLabelValues("queued")
	SnapshotReplace = Snapshots.WithLabelValues("replaced")
	SnapshotLagged  = Snapshots.WithLabelValues("skipped_lagging")
)
