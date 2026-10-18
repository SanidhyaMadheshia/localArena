// Command bots runs a swarm of LocalArena WebSocket bots and prints a report.
//
// Each bot joins via /ws, sends a random-walk input every tick with an
// increasing sequence number, and measures input->ack RTT from snapshots.
// Fault knobs reproduce the Week 1 experiments without external tools:
//
//	-latency 200ms     added one-way delay on inbound snapshots (throughput preserved)
//	-loss 0.05         drop 5% of inputs before sending (application-level loss)
//	-slow 0.1          10% of bots read snapshots very slowly (slow consumer)
//	-churn 2s          every bot reconnects on average every 2 s (reconnect storm)
//
// The swarm scrapes the server's /metrics before and after the run to compute
// tick-duration percentiles and per-player cost from the server's own view.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
)

type config struct {
	server   string
	bots     int
	duration time.Duration
	rampUp   time.Duration
	inputHz  int
	latency  time.Duration
	loss     float64
	slow     float64
	churn    time.Duration
	jsonOut  bool

	accountBase int
}

// stats is shared by all bots; samples are bounded per bot to cap memory.
type stats struct {
	mu        sync.Mutex
	rtts      []time.Duration // input->ack round-trip samples
	gaps      []time.Duration // time between consecutive snapshots
	connects  atomic.Int64
	failures  atomic.Int64
	drops     atomic.Int64 // server-initiated disconnects
	snapshots atomic.Int64
	bytes     atomic.Int64
	inputs    atomic.Int64
}

func (s *stats) add(rtts, gaps []time.Duration) {
	s.mu.Lock()
	s.rtts = append(s.rtts, rtts...)
	s.gaps = append(s.gaps, gaps...)
	s.mu.Unlock()
}

func main() {
	var c config
	flag.StringVar(&c.server, "server", "http://localhost:8080", "game server base URL")
	flag.IntVar(&c.bots, "bots", 100, "number of concurrent bots")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "measurement duration after ramp-up")
	flag.DurationVar(&c.rampUp, "ramp", 5*time.Second, "time to spread bot connections over")
	flag.IntVar(&c.inputHz, "input-hz", 20, "inputs per second per bot")
	flag.DurationVar(&c.latency, "latency", 0, "artificial delay applied to received snapshots")
	flag.Float64Var(&c.loss, "loss", 0, "fraction of inputs dropped client-side [0,1)")
	flag.Float64Var(&c.slow, "slow", 0, "fraction of bots that read slowly [0,1]")
	flag.DurationVar(&c.churn, "churn", 0, "mean session lifetime before reconnect; 0 disables")
	flag.BoolVar(&c.jsonOut, "json", false, "print the report as JSON")
	flag.IntVar(&c.accountBase, "accounts", 0, "if >0, bot i plays as account accountBase+i (?player=); else guests")
	servers := flag.String("servers", "", "comma-separated region game servers; each bot picks the lowest-RTT healthy one (region selection, adr/0021)")
	flag.Parse()
	if *servers != "" {
		best, rtts := pickRegion(strings.Split(*servers, ","))
		fmt.Fprintf(os.Stderr, "region RTTs: %v -> %s\n", rtts, best)
		if best == "" {
			fmt.Fprintln(os.Stderr, "no healthy region")
			os.Exit(1)
		}
		c.server = best
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	before, err := scrape(c.server)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scrape before:", err)
		os.Exit(1)
	}
	st := &stats{}
	runCtx, cancel := context.WithTimeout(ctx, c.rampUp+c.duration)
	defer cancel()
	measureStart := time.Now().Add(c.rampUp)

	var wg sync.WaitGroup
	for i := range c.bots {
		delay := time.Duration(0)
		if c.bots > 1 {
			delay = c.rampUp * time.Duration(i) / time.Duration(c.bots)
		}
		slow := rand.Float64() < c.slow
		wg.Go(func() {
			select {
			case <-time.After(delay):
			case <-runCtx.Done():
				return
			}
			url := wsURL(c.server)
			if c.accountBase > 0 {
				url += fmt.Sprintf("?player=%d", c.accountBase+i)
			}
			runBot(runCtx, c, url, slow, measureStart, st)
		})
	}

	// Sample server metrics at the peak, before bots disconnect.
	var peak metrics
	select {
	case <-time.After(c.rampUp + c.duration - 500*time.Millisecond):
		peak, _ = scrape(c.server)
	case <-ctx.Done():
	}
	wg.Wait()
	after, err := scrape(c.server)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scrape after:", err)
	}
	report(c, st, before, peak, after)
}

func wsURL(base string) string {
	u, _ := url.Parse(base)
	u.Scheme = map[string]string{"https": "wss"}[u.Scheme]
	if u.Scheme == "" {
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	return u.String()
}

// runBot keeps one logical player connected until ctx ends, reconnecting on churn or failure.
func runBot(ctx context.Context, c config, url string, slow bool, measureStart time.Time, st *stats) {
	for ctx.Err() == nil {
		life := time.Duration(0)
		if c.churn > 0 {
			life = time.Duration(rand.ExpFloat64() * float64(c.churn))
		}
		if err := session(ctx, c, url, slow, life, measureStart, st); err != nil {
			st.failures.Add(1)
			select { // backoff with jitter so failures don't become a reconnect storm
			case <-time.After(100*time.Millisecond + rand.N(400*time.Millisecond)):
			case <-ctx.Done():
			}
		}
	}
}

const maxSamplesPerSession = 2000

func session(ctx context.Context, c config, url string, slow bool, life time.Duration, measureStart time.Time, st *stats) error {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	st.connects.Add(1)

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if life > 0 {
		sctx, cancel = context.WithTimeout(sctx, life)
		defer cancel()
	}
	go func() { <-sctx.Done(); _ = conn.Close() }() // unblock ReadMessage

	var (
		mu       sync.Mutex
		sentAt   = map[uint32]time.Time{} // seq -> send time, pruned on ack
		seenTick atomic.Uint64            // last snapshot tick processed, reported as "st"
	)
	// Writer: one input per period, random walk.
	go func() {
		t := time.NewTicker(time.Second / time.Duration(c.inputHz))
		defer t.Stop()
		var seq uint32
		dx, dy := int8(rand.IntN(3)-1), int8(rand.IntN(3)-1) // #nosec G115 -- rand.IntN(3)-1 is in [-1,1]
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
			}
			seq++
			if rand.IntN(10) == 0 {
				dx, dy = int8(rand.IntN(3)-1), int8(rand.IntN(3)-1) // #nosec G115 -- rand.IntN(3)-1 is in [-1,1]
			}
			if c.loss > 0 && rand.Float64() < c.loss {
				continue // simulated lost input; seq gap is visible to the server
			}
			now := time.Now()
			mu.Lock()
			sentAt[seq] = now
			mu.Unlock()
			b, _ := json.Marshal(protocol.Input{T: protocol.TypeInput, Seq: seq, Dx: dx, Dy: dy, CT: now.UnixMilli(), ST: seenTick.Load()})
			_ = conn.SetWriteDeadline(now.Add(5 * time.Second))
			if conn.WriteMessage(websocket.TextMessage, b) != nil {
				cancel()
				return
			}
			st.inputs.Add(1)
		}
	}()

	// Reader goroutine timestamps frames; the processing loop releases each one
	// at arrival+latency. This models link delay without reducing throughput.
	type frame struct {
		at time.Time
		b  []byte
	}
	// A slow bot gets an unbuffered channel so its slowness propagates to the
	// TCP socket, exactly like a slow real client.
	buf := 4096
	if slow {
		buf = 0
	}
	frames := make(chan frame, buf)
	readErr := make(chan error, 1)
	go func() {
		defer close(frames)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, b, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			frames <- frame{time.Now(), b}
		}
	}()

	var (
		rtts, gaps []time.Duration
		lastSnap   time.Time
		lastAck    uint32
	)
	defer func() { st.add(rtts, gaps) }()
	for {
		f, ok := <-frames
		if !ok {
			err := <-readErr
			if sctx.Err() != nil {
				return nil // planned end (churn or run over)
			}
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseUnsupportedData) {
				st.drops.Add(1)
			}
			return err
		}
		b := f.b
		if d := time.Until(f.at.Add(c.latency)); d > 0 {
			time.Sleep(d)
		}
		if slow {
			time.Sleep(time.Duration(200+rand.IntN(800)) * time.Millisecond)
		}
		now := time.Now()
		var head struct {
			T    string `json:"t"`
			Tick uint64 `json:"tick"`
			Ack  uint32 `json:"ack"`
		}
		if json.Unmarshal(b, &head) != nil || head.T != protocol.TypeSnap {
			continue
		}
		seenTick.Store(head.Tick)
		measuring := now.After(measureStart)
		if measuring {
			st.snapshots.Add(1)
			st.bytes.Add(int64(len(b)))
		}
		if !lastSnap.IsZero() && measuring && len(gaps) < maxSamplesPerSession {
			gaps = append(gaps, now.Sub(lastSnap))
		}
		lastSnap = now
		if head.Ack > lastAck {
			mu.Lock()
			if t, ok := sentAt[head.Ack]; ok && measuring && len(rtts) < maxSamplesPerSession {
				rtts = append(rtts, now.Sub(t))
			}
			for s := range sentAt {
				if s <= head.Ack {
					delete(sentAt, s)
				}
			}
			mu.Unlock()
			lastAck = head.Ack
		}
	}
}

func pct(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	return xs[min(len(xs)-1, int(p*float64(len(xs))))]
}

func report(c config, st *stats, before, peak, after metrics) {
	slices.Sort(st.rtts)
	slices.Sort(st.gaps)
	secs := c.duration.Seconds()
	tick := before.histDelta(after, "localarena_tick_duration_seconds")
	players := peak.gauge("localarena_players")
	sent := after.counter("localarena_ws_sent_bytes_total") - before.counter("localarena_ws_sent_bytes_total")
	r := map[string]any{
		"bots": c.bots, "duration_s": secs, "latency": c.latency.String(), "loss": c.loss, "slow": c.slow, "churn": c.churn.String(),
		"connects": st.connects.Load(), "connect_failures": st.failures.Load(), "server_drops": st.drops.Load(),
		"inputs_sent":                   st.inputs.Load(),
		"rtt_ms":                        map[string]float64{"p50": ms(pct(st.rtts, .5)), "p95": ms(pct(st.rtts, .95)), "p99": ms(pct(st.rtts, .99)), "n": float64(len(st.rtts))},
		"snapshot_gap_ms":               map[string]float64{"p50": ms(pct(st.gaps, .5)), "p95": ms(pct(st.gaps, .95)), "p99": ms(pct(st.gaps, .99))},
		"client_rx_bytes_per_s_per_bot": float64(st.bytes.Load()) / secs / float64(c.bots),
		"server": map[string]any{
			"peak_players": players, "peak_matches": peak.gauge("localarena_matches"),
			"peak_goroutines": peak.gauge("go_goroutines"), "goroutines_after": after.gauge("go_goroutines"),
			"tick_p50_ms": tick.quantile(.5) * 1000, "tick_p95_ms": tick.quantile(.95) * 1000, "tick_p99_ms": tick.quantile(.99) * 1000,
			"tick_overruns": after.counter("localarena_tick_overruns_total") - before.counter("localarena_tick_overruns_total"),
			"ticks_missed":  after.counter("localarena_ticks_missed_total") - before.counter("localarena_ticks_missed_total"),
			"snapshots_replaced": after.counterL("localarena_snapshots_total", "result", "replaced") -
				before.counterL("localarena_snapshots_total", "result", "replaced"),
			"inputs_stale": after.counterL("localarena_inputs_total", "result", "stale") -
				before.counterL("localarena_inputs_total", "result", "stale"),
			"heap_inuse_peak_mib":       peak.gauge("go_memstats_heap_inuse_bytes") / (1 << 20),
			"rss_peak_mib":              peak.gauge("process_resident_memory_bytes") / (1 << 20),
			"tx_bytes_per_s_per_player": sent / (secs + c.rampUp.Seconds()) / max(players, 1),
			"gc_cycles":                 after.counter("go_gc_duration_seconds") - before.counter("go_gc_duration_seconds"),
			"tick_start_lag_p99_ms":     before.histDelta(after, "localarena_tick_start_lag_seconds").quantile(.99) * 1000,
			"snapshots_skipped_lagging": after.counterL("localarena_snapshots_total", "result", "skipped_lagging") - before.counterL("localarena_snapshots_total", "result", "skipped_lagging"),
			"disconnects": map[string]float64{
				"write_timeout": after.counterL("localarena_disconnects_total", "reason", "write_timeout") - before.counterL("localarena_disconnects_total", "reason", "write_timeout"),
				"write_error":   after.counterL("localarena_disconnects_total", "reason", "write_error") - before.counterL("localarena_disconnects_total", "reason", "write_error"),
				"read_timeout":  after.counterL("localarena_disconnects_total", "reason", "read_timeout") - before.counterL("localarena_disconnects_total", "reason", "read_timeout"),
			},
		},
	}
	if c.jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// scrapeClient is a short-timeout client for /metrics.
var scrapeClient = &http.Client{Timeout: 5 * time.Second}

// pickRegion probes /healthz of each candidate 3 times and returns the one with
// the lowest median RTT; unreachable regions are skipped (client-side region
// selection, as a game client would do with ping beacons).
func pickRegion(cands []string) (string, map[string]time.Duration) {
	cl := &http.Client{Timeout: time.Second}
	out := map[string]time.Duration{}
	best, bestRTT := "", time.Duration(1<<62)
	for _, c := range cands {
		var samples []time.Duration
		for range 3 {
			t := time.Now()
			resp, err := cl.Get(c + "/healthz")
			if err != nil {
				break
			}
			_ = resp.Body.Close()
			samples = append(samples, time.Since(t))
		}
		if len(samples) < 3 {
			continue
		}
		slices.Sort(samples)
		out[c] = samples[1].Round(time.Millisecond)
		if samples[1] < bestRTT {
			best, bestRTT = c, samples[1]
		}
	}
	return best, out
}
