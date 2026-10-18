package server

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/goleak"

	"github.com/SanidhyaMadheshia/localArena/backend/internal/match"
	"github.com/SanidhyaMadheshia/localArena/backend/internal/protocol"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func setup(t *testing.T) (*httptest.Server, *match.Manager) {
	t.Helper()
	mgr := match.NewManager(match.Options{TickHz: 50})
	ts := httptest.NewServer(New(mgr, slog.New(slog.DiscardHandler), Config{}).Handler())
	t.Cleanup(func() {
		mgr.Close() // closes sessions first so ts.Close does not wait on them
		ts.CloseClientConnections()
		ts.Close()
	})
	return ts, mgr
}

func dial(t *testing.T, ts *httptest.Server, h http.Header) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	return c
}

func TestWebSocketWelcomeAndAck(t *testing.T) {
	ts, _ := setup(t)
	c := dial(t, ts, nil)
	var w protocol.Welcome
	if err := c.ReadJSON(&w); err != nil || w.T != protocol.TypeWelcome || w.V != protocol.Version || w.ID == 0 {
		t.Fatalf("welcome = %+v, %v", w, err)
	}
	if err := c.WriteJSON(protocol.Input{T: "input", Seq: 9, Dx: 1}); err != nil {
		t.Fatal(err)
	}
	for {
		var s protocol.Snapshot
		if err := c.ReadJSON(&s); err != nil {
			t.Fatal(err)
		}
		if s.Ack == 9 {
			var ps []protocol.PlayerState
			if err := json.Unmarshal(s.Players, &ps); err != nil || len(ps) != 1 || ps[0].ID != w.ID {
				t.Fatalf("players = %s", s.Players)
			}
			return
		}
	}
}

func TestInvalidInputClosesSession(t *testing.T) {
	ts, mgr := setup(t)
	c := dial(t, ts, nil)
	_ = c.WriteMessage(websocket.TextMessage, []byte(`{"t":"input","seq":0}`))
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			if !websocket.IsCloseError(err, websocket.CloseUnsupportedData) {
				t.Fatalf("want unsupported-data close, got %v", err)
			}
			break
		}
	}
	waitPlayers(t, mgr, 0)
}

func TestDisconnectCleansUp(t *testing.T) {
	ts, mgr := setup(t)
	a, b := dial(t, ts, nil), dial(t, ts, nil)
	waitPlayers(t, mgr, 2)
	_ = a.Close() // abrupt TCP close, no close frame
	_ = b.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	waitPlayers(t, mgr, 0)
}

func TestCrossOriginRejected(t *testing.T) {
	ts, _ := setup(t)
	_, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws",
		http.Header{"Origin": {"http://evil.example"}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin upgrade not rejected: %v", err)
	}
}

func TestLongPollWakesOnChange(t *testing.T) {
	ts, mgr := setup(t)
	st, _ := mgr.Status()
	got := make(chan match.Status, 1)
	go func() {
		resp, err := http.Get(ts.URL + "/status?wait=5s&since=" + jsonNum(st.Version))
		if err != nil {
			return
		}
		defer func() { _ = resp.Body.Close() }()
		var s match.Status
		_ = json.NewDecoder(resp.Body).Decode(&s)
		got <- s
	}()
	time.Sleep(50 * time.Millisecond)
	dial(t, ts, nil)
	select {
	case s := <-got:
		if s.Players != 1 {
			t.Fatalf("long poll returned %+v", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake")
	}
}

func TestLongPollTimesOut(t *testing.T) {
	ts, mgr := setup(t)
	st, _ := mgr.Status()
	resp, err := http.Get(ts.URL + "/status?wait=50ms&since=" + jsonNum(st.Version))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", resp.StatusCode)
	}
}

func TestSSEStreamsChanges(t *testing.T) {
	ts, _ := setup(t)
	resp, err := http.Get(ts.URL + "/status/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	readEvent := func() match.Status {
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok {
				var s match.Status
				if err := json.Unmarshal([]byte(data), &s); err != nil {
					t.Fatal(err)
				}
				return s
			}
		}
	}
	if s := readEvent(); s.Players != 0 {
		t.Fatalf("initial event %+v", s)
	}
	dial(t, ts, nil)
	if s := readEvent(); s.Players != 1 {
		t.Fatalf("change event %+v", s)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(r, 0))
}

func waitPlayers(t *testing.T, mgr *match.Manager, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st, _ := mgr.Status(); st.Players == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	st, _ := mgr.Status()
	t.Fatalf("players = %d, want %d", st.Players, n)
}

func jsonNum(v uint64) string { b, _ := json.Marshal(v); return string(b) }

// A client that reports it is far behind is throttled to ~1 snapshot/s (not cut
// off) and returns to the full rate once it reports recent ticks again.
func TestLaggingClientIsThrottled(t *testing.T) {
	ts, _ := setup(t) // 50 Hz
	c := dial(t, ts, nil)
	var w protocol.Welcome
	if err := c.ReadJSON(&w); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Time{})
	var latest, stuck atomic.Uint64 // stuck != 0: report that tick instead of the latest
	arrivals := make(chan struct{}, 1024)
	go func() {
		for {
			var s protocol.Snapshot
			if c.ReadJSON(&s) != nil {
				return
			}
			latest.Store(s.Tick)
			arrivals <- struct{}{}
		}
	}()
	done := make(chan struct{})
	defer close(done)
	go func() { // like a real client: one input per tick carrying the processed tick
		for seq := uint32(1); ; seq++ {
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
			st := latest.Load()
			if s := stuck.Load(); s != 0 {
				st = s
			}
			if c.WriteJSON(protocol.Input{T: "input", Seq: seq, ST: st}) != nil {
				return
			}
		}
	}()
	count := func(d time.Duration) (n int) {
		end := time.After(d)
		for {
			select {
			case <-arrivals:
				n++
			case <-end:
				return n
			}
		}
	}
	time.Sleep(200 * time.Millisecond)
	stuck.Store(1) // pretend we stopped processing at tick 1
	count(1500 * time.Millisecond)
	if n := count(2 * time.Second); n < 1 || n > 4 {
		t.Fatalf("lagging client got %d snapshots in 2 s, want 1-4 (throttled, not cut off)", n)
	}
	stuck.Store(0) // caught up: report real progress again
	count(1200 * time.Millisecond)
	if n := count(500 * time.Millisecond); n < 15 {
		t.Fatalf("recovered client got %d snapshots in 0.5 s, want ~25", n)
	}
}
