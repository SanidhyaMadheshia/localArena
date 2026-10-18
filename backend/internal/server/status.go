package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Status is exposed three ways so they can be compared (Experiment 001):
//   GET /status                    plain JSON snapshot
//   GET /status?since=V&wait=25s   long poll: blocks until version != V
//   GET /status/stream             Server-Sent Events, one event per change

const maxWait = 30 * time.Second

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, changed := s.mgr.Status()
	if since := r.URL.Query().Get("since"); since != "" {
		v, err := strconv.ParseUint(since, 10, 64)
		if err != nil {
			http.Error(w, "bad since", http.StatusBadRequest)
			return
		}
		wait, err := time.ParseDuration(r.URL.Query().Get("wait"))
		if err != nil || wait <= 0 || wait > maxWait {
			wait = maxWait
		}
		if st.Version == v {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-changed:
				st, _ = s.mgr.Status()
			case <-t.C:
				w.WriteHeader(http.StatusNotModified)
				return
			case <-r.Context().Done():
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(st)
}

func (s *Server) handleStatusStream(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	heartbeat := time.NewTicker(15 * time.Second) // keeps proxies from idling the stream out
	defer heartbeat.Stop()
	for {
		st, changed := s.mgr.Status()
		b, _ := json.Marshal(st)
		if _, err := fmt.Fprintf(w, "id: %d\nevent: status\ndata: %s\n\n", st.Version, b); err != nil {
			return
		}
		if rc.Flush() != nil {
			return
		}
		for waiting := true; waiting; {
			select {
			case <-changed:
				waiting = false
			case <-heartbeat.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil {
					return
				}
			case <-r.Context().Done():
				return
			}
		}
	}
}
