// Command alertsink is the lab "pager": it receives Alertmanager webhooks on
// /page and /ticket, logs them, and lists recent notifications at GET /alerts.
// It stands in for PagerDuty/Slack so alert delivery is testable offline.
package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type note struct {
	Channel  string            `json:"channel"`
	Status   string            `json:"status"`
	Alert    string            `json:"alert"`
	Labels   map[string]string `json:"labels"`
	Summary  string            `json:"summary"`
	Runbook  string            `json:"runbook"`
	Received time.Time         `json:"received"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "alertsink")
	var mu sync.Mutex
	var recent []note
	recv := func(channel string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var msg struct {
				Alerts []struct {
					Status      string            `json:"status"`
					Labels      map[string]string `json:"labels"`
					Annotations map[string]string `json:"annotations"`
				} `json:"alerts"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&msg); err != nil {
				http.Error(w, "bad payload", http.StatusBadRequest)
				return
			}
			mu.Lock()
			for _, a := range msg.Alerts {
				n := note{channel, a.Status, a.Labels["alertname"], a.Labels, a.Annotations["summary"], a.Annotations["runbook_url"], time.Now()}
				recent = append(recent, n)
				log.Warn("notification", "channel", channel, "status", n.Status, "alert", n.Alert, "summary", n.Summary)
			}
			if len(recent) > 500 {
				recent = recent[len(recent)-500:]
			}
			mu.Unlock()
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /page", recv("page"))
	mux.HandleFunc("POST /ticket", recv("ticket"))
	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(recent)
	})
	srv := &http.Server{Addr: ":9095", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}
