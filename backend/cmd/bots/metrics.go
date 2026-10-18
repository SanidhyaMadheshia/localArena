package main

import (
	"fmt"
	"math"
	"net/http"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// metrics is one scrape of the server's Prometheus endpoint.
type metrics map[string]*dto.MetricFamily

func scrape(base string) (metrics, error) {
	resp, err := scrapeClient.Get(base + "/metrics")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: %s", resp.Status)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	return p.TextToMetricFamilies(resp.Body)
}

func (m metrics) first(name string) *dto.Metric {
	if f := m[name]; f != nil && len(f.Metric) > 0 {
		return f.Metric[0]
	}
	return nil
}

func (m metrics) gauge(name string) float64 {
	if x := m.first(name); x != nil {
		return x.GetGauge().GetValue()
	}
	return 0
}

func (m metrics) counter(name string) float64 {
	x := m.first(name)
	if x == nil {
		return 0
	}
	if x.Counter != nil {
		return x.GetCounter().GetValue()
	}
	if x.Summary != nil { // e.g. go_gc_duration_seconds_count is a summary's count
		return float64(x.GetSummary().GetSampleCount())
	}
	return 0
}

// counterL returns a labelled counter value.
func (m metrics) counterL(name, label, value string) float64 {
	f := m[name]
	if f == nil {
		return 0
	}
	for _, x := range f.Metric {
		for _, l := range x.Label {
			if l.GetName() == label && l.GetValue() == value {
				return x.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// hist is a cumulative histogram delta: upper bound -> count.
type hist struct {
	bounds []float64
	counts []float64
	total  float64
}

// histDelta returns the histogram observed between two scrapes.
func (m metrics) histDelta(after metrics, name string) hist {
	a, b := m.first(name), after.first(name)
	var h hist
	if b == nil {
		return h
	}
	prev := map[float64]float64{}
	var prevTotal float64
	if a != nil {
		for _, bk := range a.GetHistogram().GetBucket() {
			prev[bk.GetUpperBound()] = float64(bk.GetCumulativeCount())
		}
		prevTotal = float64(a.GetHistogram().GetSampleCount())
	}
	for _, bk := range b.GetHistogram().GetBucket() {
		h.bounds = append(h.bounds, bk.GetUpperBound())
		h.counts = append(h.counts, float64(bk.GetCumulativeCount())-prev[bk.GetUpperBound()])
	}
	h.total = float64(b.GetHistogram().GetSampleCount()) - prevTotal
	return h
}

// quantile linearly interpolates within buckets, like PromQL histogram_quantile.
// It returns 0 for an empty histogram (JSON cannot encode NaN) and the highest
// finite bound when the rank falls in the +Inf bucket.
func (h hist) quantile(q float64) float64 {
	if h.total == 0 {
		return 0
	}
	rank := q * h.total
	lower, prevCount := 0.0, 0.0
	for i, ub := range h.bounds {
		if h.counts[i] >= rank {
			if math.IsInf(ub, 1) {
				return lower
			}
			if h.counts[i] == prevCount {
				return ub
			}
			return lower + (ub-lower)*(rank-prevCount)/(h.counts[i]-prevCount)
		}
		lower, prevCount = ub, h.counts[i]
	}
	return h.bounds[len(h.bounds)-1] // beyond the last finite bucket
}
