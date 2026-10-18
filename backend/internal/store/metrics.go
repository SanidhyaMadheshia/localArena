package store

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// PoolCollector exports pgxpool statistics so pool exhaustion is visible:
// acquire wait time and "empty acquires" (callers that had to wait) rise
// before requests start timing out.
type PoolCollector struct {
	pools                                                         map[string]*pgxpool.Pool
	total, idle, acquired, maxConns, acquires, emptyAcq, waitSecs *prometheus.Desc
}

// NewPoolCollector returns a collector for the named pools.
func NewPoolCollector(pools map[string]*pgxpool.Pool) *PoolCollector {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("localarena_db_pool_"+name, help, []string{"pool"}, nil)
	}
	return &PoolCollector{
		pools:    pools,
		total:    d("conns", "Open connections."),
		idle:     d("idle_conns", "Idle connections."),
		acquired: d("acquired_conns", "Connections in use."),
		maxConns: d("max_conns", "Configured maximum connections."),
		acquires: d("acquires_total", "Successful acquires."),
		emptyAcq: d("empty_acquires_total", "Acquires that had to wait because no idle connection was available."),
		waitSecs: d("acquire_wait_seconds_total", "Total time spent waiting to acquire."),
	}
}

// Describe implements prometheus.Collector.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.total, c.idle, c.acquired, c.maxConns, c.acquires, c.emptyAcq, c.waitSecs} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	for name, p := range c.pools {
		s := p.Stat()
		g := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, name)
		}
		k := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, name)
		}
		g(c.total, float64(s.TotalConns()))
		g(c.idle, float64(s.IdleConns()))
		g(c.acquired, float64(s.AcquiredConns()))
		g(c.maxConns, float64(s.MaxConns()))
		k(c.acquires, float64(s.AcquireCount()))
		k(c.emptyAcq, float64(s.EmptyAcquireCount()))
		k(c.waitSecs, s.AcquireDuration().Seconds())
	}
}
