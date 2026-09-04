package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/emir/chrono-tree/internal/service"
)

// PromMetrics implements service.Metrics on a Prometheus registry.
type PromMetrics struct {
	ticks        *prometheus.CounterVec
	fired        *prometheus.CounterVec
	delivered    prometheus.Counter
	triggerDrops prometheus.Counter
	ticksDropped prometheus.Counter
	batch        prometheus.Histogram
	latency      prometheus.Histogram
	active       prometheus.Gauge
	watchers     prometheus.Gauge
	feed         prometheus.Gauge
}

// NewPromMetrics registers and returns the metric set. The names are
// pinned by the spec.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		ticks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_ticks_total", Help: "Ticks accepted by the engine, by venue and tier.",
		}, []string{"venue", "tier"}),
		fired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_triggers_fired_total", Help: "Alerts fired by the engine, by symbol/venue/tier.",
		}, []string{"symbol", "venue", "tier"}),
		delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_triggers_delivered_total", Help: "Triggers delivered to watchers.",
		}),
		triggerDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_trigger_drops_total", Help: "Watchers evicted for falling behind.",
		}),
		ticksDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_ticks_dropped_total", Help: "Ticks dropped for unrepresentable prices.",
		}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_batch_size", Help: "Ticks per StreamTicks batch.",
			Buckets: []float64{1, 4, 8, 16, 32, 48, 64, 96, 128},
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_latency_seconds", Help: "Tick timestamp-to-ingest latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 13), // 100µs .. ~410ms
		}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_alerts_active", Help: "Alerts currently active service-side.",
		}),
		watchers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_watchers", Help: "Connected WatchTriggers streams.",
		}),
		feed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_feed_connected", Help: "1 once a feed has ever connected.",
		}),
	}
	reg.MustRegister(m.ticks, m.fired, m.delivered, m.triggerDrops, m.ticksDropped,
		m.batch, m.latency, m.active, m.watchers, m.feed)
	return m
}

func (m *PromMetrics) Tick(venue, tier string)     { m.ticks.WithLabelValues(venue, tier).Inc() }
func (m *PromMetrics) TickDropped()                { m.ticksDropped.Inc() }
func (m *PromMetrics) TickBatch(n int)             { m.batch.Observe(float64(n)) }
func (m *PromMetrics) TickLatency(d time.Duration) { m.latency.Observe(d.Seconds()) }
func (m *PromMetrics) TriggerFired(symbol, venue, tier string) {
	m.fired.WithLabelValues(symbol, venue, tier).Inc()
}
func (m *PromMetrics) TriggerDelivered()  { m.delivered.Inc() }
func (m *PromMetrics) WatcherDrop()       { m.triggerDrops.Inc() }
func (m *PromMetrics) AlertsActive(n int) { m.active.Set(float64(n)) }
func (m *PromMetrics) Watchers(n int)     { m.watchers.Set(float64(n)) }
func (m *PromMetrics) FeedConnected(b bool) {
	if b {
		m.feed.Set(1)
	}
}

var _ service.Metrics = (*PromMetrics)(nil)
