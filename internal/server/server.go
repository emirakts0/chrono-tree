// Package server is chronod's HTTP status surface: liveness, readiness,
// a json/v2 stats snapshot, Prometheus metrics and pprof.
package server

import (
	"encoding/json/v2"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// feedStaleAfter is how long the feed may be silent before readyz flips.
const feedStaleAfter = 30 * time.Second

type Server struct {
	core     *service.Core
	stats    *stats.Stats
	reg      *prometheus.Registry
	shutting atomic.Bool
}

func New(core *service.Core, st *stats.Stats, reg *prometheus.Registry) *Server {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "chrono_nats_connected", Help: "1 while the NATS publisher connection is up.",
	}, func() float64 {
		if core.NATSConnected() {
			return 1
		}
		return 0
	}))
	return &Server{core: core, stats: st, reg: reg}
}

// SetShuttingDown flips /healthz and /readyz to 503.
func (s *Server) SetShuttingDown() { s.shutting.Store(true) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s.shutting.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// ready: not shutting down, and a feed has connected within the stale
// window. Engine and catalogs are construction-order facts (NewCore
// panics or succeeds before the server ever listens).
func (s *Server) ready() bool {
	if s.shutting.Load() || !s.core.FeedEverConnected() {
		return false
	}
	last := s.core.FeedLastSeen()
	if last.IsZero() || time.Since(last) > feedStaleAfter {
		return false
	}
	return true
}

// statusView is the /stats JSON document.
type statusView struct {
	UptimeSec              float64           `json:"uptime_sec"`
	AlertsByState          map[string]int    `json:"alerts_by_state"`
	FeedEverConnected      bool              `json:"feed_ever_connected"`
	FeedLastSeenMsAgo      int64             `json:"feed_last_seen_ms_ago"` // -1 = never
	NATSConnected          bool              `json:"nats_connected"`
	VenueTicks             map[string]uint64 `json:"venue_ticks"`
	Ticks                  uint64            `json:"ticks"`
	TicksPerSec            float64           `json:"ticks_per_sec"`
	TicksDropped           uint64            `json:"ticks_dropped"`
	TriggersFired          uint64            `json:"triggers_fired"`
	TriggersPerSec         float64           `json:"triggers_per_sec"`
	TriggersPublished      uint64            `json:"triggers_published"`
	TriggersPublishDropped uint64            `json:"triggers_publish_dropped"`
	Engine                 engineView        `json:"engine"`
}

type engineView struct {
	Live            uint64 `json:"live"`
	DroppedTriggers uint64 `json:"dropped_triggers"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	snap := s.stats.Snapshot(now)
	es := s.core.Engine().Stats()
	view := statusView{
		UptimeSec:              snap.UptimeSec,
		AlertsByState:          s.core.AlertsByState(),
		FeedEverConnected:      s.core.FeedEverConnected(),
		FeedLastSeenMsAgo:      -1,
		NATSConnected:          s.core.NATSConnected(),
		VenueTicks:             s.core.VenueTicks(),
		Ticks:                  snap.Ticks,
		TicksPerSec:            snap.TicksPerSec,
		TicksDropped:           snap.TicksDropped,
		TriggersFired:          snap.TriggersFired,
		TriggersPerSec:         snap.TriggersPerSec,
		TriggersPublished:      snap.TriggersPublished,
		TriggersPublishDropped: snap.TriggersPublishDropped,
		Engine:                 engineView{Live: es.Live, DroppedTriggers: es.DroppedTriggers},
	}
	if last := s.core.FeedLastSeen(); !last.IsZero() {
		view.FeedLastSeenMsAgo = time.Since(last).Milliseconds()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, view); err != nil {
		// Headers are sent; nothing to do but log-shape the error.
		_ = err
	}
}
