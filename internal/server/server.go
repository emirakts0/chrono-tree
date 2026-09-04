// Package server is chronod's HTTP status surface: liveness, readiness,
// a json/v2 stats snapshot, Prometheus metrics and pprof.
package server

import (
	"embed"
	"encoding/json/v2"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// feedStaleAfter is how long the feed may be silent before readyz flips.
const feedStaleAfter = 30 * time.Second

// webFS is the committed UI bundle (internal/server/web/dist, built by
// web/build.sh). The FS root is dist, so / serves index.html and /app.js
// the bundle; `go build` needs no TS toolchain.
//
//go:embed web/dist
var webFS embed.FS

// distFS is webFS re-rooted at web/dist (embed keeps the directory prefix),
// so / serves index.html and /app.js the bundle. fs.Sub cannot fail on a
// valid embed directive.
var distFS, _ = fs.Sub(webFS, "web/dist")

type Server struct {
	core     *service.Core
	stats    *stats.Stats
	reg      *prometheus.Registry
	hub      *sseHub
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
	return &Server{core: core, stats: st, reg: reg, hub: newSSEHub()}
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
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("GET /api/alerts", s.handleAlerts)
	mux.HandleFunc("GET /api/alerts/{id}", s.handleAlert)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// Method-less "/" so it does not conflict with the method-less
	// /debug/pprof/ subtree; every more specific pattern above wins.
	mux.Handle("/", http.FileServer(http.FS(distFS)))
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

// statusSnapshot builds the current /stats document.
func (s *Server) statusSnapshot() statusView {
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
	return view
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	view := s.statusSnapshot()
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, view); err != nil {
		// Headers are sent; nothing to do but log-shape the error.
		_ = err
	}
}

// helloFrame is the on-connect vocabulary frame.
type helloFrame struct {
	Type        string   `json:"type"`
	Venues      []string `json:"venues"`
	Tiers       []string `json:"tiers"`
	SymbolCount int      `json:"symbol_count"`
}

type snapshotFrame struct {
	Type     string     `json:"type"`
	Snapshot statusView `json:"snapshot"`
}

type triggerFrame struct {
	Type    string      `json:"type"`
	Trigger pub.Trigger `json:"trigger"`
}

// HandleTrigger fans one published trigger to every connected browser.
// chronod wires it to pub.NATSPublisher.SubscribeTriggers.
func (s *Server) HandleTrigger(tr pub.Trigger) {
	frame, err := json.Marshal(triggerFrame{Type: "trigger", Trigger: tr})
	if err != nil {
		return // Trigger is strings and ints; unreachable
	}
	s.hub.broadcast(frame)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch, remove := s.hub.add()
	defer remove()

	hello, err := json.Marshal(helloFrame{
		Type:        "hello",
		Venues:      s.core.Cat.DimValues(catalog.DimVenue),
		Tiers:       s.core.Cat.DimValues(catalog.DimTier),
		SymbolCount: len(s.core.Cat.Symbols()),
	})
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", hello)
	fl.Flush()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			frame, err := json.Marshal(snapshotFrame{Type: "snapshot", Snapshot: s.statusSnapshot()})
			if err != nil {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", frame)
			fl.Flush()
		case frame, ok := <-ch:
			if !ok {
				return // overflowed (broadcast closed us) or removed
			}
			fmt.Fprintf(w, "data: %s\n\n", frame)
			fl.Flush()
		}
	}
}

type alertsReply struct {
	Total int                 `json:"total"`
	Items []service.AlertView `json:"items"`
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 50
	if v, err := strconv.Atoi(q.Get("limit")); err == nil {
		limit = min(max(v, 1), 200)
	}
	offset := 0
	if v, err := strconv.Atoi(q.Get("offset")); err == nil && v > 0 {
		offset = v
	}
	items, total := s.core.ListAlerts(service.AlertFilter{
		State: q.Get("state"), Symbol: q.Get("symbol"),
		Venue: q.Get("venue"), Tier: q.Get("tier"),
		Direction: q.Get("direction"), Limit: limit, Offset: offset,
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, alertsReply{Total: total, Items: items})
}

func (s *Server) handleAlert(w http.ResponseWriter, r *http.Request) {
	view, ok := s.core.GetAlert(r.PathValue("id"))
	if !ok {
		http.Error(w, "alert not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, view)
}
