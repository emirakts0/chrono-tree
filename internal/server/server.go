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
	"sync"
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
	ring     triggerRing
	hist     *history
	sys      *sysSampler
	latestSys atomic.Pointer[sysSample] // last sample; /metrics reads, never samples
	stopTick chan struct{}
	stopOnce sync.Once
	shutting atomic.Bool
}

// New builds the status server. dbPath is the alert-store file whose
// size and filesystem the sys metrics report; "" disables those fields.
func New(core *service.Core, st *stats.Stats, reg *prometheus.Registry, dbPath string) *Server {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "chrono_nats_connected", Help: "1 while the NATS publisher connection is up.",
	}, func() float64 {
		if core.NATSConnected() {
			return 1
		}
		return 0
	}))
	s := &Server{
		core: core, stats: st, reg: reg, hub: newSSEHub(),
		hist: newHistory(), sys: newSysSampler(dbPath), stopTick: make(chan struct{}),
	}
	registerSysGauges(reg, &s.latestSys)
	go s.runTick(s.stopTick)
	return s
}

// registerSysGauges exposes the cached sys sample as Prometheus gauges.
// They read latestSys (never sampling) so a scrape cannot perturb the
// 1 Hz sampler's CPU delta windows. The daemon runs a private registry
// without the Go collector or node_exporter — these are the only
// process/host gauges it has.
func registerSysGauges(reg *prometheus.Registry, latest *atomic.Pointer[sysSample]) {
	g := func(name, help string, f func(*sysSample) float64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name, Help: help,
		}, func() float64 {
			if s := latest.Load(); s != nil {
				return f(s)
			}
			return 0
		}))
	}
	g("chrono_process_resident_bytes", "Resident set size of chronod (engine + services).",
		func(s *sysSample) float64 { return float64(s.RSSBytes) })
	g("chrono_go_heap_bytes", "In-use Go heap (runtime.HeapAlloc).",
		func(s *sysSample) float64 { return float64(s.HeapBytes) })
	g("chrono_host_mem_used_bytes", "Host RAM in use.",
		func(s *sysSample) float64 { return float64(s.HostMemUsed) })
	g("chrono_host_mem_total_bytes", "Host RAM total.",
		func(s *sysSample) float64 { return float64(s.HostMemTotal) })
	g("chrono_host_cpu_percent", "Host CPU utilization across all cores, 0-100.",
		func(s *sysSample) float64 { return s.HostCPUPercent })
	g("chrono_process_cpu_percent", "chronod CPU use, percent of one core (can exceed 100).",
		func(s *sysSample) float64 { return s.ProcCPUPercent })
	g("chrono_cpu_cores", "Logical CPU cores available.",
		func(s *sysSample) float64 { return float64(s.Cores) })
	g("chrono_alertstore_bytes", "Alert-store (bbolt) file size.",
		func(s *sysSample) float64 { return float64(max(s.DBBytes, 0)) })
	g("chrono_disk_free_bytes", "Free space on the alert store's filesystem.",
		func(s *sysSample) float64 { return float64(s.DiskFreeBytes) })
}

// Close stops the 1s tick goroutine. Idempotent.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stopTick) })
}

// runTick is the single 1Hz heartbeat: it samples history, builds ONE
// snapshot, drains the trigger ring, and broadcasts both frames through
// the hub. Every connection relays identical frames.
func (s *Server) runTick(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			snap := s.statusSnapshot()
			s.hist.sample(snap)
			sf, err := json.Marshal(snapshotFrame{Type: "snapshot", Snapshot: snap})
			if err != nil {
				continue // statusView is plain data; unreachable
			}
			s.hub.broadcast(sf)
			if batch := s.ring.drain(); len(batch) > 0 {
				if tf, err := json.Marshal(triggersFrame{Type: "triggers", Triggers: batch}); err == nil {
					s.hub.broadcast(tf)
				}
			}
		}
	}
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
	Sys                    sysView           `json:"sys"`
}

type engineView struct {
	Live            uint64 `json:"live"`
	DroppedTriggers uint64 `json:"dropped_triggers"`
}

// sysView is the resource-usage section of the snapshot. Same shape as
// sysSample; a distinct type keeps the wire contract free of sampler
// internals.
type sysView struct {
	RSSBytes       uint64  `json:"rss_bytes"`
	HeapBytes      uint64  `json:"heap_bytes"`
	HostMemUsed    uint64  `json:"host_mem_used_bytes"`
	HostMemTotal   uint64  `json:"host_mem_total_bytes"`
	HostCPUPercent float64 `json:"host_cpu_percent"`
	ProcCPUPercent float64 `json:"proc_cpu_percent"`
	Cores          int     `json:"cpu_cores"`
	DBBytes        int64   `json:"db_bytes"`
	DiskFreeBytes  uint64  `json:"disk_free_bytes"`
	DiskTotalBytes uint64  `json:"disk_total_bytes"`
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
	sv := s.sys.sample()
	s.latestSys.Store(&sv)
	view.Sys = sysView(sv)
	return view
}

// triggerRing is the protective buffer between the NATS reader goroutine
// and the browsers: it keeps at most ringSize triggers, overwriting the
// oldest on overflow, and is drained whole by the 1s tick. A 10,000-trigger
// burst ships exactly the last 10.
const ringSize = 10

type triggerRing struct {
	mu  sync.Mutex
	buf []pub.Trigger // oldest first, len <= ringSize
}

func (r *triggerRing) push(tr pub.Trigger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == ringSize {
		copy(r.buf, r.buf[1:])
		r.buf[len(r.buf)-1] = tr
		return
	}
	r.buf = append(r.buf, tr)
}

// drain returns the buffered batch (oldest first) and clears the ring.
func (r *triggerRing) drain() []pub.Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.buf
	r.buf = nil
	return out
}

// histSamples is the retained history window: 120 one-second samples =
// two minutes, delivered in the hello frame so graphs are full at load.
const histSamples = 120

// history holds the per-second sample series backing the dashboard graphs.
type history struct {
	mu      sync.Mutex
	t, f, l []float64            // ticks/s, triggers/s, engine.live
	c, m    []float64            // host cpu %, process rss bytes
	v       map[string][]float64 // per-venue ticks/s
	lastV   map[string]uint64    // cumulative venue_ticks at last sample
}

func newHistory() *history {
	return &history{v: make(map[string][]float64), lastV: make(map[string]uint64)}
}

// historyView is the wire shape; keys are short (720 numbers per hello).
type historyView struct {
	T []float64            `json:"t"`
	F []float64            `json:"f"`
	L []float64            `json:"l"`
	C []float64            `json:"c"`
	M []float64            `json:"m"`
	V map[string][]float64 `json:"v"`
}

func appendCapped(s []float64, x float64) []float64 {
	s = append(s, x)
	if len(s) > histSamples {
		s = s[1:]
	}
	return s
}

// sample records one second of the snapshot view. Venue rates are deltas
// of the cumulative venue_ticks counters; there is no prior point for a
// venue's first sample, so it diffs against zero — which at process boot
// (counters start at 0, first tick is 1s in) is exactly that first
// second's rate.
func (h *history) sample(v statusView) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.t = appendCapped(h.t, v.TicksPerSec)
	h.f = appendCapped(h.f, v.TriggersPerSec)
	h.l = appendCapped(h.l, float64(v.Engine.Live))
	h.c = appendCapped(h.c, v.Sys.HostCPUPercent)
	h.m = appendCapped(h.m, float64(v.Sys.RSSBytes))
	for venue, total := range v.VenueTicks {
		rate := float64(total - h.lastV[venue])
		h.lastV[venue] = total
		h.v[venue] = appendCapped(h.v[venue], rate)
	}
}

func (h *history) view() historyView {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := historyView{
		T: make([]float64, len(h.t)), F: make([]float64, len(h.f)),
		L: make([]float64, len(h.l)), C: make([]float64, len(h.c)),
		M: make([]float64, len(h.m)), V: make(map[string][]float64, len(h.v)),
	}
	copy(out.T, h.t)
	copy(out.F, h.f)
	copy(out.L, h.l)
	copy(out.C, h.c)
	copy(out.M, h.m)
	for venue, s := range h.v {
		out.V[venue] = append([]float64(nil), s...)
	}
	return out
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	view := s.statusSnapshot()
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, view); err != nil {
		// Headers are sent; nothing to do but log-shape the error.
		_ = err
	}
}

// helloFrame is the on-connect vocabulary + history frame.
type helloFrame struct {
	Type        string      `json:"type"`
	Venues      []string    `json:"venues"`
	Tiers       []string    `json:"tiers"`
	SymbolCount int         `json:"symbol_count"`
	History     historyView `json:"history"`
}

type snapshotFrame struct {
	Type     string     `json:"type"`
	Snapshot statusView `json:"snapshot"`
}

type triggersFrame struct {
	Type     string        `json:"type"`
	Triggers []pub.Trigger `json:"triggers"`
}

// HandleTrigger buffers one published trigger into the ring; the 1s tick
// ships it (and up to its 9 most recent predecessors) to every browser.
// chronod wires it to pub.NATSPublisher.SubscribeTriggers.
func (s *Server) HandleTrigger(tr pub.Trigger) {
	s.ring.push(tr)
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
		History:     s.hist.view(),
	})
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", hello)
	fl.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
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
		limit = min(max(v, 1), 500)
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
