package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// fakeFeedStream drives StreamTicks without a gRPC server.
type fakeFeedStream struct {
	chronov1.FeedService_StreamTicksServer
	batches []*chronov1.TickBatch
}

func (f *fakeFeedStream) Recv() (*chronov1.TickBatch, error) {
	if len(f.batches) == 0 {
		return nil, io.EOF
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeFeedStream) SendAndClose(*chronov1.FeedStatus) error { return nil }

func newServer(t *testing.T) (*Server, *service.Core) {
	t.Helper()
	now := time.Now()
	reg := prometheus.NewRegistry()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), pub.Noop{})
	t.Cleanup(core.Close)
	return New(core, stats.New(now), reg), core
}

func TestHealthz(t *testing.T) {
	s, _ := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	s.SetShuttingDown()
	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz while shutting down = %d", resp.StatusCode)
	}
}

func TestReadyzFlipsWithFeed(t *testing.T) {
	s, core := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz before feed = %d", resp.StatusCode)
	}

	fs := &fakeFeedStream{batches: []*chronov1.TickBatch{{Ticks: []*chronov1.Tick{
		{Symbol: "BTCUSDT", Bid: "65000.00", Ask: "65000.10", Venue: "ATLAS", Tier: "TOP"},
	}}}}
	if err := core.StreamTicks(fs); err != nil {
		t.Fatal(err)
	}
	resp, _ = http.Get(ts.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz after feed = %d", resp.StatusCode)
	}
}

// The 30s staleness window, deterministically: inside a synctest bubble the
// fake clock jumps 31s in no wall-clock time, against the in-memory test
// server (Go 1.27 httptest.NewTestServer).
func TestReadyzStaleUnderSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		reg := prometheus.NewRegistry()
		core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), pub.Noop{})
		defer core.Close()
		s := New(core, stats.New(now), reg)
		ts := httptest.NewTestServer(t, s.Handler())
		defer ts.Close()

		fs := &fakeFeedStream{batches: []*chronov1.TickBatch{{Ticks: []*chronov1.Tick{{
			Symbol: "BTCUSDT", Bid: "65000.00", Ask: "65000.10", Venue: "ATLAS", Tier: "TOP",
		}}}}}
		if err := core.StreamTicks(fs); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(31 * time.Second)
		// In-memory network: requests must go through the server's client
		// (ts.URL is unset and http.Get's default transport can't reach it).
		resp, err := ts.Client().Get("http://example.com/readyz")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("readyz after 31s silence = %d, want 503", resp.StatusCode)
		}
	})
}

func TestStatsJSON(t *testing.T) {
	s, _ := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"uptime_sec", "alerts_by_state", "feed_ever_connected", "venue_ticks",
		"ticks", "ticks_per_sec", "triggers_fired", "triggers_published", "triggers_publish_dropped",
		"nats_connected", "engine"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("stats missing %q: %v", key, body)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	now := time.Now()
	reg := prometheus.NewRegistry()
	pm := NewPromMetrics(reg)
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, stats.New(now), pub.Noop{})
	t.Cleanup(core.Close)
	s := New(core, stats.New(now), reg)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	pm.Tick("ATLAS", "TOP")
	pm.TriggerFired("BTCUSDT", "ATLAS", "TOP")
	pm.TriggerPublished()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	text := string(b)
	for _, name := range []string{
		"chrono_ticks_total", "chrono_triggers_fired_total", "chrono_triggers_published_total",
		"chrono_triggers_publish_dropped_total", "chrono_ticks_dropped_total", "chrono_alerts_active",
		"chrono_nats_connected", "chrono_feed_connected", "chrono_tick_batch_size", "chrono_tick_latency_seconds",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("metrics missing %s", name)
		}
	}
	if !strings.Contains(text, `chrono_ticks_total{tier="TOP",venue="ATLAS"}`) {
		t.Fatalf("tick labels wrong:\n%s", text)
	}
}
