package server

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/pub"
)

// readFrame reads one SSE data line and decodes it into the given shape.
func readFrame(t *testing.T, body *bufio.Reader) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := body.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m); err != nil {
			t.Fatalf("frame not json: %v (%q)", err, line)
		}
		return m
	}
	t.Fatal("no frame within deadline")
	return nil
}

// TestStreamContractBatched: hello (with history) on connect, snapshots at
// 1Hz from the shared tick, and trigger batches only on ticks where the
// ring drained non-empty — never per-trigger frames.
func TestStreamContractBatched(t *testing.T) {
	s := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/api/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	body := bufio.NewReader(resp.Body)

	hello := readFrame(t, body)
	if hello["type"] != "hello" {
		t.Fatalf("first frame = %v, want hello", hello["type"])
	}
	hist, ok := hello["history"].(map[string]any)
	if !ok {
		t.Fatalf("hello.history missing: %v", hello)
	}
	for _, k := range []string{"t", "f", "l", "c", "m"} {
		if _, ok := hist[k]; !ok {
			t.Fatalf("hello.history.%s missing: %v", k, hist)
		}
	}

	// A trigger handed to HandleTrigger rides the NEXT tick's batch.
	s.HandleTrigger(pub.Trigger{
		AlertID: "0192ced1-4a1e-7abc-8def-0123456789ab", Symbol: "BTCUSDT",
		Venue: "ATLAS", Tier: "TOP", FiredPrice: "65001.00",
		FiredAtUnixNanos: 1, Direction: "ABOVE", TargetPrice: "65000.00",
	})
	sawSnapshot, sawTriggers, sawPerTrigger := false, false, false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !(sawSnapshot && sawTriggers) {
		f := readFrame(t, body)
		switch f["type"] {
		case "snapshot":
			sawSnapshot = true
			snap := f["snapshot"].(map[string]any)
			for _, k := range []string{"ticks", "ticks_per_sec", "triggers_fired", "alerts_by_state", "nats_connected", "venue_ticks", "engine", "sys"} {
				if _, ok := snap[k]; !ok {
					t.Fatalf("snapshot.%s missing", k)
				}
			}
		case "triggers":
			sawTriggers = true
			batch := f["triggers"].([]any)
			if len(batch) != 1 {
				t.Fatalf("batch len = %d, want 1", len(batch))
			}
			tr := batch[0].(map[string]any)
			if tr["alert_id"] != "0192ced1-4a1e-7abc-8def-0123456789ab" {
				t.Fatalf("trigger = %v", tr)
			}
		case "trigger":
			sawPerTrigger = true
		}
	}
	if !sawSnapshot {
		t.Fatal("no snapshot frame within deadline")
	}
	if !sawTriggers {
		t.Fatal("no triggers batch within deadline")
	}
	if sawPerTrigger {
		t.Fatal("per-trigger frames must not exist in v2")
	}
}

// TestTriggerRing: overflow keeps the last 10, drain returns oldest→newest
// and clears.
func TestTriggerRing(t *testing.T) {
	r := &triggerRing{}
	for i := 0; i < 15; i++ {
		r.push(pub.Trigger{AlertID: fmt.Sprintf("id-%02d", i)})
	}
	got := r.drain()
	if len(got) != 10 {
		t.Fatalf("drain len = %d, want 10", len(got))
	}
	if got[0].AlertID != "id-05" || got[9].AlertID != "id-14" {
		t.Fatalf("drain bounds = %s..%s, want id-05..id-14", got[0].AlertID, got[9].AlertID)
	}
	if again := r.drain(); len(again) != 0 {
		t.Fatalf("drain after drain = %d, want 0", len(again))
	}
}

// TestTriggerRingConcurrent: concurrent push + drain under -race.
func TestTriggerRingConcurrent(t *testing.T) {
	r := &triggerRing{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				r.push(pub.Trigger{})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				r.drain()
			}
		}()
	}
	wg.Wait()
}

// TestHistorySampling: 130 samples cap at 120; the sys series (cpu/rss)
// ride the same cap.
func TestHistorySampling(t *testing.T) {
	h := newHistory()
	v := statusView{
		TicksPerSec:    100,
		TriggersPerSec: 2,
		Engine:         engineView{Live: 7},
		Sys:            sysView{HostCPUPercent: 12.5, RSSBytes: 4096},
	}
	for i := 0; i < 130; i++ {
		v.Engine.Live++
		h.sample(v)
	}
	view := h.view()
	if len(view.T) != 120 || len(view.F) != 120 || len(view.L) != 120 {
		t.Fatalf("lengths = %d/%d/%d, want 120/120/120", len(view.T), len(view.F), len(view.L))
	}
	if len(view.C) != 120 || len(view.M) != 120 {
		t.Fatalf("sys lengths = %d/%d, want 120/120", len(view.C), len(view.M))
	}
	if view.C[0] != 12.5 || view.M[0] != 4096 {
		t.Fatalf("c/m = %v/%v, want 12.5/4096", view.C[0], view.M[0])
	}
	if view.T[0] != 100 || view.F[0] != 2 {
		t.Fatalf("t/f = %v/%v, want 100/2", view.T[0], view.F[0])
	}
	if got := view.L[0]; got != 18 { // 11th sample (the cap evicted the first 10): Live was 7+11
		t.Fatalf("l[0] = %v, want 18", got)
	}
}

// TestServerCloseStopsTick: after Close, no more frames are broadcast.
func TestServerCloseStopsTick(t *testing.T) {
	s := newServer(t)
	ch, remove := s.hub.add()
	defer remove()
	s.Close()
	s.Close() // idempotent
	select {
	case frame, ok := <-ch:
		if ok {
			t.Fatalf("frame after Close: %s", frame)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}

// TestHubOverflowClosesSlowClient: a client that never drains its queue is
// closed by broadcast once the queue fills — senders never block.
func TestHubOverflowClosesSlowClient(t *testing.T) {
	h := newSSEHub()
	ch, remove := h.add()
	defer remove() // must be safe after broadcast already closed the channel

	// Fill past the 64-frame queue; broadcast must not block or panic.
	for i := 0; i < sseClientQ+10; i++ {
		h.broadcast([]byte("frame"))
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("client should have been closed on overflow")
		}
	case <-time.After(time.Second):
		t.Fatal("client channel not closed after overflow")
	}
	// A later broadcast with no clients is a no-op.
	h.broadcast([]byte("frame"))
}

func TestAlertsEndpoints(t *testing.T) {
	s := newServer(t)
	up := func(symbol, venue string) string {
		resp, err := s.core.UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: symbol, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: "65000.00", Venue: venue, Tier: "TOP",
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetAlertId()
	}
	id1 := up("BTCUSDT", "ATLAS")
	up("ETHUSDT", "ATLAS")

	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/api/alerts?state=active&direction=ABOVE")
	if err != nil {
		t.Fatal(err)
	}
	var page struct {
		Total int `json:"total"`
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	if err := json.UnmarshalRead(resp.Body, &page); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("page = %+v", page)
	}

	dresp, err := ts.Client().Get(ts.URL + "/api/alerts/" + id1)
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Symbol string `json:"symbol"`
		State  string `json:"state"`
	}
	if err := json.UnmarshalRead(dresp.Body, &v); err != nil {
		t.Fatal(err)
	}
	dresp.Body.Close()
	if v.Symbol != "BTCUSDT" || v.State != "active" {
		t.Fatalf("detail = %+v", v)
	}

	nresp, err := ts.Client().Get(ts.URL + "/api/alerts/not-an-id")
	if err != nil {
		t.Fatal(err)
	}
	nresp.Body.Close()
	if nresp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", nresp.StatusCode)
	}
}
