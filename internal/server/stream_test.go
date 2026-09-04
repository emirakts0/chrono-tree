package server

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestStreamHelloSnapshotTrigger(t *testing.T) {
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
	if n, _ := hello["symbol_count"].(float64); n < 100 {
		t.Fatalf("symbol_count = %v, want the full catalog", hello["symbol_count"])
	}
	if _, ok := hello["venues"].([]any); !ok {
		t.Fatalf("hello.venues missing: %v", hello)
	}

	// Snapshots flow at ~1Hz.
	var snap map[string]any
	for {
		f := readFrame(t, body)
		if f["type"] == "snapshot" {
			snap = f["snapshot"].(map[string]any)
			break
		}
	}
	for _, k := range []string{"ticks", "ticks_per_sec", "triggers_fired", "alerts_by_state", "nats_connected", "venue_ticks", "engine"} {
		if _, ok := snap[k]; !ok {
			t.Fatalf("snapshot.%s missing", k)
		}
	}

	// A trigger handed to the hub arrives as a frame.
	go s.HandleTrigger(pub.Trigger{
		AlertID: "0192ced1-4a1e-7abc-8def-0123456789ab", Symbol: "BTCUSDT",
		Venue: "ATLAS", Tier: "TOP", FiredPrice: "65001.00",
		FiredAtUnixNanos: 1, Direction: "ABOVE", TargetPrice: "65000.00",
	})
	for {
		f := readFrame(t, body)
		if f["type"] != "trigger" {
			continue
		}
		tr := f["trigger"].(map[string]any)
		if tr["alert_id"] != "0192ced1-4a1e-7abc-8def-0123456789ab" {
			t.Fatalf("trigger frame = %v", tr)
		}
		break
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
