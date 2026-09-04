# Monitoring UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A live bento-grid monitoring dashboard embedded in chronod: SSE stream of stats snapshots + live triggers (via NATS loopback), read-only alert inquiry, served from `:8080`.

**Architecture:** `pub.NATSPublisher.SubscribeTriggers` re-consumes our own published trigger stream on the existing nats.go connection. An SSE hub in `internal/server` fans trigger frames to browsers (bounded per-client queues, drop-not-block) and ticks a 1s snapshot frame. `service.Core` gains read-only alert accessors over its existing catalog map. The frontend is no-build vanilla TypeScript: `tsc --noEmit` + `esbuild` bundle committed to `dist/app.js`, `go:embed`-ed into the binary (same pattern as committed proto stubs).

**Tech Stack:** Go 1.27, existing nats.go/json-v2 stack; TypeScript (strict), esbuild + tsc as external standalone binaries — no node_modules, no new Go dependencies.

**Spec:** `docs/superpowers/specs/2026-09-04-monitoring-ui-design.md`

## Global Constraints

- **Engine and price packages are FROZEN:** zero diffs to `engine/` and `price/`. Verify after every task: `git diff --stat main -- engine price` is empty.
- **Read-only:** the UI and its endpoints create, cancel, and mutate nothing. No write path may be added to `internal/server` or the browser code.
- **`Publisher` interface unchanged:** `Publish/Connected/Close`. `SubscribeTriggers` is a concrete method on `*NATSPublisher` only.
- **SSE contract exactly:** one endpoint `GET /api/stream`; frames `data: {json}\n\n`; `{"type":"hello","venues":[...],"tiers":[...],"symbol_count":N}` on connect; `{"type":"snapshot","snapshot":{...}}` every 1s (fields = the `/stats` view verbatim); `{"type":"trigger","trigger":{...pub.Trigger body...}}` on NATS delivery.
- **Inquiry endpoints exactly:** `GET /api/alerts?state=&symbol=&venue=&tier=&direction=&limit=&offset=` → `{"total":N,"items":[...]}`, sorted `created_at` desc, limit default 50 / clamp 1–200, offset ≥ 0; `GET /api/alerts/{id}` → detail or 404. JSON via `encoding/json/v2` (Go 1.27: imports bind as `json`, no alias; no `NewDecoder` — use `UnmarshalRead`).
- **Palette verbatim** (CSS `:root` custom properties): `--onyx:#161515`, `--graphite:#2E2D29`, `--soft-linen:#F3EFE7`, `--linen:#F4EEE5`, `--brick-ember:#D40000`, `--pine-teal:#164D44`, `--coffee-bean:#1E1917`, `--silver:#BFB3AD`, `--molten-lava:#770B0C`, `--light-green:#7FDF74`. Contrast rule: `light-green` never sits on bare linen — only against `coffee-bean` or `pine-teal` chips.
- **No slow-client blocking:** per-browser frame queue is bounded (64); overflow closes that connection. Senders never block.
- **UI bundle is committed:** `internal/server/web/dist/app.js` is a committed artifact; `go build ./...` must never require the TS toolchain. Rebuild only via `internal/server/web/build.sh`.
- **No external fonts/CDNs, no chart libraries:** the dashboard is fully offline; sparklines are inline SVG.
- **Task 4 REQUIRES the frontend-design skill** for the visual/CSS/layout work; this plan pins the contract, palette, layout structure, and behavior.
- Every task: `go build ./... && go vet ./...` clean; tests green before commit (`-race`).
- Commit style: `feat(...)`, `test(...)`, `docs(...)`, `refactor(...)`; trailer `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Never `git add -A` (untracked scratch dirs `.idea/`, `graphify-out/` live in the worktree).

---

### Task 1: `pub.SubscribeTriggers` — NATS loopback subscription

**Files:**
- Modify: `internal/pub/pub.go`
- Modify: `internal/pub/pub_test.go`

**Interfaces:**
- Consumes: existing `NATSPublisher`, `Trigger`, `Subject` contract, `pubtest`.
- Produces (Tasks 3/5 rely on exactly this): `func (p *NATSPublisher) SubscribeTriggers(fn func(Trigger)) error` — callable once per publisher; second call errors. The handler runs on nats.go's reader goroutine; it must never block (Task 3's hub honors this).

- [ ] **Step 1: Write the failing test** — append to `internal/pub/pub_test.go`:

```go
func TestSubscribeTriggersRoundTrip(t *testing.T) {
	url := pubtest.Start(t)
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	got := make(chan Trigger, 4)
	if err := p.SubscribeTriggers(func(tr Trigger) { got <- tr }); err != nil {
		t.Fatal(err)
	}
	if err := p.SubscribeTriggers(func(Trigger) {}); err == nil {
		t.Fatal("second SubscribeTriggers should error")
	}

	// Publish on a second connection (the loopback the dashboard uses).
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err := p.Publish(sampleTrigger()); err != nil {
		t.Fatal(err)
	}

	select {
	case tr := <-got:
		if tr != sampleTrigger() {
			t.Fatalf("received = %+v, want %+v", tr, sampleTrigger())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not receive the published trigger")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/pub/ -run SubscribeTriggers -v`
Expected: FAIL — `SubscribeTriggers` undefined.

- [ ] **Step 3: Implement.** In `internal/pub/pub.go`: add `sub *nats.Subscription` to `NATSPublisher`, import `"errors"`, and add:

```go
// SubscribeTriggers re-consumes our own published trigger stream — the
// NATS loopback the monitoring dashboard uses. fn runs on nats.go's
// reader goroutine and must never block. The subscription rides the
// publisher's connection: re-established across reconnects, gone at
// Close. Only one subscription per publisher.
func (p *NATSPublisher) SubscribeTriggers(fn func(Trigger)) error {
	if p.sub != nil {
		return errors.New("pub: triggers already subscribed")
	}
	sub, err := p.nc.Subscribe("chrono.triggers.>", func(m *nats.Msg) {
		var tr Trigger
		if err := json.Unmarshal(m.Data, &tr); err != nil {
			return // not a trigger payload; ignore
		}
		fn(tr)
	})
	if err != nil {
		return fmt.Errorf("subscribe chrono.triggers.>: %w", err)
	}
	p.sub = sub
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/pub/ -race -count=1`
Expected: PASS (all six tests).

- [ ] **Step 5: Commit**

```bash
git add internal/pub/
git commit -m "feat(pub): SubscribeTriggers — NATS loopback for the dashboard"
```

---

### Task 2: Alert inquiry accessors on `service.Core`

**Files:**
- Modify: `internal/service/core.go`
- Create: `internal/service/alerts_test.go`

**Interfaces:**
- Consumes: existing `Alert`, `alerts` map, `parseAlertID`, `alertIDString`, `directionOf`, `price.Format`.
- Produces (Task 3 relies on exactly these):

```go
// internal/service
type AlertView struct {
	ID                 string `json:"id"`
	Symbol             string `json:"symbol"`
	Venue              string `json:"venue"`
	Tier               string `json:"tier"`
	PriceType          string `json:"price_type"`
	Direction          string `json:"direction"`
	TargetPrice        string `json:"target_price"`
	State              string `json:"state"`
	ValidFromUnixNanos int64  `json:"valid_from_unix_nanos"`
	ExpiresUnixNanos   int64  `json:"expires_unix_nanos"`
	CreatedAtUnixNanos int64  `json:"created_at_unix_nanos"`
}
type AlertFilter struct {
	State, Symbol, Venue, Tier, Direction string
	Limit, Offset                         int
}
func (c *Core) GetAlert(id string) (AlertView, bool)
func (c *Core) ListAlerts(f AlertFilter) (items []AlertView, total int)
```

- [ ] **Step 1: Write the failing test** — create `internal/service/alerts_test.go`:

```go
package service

import (
	"context"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
)

// seedThree registers three alerts: one stays active, one is triggered by
// a tick, one is cancelled. Returns their string ids (active, triggered,
// cancelled) in that order.
func seedThree(t *testing.T, e *testEnv) (string, string, string) {
	t.Helper()
	venues := catalog.Default().DimValues(catalog.DimVenue)
	upsert := func(symbol string, venue string, i int64) string {
		resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: symbol, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:      chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice:    "65000.00", Venue: venue, Tier: "TOP",
			ValidFromUnixNanos: i, ExpiresUnixNanos: i + 1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetAlertId()
	}
	active := upsert("BTCUSDT", venues[0], 100)
	triggered := upsert("ETHUSDT", venues[0], 200)
	cancelled := upsert("BTCUSDT", venues[len(venues)-1], 300)

	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("ETHUSDT", "65000.00", "65001.00", venues[0], "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.core.AlertsByState()["triggered"] == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	ctx := context.Background()
	if _, err := e.alerts().CancelAlert(ctx, &chronov1.CancelAlertRequest{AlertId: cancelled}); err != nil {
		t.Fatal(err)
	}
	return active, triggered, cancelled
}

func TestListAlertsFilters(t *testing.T) {
	e := newEnv(t)
	active, triggered, cancelled := seedThree(t, e)

	// No filter: everything, newest first.
	items, total := e.core.ListAlerts(AlertFilter{})
	if total != 3 || len(items) != 3 {
		t.Fatalf("total=%d len=%d, want 3/3", total, len(items))
	}
	if items[0].Symbol != "BTCUSDT" || items[0].ID != cancelled {
		t.Fatalf("newest first violated: %+v", items[0])
	}

	byState := func(s string) int {
		_, n := e.core.ListAlerts(AlertFilter{State: s})
		return n
	}
	if byState("active") != 1 || byState("triggered") != 1 || byState("cancelled") != 1 {
		t.Fatalf("state totals: active=%d triggered=%d cancelled=%d",
			byState("active"), byState("triggered"), byState("cancelled"))
	}

	if _, n := e.core.ListAlerts(AlertFilter{Symbol: "BTCUSDT"}); n != 2 {
		t.Fatalf("symbol filter total = %d, want 2", n)
	}
	venues := catalog.Default().DimValues(catalog.DimVenue)
	if _, n := e.core.ListAlerts(AlertFilter{Venue: venues[0]}); n != 2 {
		t.Fatalf("venue filter total = %d, want 2", n)
	}
	if _, n := e.core.ListAlerts(AlertFilter{Direction: "ABOVE"}); n != 3 {
		t.Fatalf("direction ABOVE total = %d, want 3", n)
	}
	if _, n := e.core.ListAlerts(AlertFilter{Direction: "SIDEWAYS"}); n != 0 {
		t.Fatalf("unknown direction total = %d, want 0", n)
	}

	// Pagination is over the filtered set, after sorting.
	page, total := e.core.ListAlerts(AlertFilter{Limit: 2, Offset: 0})
	if total != 3 || len(page) != 2 {
		t.Fatalf("page1 total=%d len=%d, want 3/2", total, len(page))
	}
	page2, _ := e.core.ListAlerts(AlertFilter{Limit: 2, Offset: 2})
	if len(page2) != 1 {
		t.Fatalf("page2 len = %d, want 1", len(page2))
	}
}

func TestGetAlert(t *testing.T) {
	e := newEnv(t)
	active, _, _ := seedThree(t, e)

	v, ok := e.core.GetAlert(active)
	if !ok {
		t.Fatal("GetAlert should resolve a known id")
	}
	if v.Symbol != "BTCUSDT" || v.State != "active" || v.TargetPrice != "65000.00" {
		t.Fatalf("view = %+v", v)
	}
	if v.PriceType != "ASK" || v.Direction != "ABOVE" {
		t.Fatalf("price_type/direction = %s/%s", v.PriceType, v.Direction)
	}
	if v.ValidFromUnixNanos != 100 || v.ExpiresUnixNanos != 1100 {
		t.Fatalf("valid_from/expires = %d/%d", v.ValidFromUnixNanos, v.ExpiresUnixNanos)
	}
	if _, ok := e.core.GetAlert("0192ced1-4a1e-7abc-8def-0123456789ab"); ok {
		t.Fatal("unknown id should not resolve")
	}
	if _, ok := e.core.GetAlert("not-an-id"); ok {
		t.Fatal("malformed id should not resolve")
	}
}
```

Note: `seedThree` relies on the existing `runTicks`/`tick` helpers from the package's other tests, and on `CancelAlert` accepting the string id form (it does — `parseAlertID`). Check the helper signatures in `internal/service` before writing; adapt call shape to the real ones (the pattern is exactly what `pub_test.go` uses).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/service/ -run 'TestListAlertsFilters|TestGetAlert' -v`
Expected: FAIL — `AlertView`/`AlertFilter`/`GetAlert`/`ListAlerts` undefined.

- [ ] **Step 3: Implement.** In `internal/service/core.go`:

Add two fields to `Alert` (after `TargetPrice`):

```go
	ValidFrom int64
	Expires   int64
```

In `UpsertAlert`, set them on the entry (they are currently forwarded to the engine and dropped):

```go
		entry := &Alert{
			ID: id, Symbol: sym.Name, Decimals: sym.Decimals,
			Venue: req.GetVenue(), Tier: req.GetTier(),
			PriceType: pt, Direction: dir, TargetPrice: engine.Price(base),
			ValidFrom: req.GetValidFromUnixNanos(), Expires: req.GetExpiresUnixNanos(),
			State: StateActive, CreatedAt: c.now(),
		}
```

Add the inquiry surface at the bottom of the file:

```go
// AlertView is the read-only shape of a catalog alert for the monitoring
// surface. Prices are decimal strings, ready to render.
type AlertView struct {
	ID                 string `json:"id"`
	Symbol             string `json:"symbol"`
	Venue              string `json:"venue"`
	Tier               string `json:"tier"`
	PriceType          string `json:"price_type"`
	Direction          string `json:"direction"`
	TargetPrice        string `json:"target_price"`
	State              string `json:"state"`
	ValidFromUnixNanos int64  `json:"valid_from_unix_nanos"`
	ExpiresUnixNanos   int64  `json:"expires_unix_nanos"`
	CreatedAtUnixNanos int64  `json:"created_at_unix_nanos"`
}

// AlertFilter selects catalog alerts for the inquiry API. Empty string
// fields match anything; Direction is "ABOVE" or "BELOW" (anything else
// matches nothing). Limit/Offset are applied after sorting; Limit <= 0
// means no cap.
type AlertFilter struct {
	State, Symbol, Venue, Tier, Direction string
	Limit, Offset                         int
}

func priceTypeString(pt engine.PriceType) string {
	switch pt {
	case engine.PriceBid:
		return "BID"
	case engine.PriceAsk:
		return "ASK"
	case engine.PriceMid:
		return "MID"
	default:
		return "LAST"
	}
}

func (a *Alert) view() AlertView {
	return AlertView{
		ID:                 alertIDString(a.ID),
		Symbol:             a.Symbol,
		Venue:              a.Venue,
		Tier:               a.Tier,
		PriceType:          priceTypeString(a.PriceType),
		Direction:          directionOf(a.Direction),
		TargetPrice:        price.Format(int64(a.TargetPrice), a.Decimals),
		State:              string(a.State),
		ValidFromUnixNanos: a.ValidFrom,
		ExpiresUnixNanos:   a.Expires,
		CreatedAtUnixNanos: a.CreatedAt.UnixNano(),
	}
}

// GetAlert resolves one alert by its string id (the form UpsertAlert
// returned). ok is false for unknown or malformed ids.
func (c *Core) GetAlert(id string) (AlertView, bool) {
	raw, err := parseAlertID(id)
	if err != nil {
		return AlertView{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	a, ok := c.alerts[raw]
	if !ok {
		return AlertView{}, false
	}
	return a.view(), true
}

// ListAlerts returns one page of the filtered catalog, newest first,
// plus the total match count.
func (c *Core) ListAlerts(f AlertFilter) ([]AlertView, int) {
	dirOK := true
	var dir engine.Direction
	switch f.Direction {
	case "":
	case "ABOVE":
		dir = engine.DirGTE
	case "BELOW":
		dir = engine.DirLTE
	default:
		dirOK = false
	}
	c.mu.RLock()
	views := make([]AlertView, 0, len(c.alerts))
	for _, a := range c.alerts {
		if !dirOK {
			break
		}
		if f.State != "" && string(a.State) != f.State {
			continue
		}
		if f.Symbol != "" && a.Symbol != f.Symbol {
			continue
		}
		if f.Venue != "" && a.Venue != f.Venue {
			continue
		}
		if f.Tier != "" && a.Tier != f.Tier {
			continue
		}
		if f.Direction != "" && a.Direction != dir {
			continue
		}
		views = append(views, a.view())
	}
	c.mu.RUnlock()

	slices.SortStableFunc(views, func(a, b AlertView) int {
		if c := cmp.Compare(b.CreatedAtUnixNanos, a.CreatedAtUnixNanos); c != 0 {
			return c // newest first
		}
		return strings.Compare(b.ID, a.ID) // deterministic tiebreak; map order is random
	})
	total := len(views)
	start := min(max(f.Offset, 0), total)
	end := total
	if f.Limit > 0 {
		end = min(start+f.Limit, total)
	}
	return views[start:end], total
}
```

Imports to add: `"cmp"`, `"slices"`, `"strings"`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/service/ -race -count=1`
Expected: PASS (whole package, including the new tests).

- [ ] **Step 5: Commit**

```bash
git add internal/service/core.go internal/service/alerts_test.go
git commit -m "feat(service): read-only alert inquiry accessors"
```

---

### Task 3: SSE hub + `/api/stream` + `/api/alerts` endpoints

**Files:**
- Create: `internal/server/hub.go`
- Create: `internal/server/stream_test.go`
- Modify: `internal/server/server.go`

**Interfaces:**
- Consumes: Task 2's `AlertView`/`AlertFilter`/`GetAlert`/`ListAlerts`; `pub.Trigger`.
- Produces (Tasks 4/5 rely on exactly these): `func (s *Server) HandleTrigger(tr pub.Trigger)` (chronod wires it to `SubscribeTriggers`); routes `GET /api/stream`, `GET /api/alerts`, `GET /api/alerts/{id}` on the existing mux.

- [ ] **Step 1: Write the failing test** — create `internal/server/stream_test.go`:

```go
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
	s := newServer(t) // existing helper in server_test.go
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
			Direction: chronov1.Direction_DIRECTION_ABOVE,
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
```

Before writing, read `internal/server/server_test.go` and adapt: the helper may be named differently, and `Server` must expose `core` to the test package (same package, so unexported fields are fine — assert the helper returns something with `.Handler()` and `.core`; if the helper returns `*http.Handler` instead of `*Server`, refactor it to return `*Server` and adjust existing callers).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/server/ -run 'TestStream|TestAlertsEndpoints' -v`
Expected: FAIL — no such routes (`/api/...` hits 404) and `HandleTrigger` undefined.

- [ ] **Step 3: Create `internal/server/hub.go`**

```go
package server

import (
	"sync"

	"github.com/emir/chrono-tree/internal/pub"
)

// sseClientQ bounds the frames buffered per browser. A client that falls
// behind is closed (its browser reconnects) — senders never block.
const sseClientQ = 64

// sseHub fans trigger frames out to connected browsers.
type sseHub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{subs: make(map[chan []byte]struct{})}
}

// add registers a client and returns its frame channel plus remove. The
// channel is closed exactly once — by remove on disconnect, or by
// broadcast on overflow, whichever comes first (both hold mu and only
// close members still in the map).
func (h *sseHub) add() (<-chan []byte, func()) {
	ch := make(chan []byte, sseClientQ)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	remove := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
	return ch, remove
}

// broadcastTrigger fans one pre-marshaled frame to every client. Runs on
// the NATS reader goroutine — must never block.
func (h *sseHub) broadcast(frame []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- frame:
		default: // slow client: drop it, it will reconnect
			delete(h.subs, ch)
			close(ch)
		}
	}
}
```

- [ ] **Step 4: Modify `internal/server/server.go`**

1. `Server` struct gains `hub *sseHub`; `New` sets `hub: newSSEHub()`.
2. `Handler()` mux gains:

```go
		mux.HandleFunc("GET /api/stream", s.handleStream)
		mux.HandleFunc("GET /api/alerts", s.handleAlerts)
		mux.HandleFunc("GET /api/alerts/{id}", s.handleAlert)
```

3. Extract the `handleStats` view-building body into a method (the JSON body of `/stats` and the snapshot frame must be the same document):

```go
// statusSnapshot builds the current /stats document.
func (s *Server) statusSnapshot() statusView { /* body moved from handleStats verbatim */ }
```

`handleStats` becomes: marshal `json.MarshalWrite(w, s.statusSnapshot())`.

4. Add the stream + inquiry handlers and the trigger entry point:

```go
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
```

Imports to add in server.go: `"fmt"`, `"strconv"`, `"github.com/emir/chrono-tree/internal/catalog"`, `"github.com/emir/chrono-tree/internal/pub"`.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/server/ -race -count=1`
Expected: PASS (whole package).

- [ ] **Step 6: Commit**

```bash
git add internal/server/
git commit -m "feat(server): SSE stream hub and alert inquiry endpoints"
```

---

### Task 4: The dashboard (TypeScript, embedded)

> **REQUIRED SUB-SKILL: superpowers:frontend-design** — apply it for all visual/CSS/layout decisions in this task. This task's code below pins the contract, structure, palette tokens, grid, and behavior; the skill's checklist governs polish (spacing rhythm, type scale, motion, states). Stay inside the pinned palette and layout.

**Files:**
- Create: `internal/server/web/index.html`
- Create: `internal/server/web/build.sh` (chmod +x)
- Create: `internal/server/web/src/app.ts`
- Create: `internal/server/web/src/store.ts`
- Create: `internal/server/web/src/api.ts`
- Create: `internal/server/web/src/components/layout.ts`
- Create: `internal/server/web/src/components/cards.ts`
- Create: `internal/server/web/src/components/inquiry.ts`
- Create: `internal/server/web/src/style.css`
- Create: `internal/server/web/dist/app.js` (built artifact, committed)
- Modify: `internal/server/server.go` (`go:embed` + `GET /`)
- Modify: `internal/server/server_test.go` (asset serving test)

**Interfaces:**
- Consumes: Task 3's `/api/stream` frames and `/api/alerts` endpoints; palette/layout from the spec.
- Produces: `GET /` serves `index.html`; `GET /app.js` serves the bundle; the binary is self-contained.

- [ ] **Step 1: Scaffold** — `internal/server/web/index.html`:

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>chrono — monitor</title>
</head>
<body>
<div id="app"></div>
<script src="/app.js"></script>
</body>
</html>
```

`internal/server/web/build.sh` (then `chmod +x`):

```sh
#!/usr/bin/env bash
# Rebuild the committed UI bundle. Requires the standalone esbuild and tsc
# binaries on PATH — no node_modules, no package.json. The bundle is a
# committed artifact (same posture as `buf generate` for proto stubs):
# `go build` never runs this.
set -euo pipefail
cd "$(dirname "$0")"
tsc --noEmit
esbuild src/app.ts --bundle --minify --target=es2022 --outfile=dist/app.js
echo "wrote dist/app.js"
```

- [ ] **Step 2: Write the TypeScript source**

`src/store.ts` — the client state; types mirror the server frames exactly:

```ts
export interface Snapshot {
  uptime_sec: number;
  alerts_by_state: Record<string, number>;
  feed_ever_connected: boolean;
  feed_last_seen_ms_ago: number;
  nats_connected: boolean;
  venue_ticks: Record<string, number>;
  ticks: number;
  ticks_per_sec: number;
  ticks_dropped: number;
  triggers_fired: number;
  triggers_per_sec: number;
  triggers_published: number;
  triggers_publish_dropped: number;
  engine: { live: number; dropped_triggers: number };
}

export interface Trigger {
  alert_id: string;
  symbol: string;
  venue: string;
  tier: string;
  fired_price: string;
  fired_at_unix_nanos: number;
  direction: "ABOVE" | "BELOW";
  target_price: string;
}

export interface Hello {
  venues: string[];
  tiers: string[];
  symbol_count: number;
}

export const state = {
  hello: null as Hello | null,
  snapshot: null as Snapshot | null,
  triggers: [] as Trigger[], // newest first
};

/** Rolling ticks/s series for the pulse sparkline (120 samples = 2 min). */
export const tickSeries: number[] = [];

export function pushTrigger(tr: Trigger): void {
  state.triggers.unshift(tr);
  if (state.triggers.length > 200) state.triggers.pop();
}
```

`src/api.ts`:

```ts
export interface AlertRow {
  id: string;
  symbol: string;
  venue: string;
  tier: string;
  price_type: string;
  direction: string;
  target_price: string;
  state: string;
  valid_from_unix_nanos: number;
  expires_unix_nanos: number;
  created_at_unix_nanos: number;
}

export interface AlertsPage {
  total: number;
  items: AlertRow[];
}

export async function fetchAlerts(q: Record<string, string>): Promise<AlertsPage> {
  const params = new URLSearchParams(
    Object.entries(q).filter(([, v]) => v !== "")
  );
  const res = await fetch(`/api/alerts?${params.toString()}`);
  if (!res.ok) throw new Error(`alerts: HTTP ${res.status}`);
  return (await res.json()) as AlertsPage;
}
```

`src/app.ts` — entry; `EventSource` reconnects on its own:

```ts
import "./style.css";
import { state, tickSeries, pushTrigger } from "./store";
import type { Hello, Snapshot, Trigger } from "./store";
import { mount, update } from "./components/layout";
import { mountInquiry } from "./components/inquiry";

function main(): void {
  mountInquiry(mount(document.getElementById("app")!));
  const es = new EventSource("/api/stream");
  es.onmessage = (ev: MessageEvent<string>) => {
    const frame = JSON.parse(ev.data) as { type: string } & Record<string, unknown>;
    switch (frame.type) {
      case "hello":
        state.hello = frame as unknown as Hello;
        break;
      case "snapshot":
        state.snapshot = frame.snapshot as Snapshot;
        tickSeries.push(state.snapshot.ticks_per_sec);
        if (tickSeries.length > 120) tickSeries.shift();
        break;
      case "trigger":
        pushTrigger(frame.trigger as Trigger);
        break;
    }
    update();
  };
}

main();
```

`src/components/layout.ts` — the bento skeleton (header + 8 cards). `mount(el)` builds the DOM once and returns the inquiry mount point; `update()` refreshes every card from `state`:

```ts
import { state, tickSeries } from "../store";
import { fmt, fmtDuration, sparkline, statusDot } from "./cards";

export interface Mounts {
  inquiry: HTMLElement;
}

const card = (cls: string, title: string, body: string): HTMLElement => {
  const el = document.createElement("section");
  el.className = `card ${cls}`;
  el.innerHTML = `<h2>${title}</h2>${body}`;
  return el;
};

export function mount(root: HTMLElement): Mounts {
  root.innerHTML = `
  <header class="topbar">
    <span class="brand">chrono<span class="brand-dot">▸</span></span>
    <span class="status mono">
      <span id="dot-feed"></span> feed
      <span id="dot-nats"></span> nats
      <span class="sep"></span> up <span id="uptime">—</span>
    </span>
  </header>
  <main class="grid">
    <section class="card area-pulse"><h2>pulse</h2>
      <div class="big mono" id="tickrate">—</div>
      <div class="sub">ticks/s</div>
      <div id="spark"></div>
      <div class="duo"><span class="mono" id="ticks">—</span> ticks
        · <span class="mono" id="ticksdropped">—</span> dropped</div>
    </section>
    <section class="card area-fires red"><h2>fires</h2>
      <div class="big mono" id="fired">—</div>
      <div class="sub">triggers fired</div>
      <div class="duo"><span class="mono" id="published">—</span> published
        · <span class="mono" id="pubdropped">—</span> dropped</div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile"><div class="big mono" id="st-active">—</div><div class="sub">active</div></div>
        <div class="tile"><div class="big mono" id="st-triggered">—</div><div class="sub">triggered</div></div>
        <div class="tile"><div class="big mono" id="st-cancelled">—</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div class="rows" id="engine"></div></section>
    <section class="card area-stream teal"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger…</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;
  return { inquiry: document.getElementById("inquiry")! };
}

export function update(): void {
  const s = state.snapshot;
  const set = (id: string, v: string) => {
    const el = document.getElementById(id);
    if (el) el.textContent = v;
  };
  set("dot-feed", statusDot(s ? s.feed_ever_connected && s.feed_last_seen_ms_ago < 10_000 : false));
  set("dot-nats", statusDot(!!s?.nats_connected));
  if (!s) return;
  set("uptime", fmtDuration(s.uptime_sec));
  set("tickrate", fmt(s.ticks_per_sec));
  set("ticks", fmt(s.ticks));
  set("ticksdropped", fmt(s.ticks_dropped));
  set("fired", fmt(s.triggers_fired));
  set("published", fmt(s.triggers_published));
  set("pubdropped", fmt(s.triggers_publish_dropped));
  set("st-active", String(s.alerts_by_state.active ?? 0));
  set("st-triggered", String(s.alerts_by_state.triggered ?? 0));
  set("st-cancelled", String(s.alerts_by_state.cancelled ?? 0));

  const spark = document.getElementById("spark");
  if (spark) spark.innerHTML = sparkline(tickSeries);

  const venues = document.getElementById("venues");
  if (venues) {
    const entries = Object.entries(s.venue_ticks).sort((a, b) => b[1] - a[1]);
    const max = entries[0]?.[1] || 1;
    venues.innerHTML = entries
      .map(([v, n]) => `<div class="row"><span>${v}</span>
        <span class="bar"><i style="width:${(100 * n) / max}%"></i></span>
        <span class="mono">${fmt(n)}</span></div>`)
      .join("");
  }
  const eng = document.getElementById("engine");
  if (eng) {
    eng.innerHTML = `<div class="row"><span>live alerts</span><span class="mono">${fmt(s.engine.live)}</span></div>
      <div class="row"><span>ring drops</span><span class="mono">${fmt(s.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${state.hello ? fmt(state.hello.symbol_count) : "—"}</span></div>`;
  }

  const stream = document.getElementById("stream");
  if (stream) {
    stream.innerHTML =
      state.triggers.length === 0
        ? `<div class="empty">waiting for the first trigger…</div>`
        : state.triggers
            .slice(0, 12)
            .map(
              (tr) => `<div class="trg">
              <span class="badge ${tr.direction === "ABOVE" ? "up" : "down"}">${tr.direction}</span>
              <span class="sym">${tr.symbol}</span>
              <span class="mono">${tr.fired_price}</span>
              <span class="meta">${tr.venue}/${tr.tier}</span>
              <span class="mono time">${new Date(tr.fired_at_unix_nanos / 1e6).toLocaleTimeString()}</span>
            </div>`
            )
            .join("");
  }
}
```

`src/components/cards.ts` — pure formatting helpers (no DOM state):

```ts
export const fmt = (n: number): string =>
  n >= 1e9 ? `${(n / 1e9).toFixed(1)}B` : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e4 ? `${(n / 1e3).toFixed(1)}k` : n.toLocaleString();

export function fmtDuration(sec: number): string {
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = Math.floor(sec % 60);
  return h > 0 ? `${h}h ${m}m` : m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export function statusDot(ok: boolean): string {
  return `<span class="dot ${ok ? "on" : "off"}"></span>`;
}

/** Inline-SVG sparkline, no chart library. */
export function sparkline(series: number[]): string {
  if (series.length < 2) return "";
  const w = 100, h = 28;
  const max = Math.max(...series, 1);
  const pts = series.map((v, i) => `${(i / (series.length - 1)) * w},${h - (v / max) * h}`);
  return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${pts.join(" ")}"/>
  </svg>`;
}
```

`src/components/inquiry.ts` — filter bar + table + pager, fetches on demand:

```ts
import { fetchAlerts, type AlertsPage } from "../api";

const PAGE = 25;

const filters: Array<{ key: string; label: string; options: string[] }> = [];

export function mountInquiry(root: HTMLElement): void {
  let offset = 0;
  let page: AlertsPage | null = null;
  const state2sym = ["active", "triggered", "cancelled"];
  const venues = window.__chrono_venues__ ?? [];
  const tiers = window.__chrono_tiers__ ?? ["TOP", "MID"];

  root.innerHTML = `
    <h2>alert inquiry</h2>
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <select name="state"><option value="">state: any</option>
        ${state2sym.map((s) => `<option>${s}</option>`).join("")}</select>
      <select name="venue"><option value="">venue: any</option>
        ${venues.map((v) => `<option>${v}</option>`).join("")}</select>
      <select name="tier"><option value="">tier: any</option>
        ${tiers.map((v) => `<option>${v}</option>`).join("")}</select>
      <select name="direction"><option value="">direction: any</option>
        <option>ABOVE</option><option>BELOW</option></select>
      <button type="submit">query</button>
    </form>
    <table class="tbl"><thead><tr>
      <th>symbol</th><th>dims</th><th>type</th><th>dir</th><th>target</th>
      <th>state</th><th>created</th><th></th>
    </tr></thead><tbody id="rows"></tbody></table>
    <div class="pager"><button id="prev">← prev</button>
      <span id="pg" class="mono"></span>
      <button id="next">next →</button></div>`;

  const form = root.querySelector("HTMLFormElement")!;
  const rows = root.querySelector("#rows") as HTMLElement;

  async function load(): Promise<void> {
    const q: Record<string, string> = { limit: String(PAGE), offset: String(offset) };
    new FormData(form as HTMLFormElement).forEach((v, k) => (q[k] = String(v)));
    page = await fetchAlerts(q);
    rows.innerHTML =
      page.items.length === 0
        ? `<tr><td colspan="8" class="empty">no alerts match</td></tr>`
        : page.items
            .map(
              (a) => `<tr>
              <td>${a.symbol}</td><td class="meta">${a.venue}/${a.tier}</td>
              <td class="meta">${a.price_type}</td>
              <td><span class="badge ${a.direction === "ABOVE" ? "up" : "down"}">${a.direction}</span></td>
              <td class="mono">${a.target_price}</td>
              <td><span class="chip state-${a.state}">${a.state}</span></td>
              <td class="mono time">${new Date(a.created_at_unix_nanos / 1e6).toLocaleTimeString()}</td>
              <td class="mono meta">${a.id.slice(0, 8)}</td>
            </tr>`
            )
            .join("");
    const pg = root.querySelector("#pg") as HTMLElement;
    pg.textContent = `${offset + 1}–${offset + page.items.length} of ${page.total}`;
  }

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    offset = 0;
    void load();
  });
  root.querySelector("#prev")!.addEventListener("click", () => {
    if (offset > 0) { offset = Math.max(0, offset - PAGE); void load(); }
  });
  root.querySelector("#next")!.addEventListener("click", () => {
    if (page && offset + page.items.length < page.total) { offset += PAGE; void load(); }
  });
  void load();
}
```

(The `window.__chrono_venues__` hook is optional polish: if awkward, drop it and keep the free-text/`TOP|MID` selects above — the hello frame already renders the vocabulary in the header area. Fix the `querySelector("HTMLFormElement")` typo when implementing: it must be `querySelector("form") as HTMLFormElement`.)

`src/style.css` — palette tokens verbatim + grid + card system; the frontend-design pass refines within these tokens:

```css
:root {
  --onyx: #161515;
  --graphite: #2E2D29;
  --soft-linen: #F3EFE7;
  --linen: #F4EEE5;
  --brick-ember: #D40000;
  --pine-teal: #164D44;
  --coffee-bean: #1E1917;
  --silver: #BFB3AD;
  --molten-lava: #770B0C;
  --light-green: #7FDF74;
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--onyx); color: var(--coffee-bean);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
}
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-variant-numeric: tabular-nums; }
.topbar {
  display: flex; justify-content: space-between; align-items: center;
  padding: 14px 20px; color: var(--silver);
}
.brand { font-weight: 700; letter-spacing: .08em; color: var(--soft-linen); font-size: 18px; }
.brand-dot { color: var(--light-green); }
.status { font-size: 13px; }
.sep { display: inline-block; width: 1px; height: 12px; background: var(--graphite); margin: 0 8px; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; background: var(--graphite); }
.dot.on { background: var(--light-green); }
.dot.off { background: var(--brick-ember); }
.grid {
  display: grid; gap: 14px; padding: 0 14px 14px;
  grid-template-columns: repeat(12, 1fr);
  grid-template-areas:
    "pulse pulse pulse pulse fires fires fires fires book book book book"
    "pulse pulse pulse pulse fires fires fires fires book book book book"
    "venues venues venues venues engine engine engine engine book book book book"
    "stream stream stream stream stream stream stream stream stream stream stream stream"
    "inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry";
}
.card {
  background: var(--soft-linen); border-radius: 18px; padding: 18px;
  box-shadow: 0 10px 30px rgba(0,0,0,.35);
  transition: transform .15s ease;
}
.card:hover { transform: translateY(-2px); }
.card h2 {
  margin: 0 0 10px; font-size: 11px; text-transform: uppercase;
  letter-spacing: .14em; color: var(--silver);
}
.card.teal { background: var(--pine-teal); color: var(--soft-linen); }
.card.teal h2 { color: var(--silver); }
.card.red { background: var(--brick-ember); color: #fff; box-shadow: 0 10px 30px rgba(119,11,12,.45); }
.card.red h2 { color: #ffb3b3; }
.area-pulse { grid-area: pulse; }
.area-fires { grid-area: fires; }
.area-book { grid-area: book; }
.area-venues { grid-area: venues; }
.area-engine { grid-area: engine; }
.area-stream { grid-area: stream; }
.area-inquiry { grid-area: inquiry; }
.big { font-size: 42px; font-weight: 700; line-height: 1.1; }
.sub { font-size: 12px; color: var(--silver); margin: 2px 0 10px; }
.duo { font-size: 13px; color: var(--silver); }
.duo .mono { color: inherit; }
#spark svg { width: 100%; height: 46px; color: var(--coffee-bean); margin-top: 6px; }
.trio { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.tile { background: var(--linen); border-radius: 12px; padding: 14px; text-align: center; }
.rows .row {
  display: grid; grid-template-columns: 70px 1fr 90px; gap: 10px;
  align-items: center; padding: 6px 0; font-size: 14px;
}
.bar { height: 6px; border-radius: 3px; background: var(--linen); overflow: hidden; }
.bar i { display: block; height: 100%; background: var(--pine-teal); border-radius: 3px; }
.stream .trg {
  display: grid; grid-template-columns: 64px 90px 1fr 110px 90px; gap: 10px;
  align-items: center; padding: 8px 4px; font-size: 14px;
  border-bottom: 1px solid rgba(243,239,231,.08);
  animation: slidein .25s ease;
}
@keyframes slidein { from { opacity: 0; transform: translateY(-6px); } to { opacity: 1; } }
.stream .sym { font-weight: 600; }
.stream .meta { color: var(--silver); font-size: 12px; }
.time { color: var(--silver); font-size: 12px; }
.badge {
  font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-align: center;
}
.badge.up { background: var(--molten-lava); color: #fff; }
.badge.down { background: var(--linen); color: var(--coffee-bean); }
.filters { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
.filters input, .filters select, .filters button {
  font: inherit; font-size: 13px; padding: 7px 10px; border-radius: 9px;
  border: 1px solid var(--coffee-bean); background: #fff0; color: var(--coffee-bean);
}
.filters button {
  background: var(--pine-teal); color: var(--soft-linen); border: 0; font-weight: 600; cursor: pointer;
}
.tbl { width: 100%; border-collapse: collapse; font-size: 14px; }
.tbl th {
  text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: .1em;
  color: var(--silver); padding: 6px 8px; border-bottom: 1px solid var(--linen);
}
.tbl td { padding: 8px; border-bottom: 1px solid var(--linen); }
.chip { font-size: 11px; padding: 2px 8px; border-radius: 999px; }
.chip.state-active { background: var(--pine-teal); color: var(--light-green); }
.chip.state-triggered { background: var(--brick-ember); color: #fff; }
.chip.state-cancelled { background: var(--linen); color: var(--silver); }
.pager { display: flex; align-items: center; gap: 14px; margin-top: 10px; color: var(--silver); }
.pager button {
  font: inherit; font-size: 13px; padding: 6px 12px; border-radius: 9px;
  border: 1px solid var(--coffee-bean); background: #fff0; color: var(--coffee-bean); cursor: pointer;
}
.empty { color: var(--silver); font-size: 13px; padding: 12px 0; }
@media (max-width: 900px) {
  .grid { grid-template-columns: 1fr; grid-template-areas: "pulse" "fires" "book" "venues" "engine" "stream" "inquiry"; }
  .stream .trg { grid-template-columns: 56px 80px 1fr 80px; }
  .stream .meta { display: none; }
}
```

- [ ] **Step 3: Build and commit the bundle**

```bash
cd internal/server/web && ./build.sh && cd -
git add internal/server/web/
```

(Requires standalone `esbuild` and `tsc` on PATH. If unavailable in the execution environment, BLOCK and report — the bundle must be the output of the real toolchain, not hand-written.)

- [ ] **Step 4: Serve it** — in `internal/server/server.go`:

```go
//go:embed web/dist
var webFS embed.FS
```

(import `"embed"`; the mux in `Handler()` gains `mux.Handle("GET /", http.FileServer(http.FS(webFS)))` — the FS root is `web/dist`, so `/` serves `index.html` and `/app.js` the bundle. Keep it after the API routes; `GET /` only matches the root, other paths 404.)

- [ ] **Step 5: Asset-serving test** — add to `internal/server/server_test.go`:

```go
func TestUIAssetsServed(t *testing.T) {
	s := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(body, []byte("app.js")) {
		t.Fatalf("index: status=%d body=%q", resp.StatusCode, body[:min(len(body), 200)])
	}
	js, err := ts.Client().Get(ts.URL + "/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js.Body.Close()
	if js.StatusCode != 200 {
		t.Fatalf("app.js status = %d", js.StatusCode)
	}
}
```

- [ ] **Step 6: Run tests and gates**

```bash
go build ./... && go vet ./...
go test ./internal/server/ -race -count=1
git diff --stat main -- engine price   # must be empty
```

- [ ] **Step 7: Commit**

```bash
git add internal/server/
git commit -m "feat(server): embedded bento dashboard (vanilla TS, committed bundle)"
```

---

### Task 5: chronod wiring, integration coverage, README

**Files:**
- Modify: `cmd/chronod/main.go`
- Modify: `tests/integration_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1's `SubscribeTriggers`, Task 3's `Server.HandleTrigger`, Task 4's served assets.
- Produces: the finished daemon — NATS loopback wired to the hub; e2e coverage of `/` and `/api/alerts/{id}`.

- [ ] **Step 1: Wire chronod.** In `cmd/chronod/main.go`, immediately after `statusSrv := server.New(core, st, reg)`:

```go
	// Dashboard live feed: re-consume our own published triggers. The
	// handler fans out to browsers over SSE; it never blocks us.
	if err := publisher.SubscribeTriggers(statusSrv.HandleTrigger); err != nil {
		return fmt.Errorf("subscribe triggers: %w", err)
	}
```

- [ ] **Step 2: Extend the integration test.** In `tests/integration_test.go`, keep a reference to the alert id from the existing upsert (`resp` from `alerts.UpsertAlert` — capture `alertID := resp.GetAlertId()`), and after the existing rate-gate block append:

```go
	// Dashboard: the SPA is served and the inquiry API resolves the alert.
	ui, err := tsClient.Get("http://" + httpAddr + "/")
	if err != nil {
		t.Fatal(err)
	}
	uiBody, _ := io.ReadAll(ui.Body)
	ui.Body.Close()
	if ui.StatusCode != 200 || !strings.Contains(string(uiBody), "app.js") {
		t.Fatalf("ui index: status=%d", ui.StatusCode)
	}
	inq, err := tsClient.Get("http://" + httpAddr + "/api/alerts/" + alertID)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Symbol string `json:"symbol"`
		State  string `json:"state"`
	}
	if err := json.UnmarshalRead(inq.Body, &detail); err != nil {
		inq.Body.Close()
		t.Fatal(err)
	}
	inq.Body.Close()
	if detail.Symbol != "BTCUSDT" || detail.State != "triggered" {
		t.Fatalf("inquiry detail = %+v", detail)
	}
```

Adapt to the file's actual client/imports (`http.Get` and a fresh `http.Client{}` are fine if `tsClient` doesn't exist; add `"io"`/`"strings"` only if unused). The alert state is `triggered` because the trigger assertion earlier in the test already proved it fired. Also assert one live SSE trigger if cheap to add (optional; the unit test in Task 3 covers the stream contract):

```go
	// One SSE frame check: hello + at least one snapshot within 3s.
	sse, err := http.Get("http://" + httpAddr + "/api/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer sse.Body.Close()
	sc := bufio.NewScanner(sse.Body)
	sawHello, sawSnap := false, false
	for sc.Scan() && !(sawHello && sawSnap) {
		line := sc.Text()
		if strings.Contains(line, `"type":"hello"`) { sawHello = true }
		if strings.Contains(line, `"type":"snapshot"`) { sawSnap = true }
	}
	if !sawHello || !sawSnap {
		t.Fatalf("sse: hello=%v snapshot=%v", sawHello, sawSnap)
	}
```

(With the standard `http.Get` client and no read timeout this loop ends when the deferred close runs — keep it bounded by the loop condition above and the test's own ctx timeout.)

- [ ] **Step 3: README.** In `README.md`:

- `internal/server` package line becomes: "chronod's HTTP status surface: `/healthz`, `/readyz`, `/stats` (encoding/json/v2), `/metrics` (Prometheus), `/debug/pprof/` — plus the live monitoring dashboard: `/` (embedded bento-grid SPA), `/api/stream` (SSE: 1s snapshots + live triggers via NATS loopback), `/api/alerts` (read-only inquiry)."
- Demo block gains a line after the `nats sub` line:

```sh
xdg-open http://localhost:8080/     # live bento dashboard
```

- After the demo block, add: "The dashboard is embedded in the binary — no extra process. Its TypeScript source lives in `internal/server/web/src`; the committed bundle (`dist/app.js`) is rebuilt with `internal/server/web/build.sh` (standalone `esbuild` + `tsc`; `go build` never needs it)."

- [ ] **Step 4: Run everything (the gates)**

```bash
go build ./... && go vet ./...
go test ./... -race -count=1
go test -tags integration ./tests -run Integration -v -timeout 180s
git diff --stat main -- engine price   # must be empty
```

Expected: all green, integration rate gate ≥19.5k unchanged.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronod/main.go tests/integration_test.go README.md
git commit -m "feat(chronod): wire dashboard feed; e2e coverage and docs"
```

---

## Post-plan notes for the executor

- **Task 4 brief must name the frontend-design skill as REQUIRED** and paste the palette tokens + grid structure from Global Constraints verbatim; the skill governs visual polish, the plan governs contract and behavior.
- Task 4's TS listing contains two intentional correction notes (`querySelector("form")`, optional `window.__chrono_venues__` hook) — the implementer resolves them while writing the file; they are not blockers.
- `dist/app.js` must be produced by the real toolchain (`./build.sh`), never hand-authored. If `esbuild`/`tsc` are unavailable, BLOCK.
- Go 1.27: `encoding/json/v2` imports bind as `json` — no alias; `json.UnmarshalRead`/`MarshalWrite`, no `NewDecoder`/`Encoder` on that import.
- The engine and price packages must show ZERO diffs at the end of every task.
- Never `git add -A` — untracked scratch dirs (`.idea/`, `graphify-out/`) must stay out of commits.
