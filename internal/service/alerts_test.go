package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
)

func validUpsert() *chronov1.UpsertAlertRequest {
	return &chronov1.UpsertAlertRequest{
		Symbol:      "BTCUSDT",
		PriceType:   chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00",
		Venue:       "ATLAS",
		Tier:        "TOP",
	}
}

func TestUpsertGeneratesUUID(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	id := resp.GetAlertId()
	if len(id) != 36 || !strings.Contains(id, "-") {
		t.Fatalf("alert id %q not UUID string form", id)
	}
	// 0x70 nibble: UUIDv7 version marker.
	if id[14] != '7' {
		t.Fatalf("alert id %q not v7", id)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("alert count = %d, want 1", got)
	}
}

func TestUpsertVisibleImmediately(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live = %d, want 1 (Sync before return)", got)
	}
}

func TestUpsertValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name string
		mut  func(*chronov1.UpsertAlertRequest)
		want codes.Code
	}{
		{"unknown symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "NOSUCH" }, codes.InvalidArgument},
		{"no price type", func(r *chronov1.UpsertAlertRequest) { r.PriceType = chronov1.PriceType_PRICE_TYPE_UNSPECIFIED }, codes.InvalidArgument},
		{"no direction", func(r *chronov1.UpsertAlertRequest) { r.Direction = chronov1.Direction_DIRECTION_UNSPECIFIED }, codes.InvalidArgument},
		{"precision loss", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "65000.005" }, codes.InvalidArgument},
		{"bad number", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "abc" }, codes.InvalidArgument},
		{"overflow", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "99999999999999999.00" }, codes.InvalidArgument},
		{"unknown venue", func(r *chronov1.UpsertAlertRequest) { r.Venue = "BINANCE" }, codes.InvalidArgument},
		{"unknown tier", func(r *chronov1.UpsertAlertRequest) { r.Tier = "DEEP" }, codes.InvalidArgument},
		{"expires before valid", func(r *chronov1.UpsertAlertRequest) { r.ValidFromUnixNanos = 200; r.ExpiresUnixNanos = 100 }, codes.InvalidArgument},
		{"empty symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "" }, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validUpsert()
			tc.mut(req)
			_, err := e.alerts().UpsertAlert(context.Background(), req)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v, want %v (err %v)", status.Code(err), tc.want, err)
			}
			if got := e.core.AlertCount(); got != 0 {
				t.Fatalf("rejected upsert left %d alerts behind", got)
			}
		})
	}
}

func TestUpsertReplaceCancelsOld(t *testing.T) {
	e := newEnv(t)
	first, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	req := validUpsert()
	req.TargetPrice = "64000.00"
	req.AlertId = mustBytes(t, first.GetAlertId())
	if _, err := e.alerts().UpsertAlert(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("count after replace = %d, want 1", got)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live after replace = %d, want 1", got)
	}
}

func TestCancel(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); err != nil {
		t.Fatalf("CancelAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 0 {
		t.Fatalf("engine live after cancel = %d, want 0", got)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("double cancel code = %v, want NotFound", status.Code(err))
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad uuid code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestAlertsByState(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertsByState()["active"]; got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

func mustBytes(t *testing.T, s string) []byte {
	t.Helper()
	id, err := parseAlertID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id[:]
}

// seedThree registers three alerts: one stays active, one is triggered by
// a tick, one is cancelled. Returns their string ids (active, triggered,
// cancelled) in that order.
func seedThree(t *testing.T, e *testEnv) (string, string, string) {
	t.Helper()
	venues := catalog.Default().DimValues(catalog.DimVenue)
	upsert := func(symbol string, venue string, validFrom, expires int64) string {
		resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: symbol, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: "65000.00", Venue: venue, Tier: "TOP",
			ValidFromUnixNanos: validFrom, ExpiresUnixNanos: expires,
		})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetAlertId()
	}
	// The engine reads valid_from/expires as absolute unix nanos (match
	// skips ts >= expires and the reaper sweeps due entries), so live
	// windows are anchored to the wall clock; 0 expires means "never". The
	// active alert is never matched, so it keeps the small sentinel values
	// GetAlert asserts on.
	active := upsert("BTCUSDT", venues[0], 100, 1100)
	now := time.Now().UnixNano()
	triggered := upsert("ETHUSDT", venues[0], now, 0)
	cancelled := upsert("BTCUSDT", venues[len(venues)-1], now, 0)

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
	_, _, cancelled := seedThree(t, e)

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

// TestAlertsByStateIncremental pins the stateCounts tallies against a full
// catalog scan across every transition kind: insert, replace, cancel,
// trigger. (The counters made seeding O(N) instead of O(N^2); drift here
// would silently skew /stats and the dashboard's alert book.)
func TestAlertsByStateIncremental(t *testing.T) {
	e := newEnv(t)
	upsert := func() string {
		resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetAlertId()
	}

	scan := func() map[string]int {
		e.core.mu.RLock()
		defer e.core.mu.RUnlock()
		out := map[string]int{}
		for _, a := range e.core.alerts {
			out[string(a.State)]++
		}
		return out
	}
	check := func(stage string) {
		t.Helper()
		got := e.core.AlertsByState()
		want := scan()
		if len(got) != len(want) {
			t.Fatalf("%s: %d states, scan has %d (%v vs %v)", stage, len(got), len(want), got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("%s: state %q = %d, scan says %d", stage, k, got[k], v)
			}
		}
	}

	a1 := upsert()
	req := validUpsert()
	req.Symbol = "ETHUSDT"
	if _, err := e.alerts().UpsertAlert(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	check("after inserts")

	// Replace: same id upserted again re-enters as active.
	rep := validUpsert()
	rep.AlertId = []byte(nil)
	idRaw, err := parseAlertID(a1)
	if err != nil {
		t.Fatal(err)
	}
	rep.AlertId = idRaw[:]
	if _, err := e.alerts().UpsertAlert(context.Background(), rep); err != nil {
		t.Fatal(err)
	}
	check("after replace")

	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: a1}); err != nil {
		t.Fatal(err)
	}
	check("after cancel")

	// Fire ETHUSDT via a tick; counters must follow to triggered.
	venues := catalog.Default().DimValues(catalog.DimVenue)
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
	check("after trigger")
}
