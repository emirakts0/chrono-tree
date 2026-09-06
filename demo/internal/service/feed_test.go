package service

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/demo/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/demo/internal/catalog"
)

type tickStream struct {
	chronov1.FeedService_StreamTicksServer
	recv chan *chronov1.TickBatch
	sent chan *chronov1.FeedStatus
}

func (s *tickStream) Recv() (*chronov1.TickBatch, error) {
	b, ok := <-s.recv
	if !ok {
		return nil, io.EOF
	}
	return b, nil
}

func (s *tickStream) SendAndClose(fs *chronov1.FeedStatus) error {
	s.sent <- fs
	return nil
}

func (s *tickStream) Context() context.Context { return context.Background() }

func runTicks(t *testing.T, e *testEnv, batches ...*chronov1.TickBatch) (*chronov1.FeedStatus, error) {
	t.Helper()
	st := &tickStream{recv: make(chan *chronov1.TickBatch, len(batches)), sent: make(chan *chronov1.FeedStatus, 1)}
	for _, b := range batches {
		st.recv <- b
	}
	close(st.recv)
	err := e.core.StreamTicks(st)
	var fs *chronov1.FeedStatus
	select {
	case fs = <-st.sent:
	default:
	}
	return fs, err
}

func tick(symbol, bid, ask, venue, tier string) *chronov1.Tick {
	return &chronov1.Tick{Symbol: symbol, Bid: bid, Ask: ask, Venue: venue, Tier: tier, TsUnixNanos: time.Now().UnixNano()}
}

// waitTriggers waits until the trigger pump (the sole engine-ring consumer)
// has fired exactly n triggers.
func waitTriggers(t *testing.T, e *testEnv, n uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for e.core.stats.TriggersFired.Load() < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(25 * time.Millisecond) // let a buggy extra fire land
	if got := e.core.stats.TriggersFired.Load(); got != n {
		t.Fatalf("triggers fired = %d, want %d", got, n)
	}
}

func TestStreamTicksAccepts(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("PEPEUSDT", "0.00001234", "0.00001240", "NOVA", "MID"),
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 2 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %v", fs)
	}
	if !e.core.FeedEverConnected() {
		t.Fatal("feed not marked connected")
	}
	if e.core.FeedLastSeen().IsZero() {
		t.Fatal("feed last-seen not set")
	}
}

func TestStreamTicksDropsBadPriceOnly(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("BTCUSDT", "65000.005", "65000.10", "ATLAS", "TOP"), // 3 decimals at a pinned 2: Parse-level precision loss (BTCUSDT pinned by the first tick)
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 1 || fs.GetDropped() != 1 {
		t.Fatalf("FeedStatus = %+v, want 1 accepted 1 dropped", fs)
	}
	if e.core.stats.TicksDropped.Load() != 1 {
		t.Fatal("TicksDropped counter not bumped")
	}
}

// TestStreamTicksDropsMalformedNames: the client-streaming path bypasses
// the protovalidate interceptor, so ingestTick must bound what the feed
// can intern itself — empty or oversized names are dropped-and-counted
// like price errors, never interned.
func TestStreamTicksDropsMalformedNames(t *testing.T) {
	longVenue := strings.Repeat("V", maxNameLen+1)
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("BTCUSDT", "65000.00", "65000.10", longVenue, "TOP"),
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 1 || fs.GetDropped() != 2 {
		t.Fatalf("FeedStatus = %+v, want 1 accepted 2 dropped", fs)
	}
	if got := e.core.stats.TicksDropped.Load(); got != 2 {
		t.Fatalf("TicksDropped = %d, want 2", got)
	}
	if _, ok := e.core.Cat.Symbol(""); ok {
		t.Fatal("empty symbol interned")
	}
	if _, ok := e.core.Cat.Value(catalog.DimVenue, longVenue); ok {
		t.Fatal("oversized venue interned")
	}
}

func TestStreamTicksAcceptsUnknownRefdata(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("NOSUCH", "1.00", "1.01", "ATLAS", "TOP"),
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 2 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %+v, want 2 accepted 0 dropped", fs)
	}
	fs, err = runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "1.00", "1.01", "BINANCE", "TOP"),
	}})
	if err != nil {
		t.Fatalf("unknown venue rejected: %v", err)
	}
	if fs.GetAccepted() != 1 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %+v, want 1 accepted 0 dropped", fs)
	}
}

func TestIngestFiresAlert(t *testing.T) {
	e := newEnv(t)
	_, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	// The trigger pump (Task 6) drains the ring asynchronously; the fired
	// price itself is asserted in TestWatchReceivesEnrichedTrigger.
	waitTriggers(t, e, 1)
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
}

func TestIngestDimsScoping(t *testing.T) {
	e := newEnv(t)
	up := &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	}
	if _, err := e.alerts().UpsertAlert(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	up.Venue = "NOVA" // different dim combo: must NOT fire on ATLAS ticks
	if _, err := e.alerts().UpsertAlert(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	waitTriggers(t, e, 1)
	if got := e.core.AlertsByState()["active"]; got != 1 {
		t.Fatalf("active alerts = %d, want 1 (NOVA must stay active)", got)
	}
}

func TestGetCatalog(t *testing.T) {
	e := newEnv(t)
	// The catalog is learned from the feed: two ticks teach two symbols
	// (at the scales their prices declare) and two venues.
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("PEPEUSDT", "0.00001234", "0.00001240", "NOVA", "MID"),
	}}); err != nil {
		t.Fatal(err)
	}
	reply, err := e.feed().GetCatalog(context.Background(), &chronov1.CatalogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.GetSymbols()) != 2 {
		t.Fatalf("symbols = %d", len(reply.GetSymbols()))
	}
	if len(reply.GetDims()) != 2 || reply.GetDims()[0].GetName() != "venue" {
		t.Fatalf("dims = %v", reply.GetDims())
	}
	byName := map[string]*chronov1.SymbolInfo{}
	for _, s := range reply.GetSymbols() {
		byName[s.GetSymbol()] = s
	}
	if btc := byName["BTCUSDT"]; btc == nil || btc.GetDecimals() != 2 {
		t.Fatalf("BTCUSDT info = %+v, want 2 decimals from the wire scale", btc)
	}
	if pepe := byName["PEPEUSDT"]; pepe == nil || pepe.GetDecimals() != 8 {
		t.Fatalf("PEPEUSDT info = %+v, want 8 decimals from the wire scale", pepe)
	}
	if venues := reply.GetDims()[0].GetValues(); len(venues) != 2 || venues[0] != "ATLAS" || venues[1] != "NOVA" {
		t.Fatalf("venue values = %v, want learned [ATLAS NOVA]", venues)
	}
	if tiers := reply.GetDims()[1].GetValues(); len(tiers) != 2 || tiers[0] != "TOP" || tiers[1] != "MID" {
		t.Fatalf("tier values = %v, want learned [TOP MID]", tiers)
	}
}

// TestLearnedSymbolFiresAlert is the headline scenario: an alert on a
// never-seen symbol, then the symbol's first tick — interned, matched,
// fired. The trailing feed also interns an unseen venue (BINANCE).
func TestLearnedSymbolFiresAlert(t *testing.T) {
	e := newEnv(t)
	_, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "NEWPAIR", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "12.34", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatalf("upsert on unknown symbol: %v", err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("NEWPAIR", "12.30", "12.40", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	waitTriggers(t, e, 1)
	tr := e.rec.triggers()
	if len(tr) != 1 || tr[0].Symbol != "NEWPAIR" {
		t.Fatalf("triggers = %+v, want 1 NEWPAIR", tr)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("TICKFIRST", "1.500", "1.600", "BINANCE", "DEEP"),
	}}); err != nil {
		t.Fatal(err)
	}
	if vt := e.core.VenueTicks(); vt["BINANCE"] != 1 {
		t.Fatalf("venue ticks = %v, want BINANCE=1", vt)
	}
}
