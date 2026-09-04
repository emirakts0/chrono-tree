package service

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
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
		tick("BTCUSDT", "65000.005", "65000.10", "ATLAS", "TOP"), // precision loss
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

func TestStreamTicksRejectsUnknownRefdata(t *testing.T) {
	e := newEnv(t)
	_, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("NOSUCH", "1.00", "1.01", "ATLAS", "TOP"),
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	_, err = runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "1.00", "1.01", "BINANCE", "TOP"),
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown venue code = %v", status.Code(err))
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
	// Pump runs asynchronously (Task 6 drains the ring); for this test,
	// drain the engine ring directly.
	tr, ok := e.core.Engine().Triggers().Pop()
	if !ok {
		t.Fatal("alert did not fire")
	}
	if tr.Price != engine.Price(6500100) { // 65001.00 at 2 decimals
		t.Fatalf("fired price = %d, want 6500100", tr.Price)
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
	if _, ok := e.core.Engine().Triggers().Pop(); !ok {
		t.Fatal("ATLAS alert did not fire")
	}
	if _, ok := e.core.Engine().Triggers().Pop(); ok {
		t.Fatal("NOVA alert fired on an ATLAS tick")
	}
}

func TestGetCatalog(t *testing.T) {
	e := newEnv(t)
	reply, err := e.feed().GetCatalog(context.Background(), &chronov1.CatalogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.GetSymbols()) < 490 {
		t.Fatalf("symbols = %d", len(reply.GetSymbols()))
	}
	if len(reply.GetDims()) != 2 || reply.GetDims()[0].GetName() != "venue" {
		t.Fatalf("dims = %v", reply.GetDims())
	}
	var btc *chronov1.SymbolInfo
	for _, s := range reply.GetSymbols() {
		if s.GetSymbol() == "BTCUSDT" {
			btc = s
		}
	}
	if btc == nil || btc.GetDecimals() != 2 || btc.GetReferencePrice() != "65000.00" {
		t.Fatalf("BTCUSDT info = %+v", btc)
	}
}
