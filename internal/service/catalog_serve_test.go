package service

import (
	"context"
	"slices"
	"testing"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// TestGetCatalogLearns: the served catalog reflects what the feed and
// alerts interned — reference prices are simulator-only and stay empty.
func TestGetCatalogLearns(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "LEARNT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "5.50", Venue: "ATLAS", Tier: "TOP",
	}); err != nil {
		t.Fatal(err)
	}
	rep, err := e.feed().GetCatalog(context.Background(), &chronov1.CatalogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, s := range rep.GetSymbols() {
		if s.GetSymbol() != "LEARNT" {
			continue
		}
		found = true
		if s.GetDecimals() != 2 {
			t.Fatalf("LEARNT decimals = %d, want 2", s.GetDecimals())
		}
		if s.GetReferencePrice() != "" {
			t.Fatalf("LEARNT reference = %q, want empty", s.GetReferencePrice())
		}
	}
	if !found {
		t.Fatal("LEARNT not served by GetCatalog")
	}
	var venue *chronov1.DimInfo
	for _, d := range rep.GetDims() {
		if d.GetName() == "venue" {
			venue = d
		}
	}
	if venue == nil {
		t.Fatalf("dims = %v, venue dim missing", rep.GetDims())
	}
	if !slices.Contains(venue.GetValues(), "ATLAS") {
		t.Fatalf("venue values = %v, want ATLAS interned", venue.GetValues())
	}
}
