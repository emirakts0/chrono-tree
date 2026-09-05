package catalog

import (
	"slices"
	"testing"

	"github.com/emir/chrono-tree/internal/price"
)

func TestDefaultDeterministic(t *testing.T) {
	a, b := Default(), Default()
	if a == nil || b == nil || a == b {
		t.Fatalf("Default should return fresh catalogs")
	}
	sa, sb := a.Symbols(), b.Symbols()
	if len(sa) != len(sb) {
		t.Fatalf("symbol count differs between calls")
	}
	for i := range sa {
		if sa[i] != sb[i] {
			t.Fatalf("symbol %d differs: %v vs %v", i, sa[i], sb[i])
		}
	}
}

func TestSymbolCount(t *testing.T) {
	n := len(Default().Symbols())
	if n < 490 || n > 520 {
		t.Fatalf("want ~500 symbols, got %d", n)
	}
}

func TestMajorsAnchored(t *testing.T) {
	for sym, want := range map[string]string{
		"BTCUSDT": "65000.00", "ETHUSDT": "3400.00", "SOLUSDT": "150.0000",
	} {
		s, ok := Default().Symbol(sym)
		if !ok {
			t.Fatalf("%s missing", sym)
		}
		if s.Reference != want {
			t.Fatalf("%s reference = %q, want %q", sym, s.Reference, want)
		}
	}
}

func TestSubCentMemecoinPresent(t *testing.T) {
	s, ok := Default().Symbol("PEPEUSDT")
	if !ok {
		t.Fatal("PEPEUSDT missing")
	}
	if s.Decimals != 8 {
		t.Fatalf("PEPEUSDT decimals = %d, want 8", s.Decimals)
	}
	base, err := price.Parse(s.Reference, s.Decimals)
	if err != nil {
		t.Fatalf("reference unparseable: %v", err)
	}
	if base >= 1_000_000 { // < 0.01 at 8 decimals
		t.Fatalf("PEPEUSDT reference %q not sub-cent", s.Reference)
	}
}

func TestDecimalsRule(t *testing.T) {
	// >= 1000 → 2 decimals; >= 1 → 4; < 1 → 8.
	cases := map[string]uint8{"BTCUSDT": 2, "SOLUSDT": 4, "PEPEUSDT": 8}
	for sym, want := range cases {
		got, ok := Default().Decimals(sym)
		if !ok || got != want {
			t.Fatalf("%s decimals = (%d,%v), want %d", sym, got, ok, want)
		}
	}
}

func TestEveryReferenceRoundTrips(t *testing.T) {
	for _, s := range Default().Symbols() {
		base, err := price.Parse(s.Reference, s.Decimals)
		if err != nil {
			t.Fatalf("%s reference %q: %v", s.Name, s.Reference, err)
		}
		if got := price.Format(base, s.Decimals); got != s.Reference {
			t.Fatalf("%s round-trip: %q != %q", s.Name, got, s.Reference)
		}
	}
}

func TestDims(t *testing.T) {
	c := Default()
	if got := c.Dims(); !slices.Equal(got, []string{DimVenue, DimTier}) {
		t.Fatalf("Dims() = %v", got)
	}
	if !slices.Equal(c.DimValues(DimVenue), Venues) {
		t.Fatalf("venue values = %v", c.DimValues(DimVenue))
	}
	v, ok := c.Value(DimVenue, "NOVA")
	if !ok || v != 1 {
		t.Fatalf("Value(venue, NOVA) = (%d,%v), want (1,true)", v, ok)
	}
	name, ok := c.Name(DimTier, v)
	if !ok || name != "MID" {
		t.Fatalf("Name(tier, 1) = (%q,%v), want (MID,true)", name, ok)
	}
	if _, ok := c.Value(DimVenue, "BINANCE"); ok {
		t.Fatal("unknown venue should miss")
	}
}
