package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/alertstore"
)

func TestRunCompact(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "alerts.bbolt")
	// Build a churning store so freelist fragmentation is real.
	s, err := openTestStore(t, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "compact.bbolt")
	before, _ := os.Stat(src)
	if err := runCompact(src, out); err != nil {
		t.Fatalf("runCompact: %v", err)
	}
	after, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() == 0 || after.Size() > before.Size() {
		t.Fatalf("compacted size = %d, before = %d", after.Size(), before.Size())
	}
	if _, err := os.Stat(out + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file must be renamed away")
	}
}

func TestRunCompactMissingSource(t *testing.T) {
	if err := runCompact(filepath.Join(t.TempDir(), "nope.bbolt"), "out.bbolt"); err == nil {
		t.Fatal("missing source must be an error")
	}
}

func openTestStore(t *testing.T, path string) (*alertstore.Store, error) {
	s, err := alertstore.Open(path)
	if err != nil {
		return nil, err
	}
	// Some churn: puts + cancels so the freelist has reclaimable pages.
	for i := range 200 {
		a := alertstore.Alert{ID: engine.AlertID{byte(i >> 8), byte(i), 1}, Symbol: "BTCUSDT", Decimals: 8,
			Venue: "ATLAS", Tier: "TOP", PriceType: engine.PriceAsk, Direction: engine.DirGTE,
			TargetPrice: 65000000000000, CreatedAt: int64(i)}
		if err := s.Put(a); err != nil {
			return nil, err
		}
		if i%2 == 0 {
			if _, err := s.Cancel(a.ID); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}
