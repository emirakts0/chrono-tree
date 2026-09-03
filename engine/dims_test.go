package engine

import (
	"errors"
	"fmt"
	"testing"
)

func TestDimsHelper(t *testing.T) {
	allSentinel := [dimMax]uint16{
		DimSentinel, DimSentinel, DimSentinel, DimSentinel,
		DimSentinel, DimSentinel, DimSentinel, DimSentinel,
	}
	if Dims() != allSentinel {
		t.Fatalf("Dims() = %v, want all-sentinel", Dims())
	}
	d := Dims(3, 1, 42)
	want := allSentinel
	want[0], want[1], want[2] = 3, 1, 42
	if d != want {
		t.Fatalf("Dims(3,1,42) = %v, want %v", d, want)
	}
}

func TestNormalizeDims(t *testing.T) {
	// Zero-value array at width 0 normalizes to all-sentinel (this is what
	// keeps every existing zero-value AlertSpec/Tick valid).
	if d, ok := normalizeDims([dimMax]uint16{}, 0); !ok || d != Dims() {
		t.Fatalf("width 0: got (%v, %v)", d, ok)
	}
	// Trailing real values are overwritten with the sentinel.
	if d, ok := normalizeDims([dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7}, 2); !ok || d != Dims(1, 2) {
		t.Fatalf("width 2: got (%v, %v)", d, ok)
	}
	// Sentinel inside width is malformed.
	if _, ok := normalizeDims([dimMax]uint16{1, DimSentinel}, 2); ok {
		t.Fatal("sentinel inside width must not be ok")
	}
	// Width 8 with real values in all slots is the maximal valid input.
	if d, ok := normalizeDims(Dims(1, 2, 3, 4, 5, 6, 7, 8), 8); !ok || d != Dims(1, 2, 3, 4, 5, 6, 7, 8) {
		t.Fatalf("width 8: got (%v, %v)", d, ok)
	}
}

func TestDimsValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Dims = []string{"segment", "tier"}
	e := New(cfg)
	defer e.Close()
	base := AlertSpec{ID: mkID(1), Symbol: "S", PriceType: PriceAsk,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1, AutoDeactivate: true}

	bad := base // slot 1 (tier) left at sentinel
	bad.Dims = Dims(1)
	if err := e.Upsert(bad); !errors.Is(err, ErrDims) {
		t.Fatalf("sentinel inside width: err=%v, want ErrDims", err)
	}

	good := base // trailing junk past width is normalized away
	good.ID = mkID(2)
	good.Dims = [dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7}
	if err := e.Upsert(good); err != nil {
		t.Fatalf("trailing junk should be normalized: %v", err)
	}
}

func TestNewDimsConfig(t *testing.T) {
	mustPanic := func(name string, cfg Config) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: New must panic", name)
			}
		}()
		New(cfg)
	}
	nine := DefaultConfig()
	nine.Dims = make([]string, 9)
	for i := range nine.Dims {
		nine.Dims[i] = fmt.Sprintf("d%d", i)
	}
	mustPanic("nine dims", nine)
	empty := DefaultConfig()
	empty.Dims = []string{"ok", ""}
	mustPanic("empty name", empty)
}
