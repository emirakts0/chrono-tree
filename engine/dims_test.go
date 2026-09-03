package engine

import "testing"

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
