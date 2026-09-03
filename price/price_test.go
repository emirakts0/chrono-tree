package price

import (
	"errors"
	"math"
	"testing"
)

func TestParseExact(t *testing.T) {
	cases := []struct {
		in       string
		decimals uint8
		want     int64
		wantErr  error
	}{
		// Exactness matrix pinned by spec §4.
		{"0.1", 8, 10000000, nil},
		{"0.100000000", 8, 10000000, nil},          // zeros beyond scale are exact
		{"0.000000005", 8, 0, ErrPrecisionLoss},    // never silently rounded
		{"9223372036854775808", 0, 0, ErrOverflow}, // MaxInt64+1
		{"-0", 8, 0, nil},
		{"", 8, 0, ErrSyntax},
		{"1e5", 8, 0, ErrSyntax}, // exponent notation rejected
		{"0x1", 8, 0, ErrSyntax},
		// Grammar edges.
		{"1.", 8, 100000000, nil},
		{".5", 8, 50000000, nil},
		{"+1.25", 2, 125, nil},
		{"-1.25", 2, -125, nil},
		{"00012.3400", 2, 1234, nil},
		{".", 8, 0, ErrSyntax},
		{"1.2.3", 8, 0, ErrSyntax},
		{" 1", 8, 0, ErrSyntax},
		{"1 ", 8, 0, ErrSyntax},
		// Scale extremes.
		{"1", 0, 1, nil},
		{"1.5", 0, 0, ErrPrecisionLoss},
		{"0.123456789012345678", 18, 123456789012345678, nil},
		{"123456789012345678", 18, 0, ErrOverflow}, // 1.2e17 major units × 1e18 scale ≫ MaxInt64
		{"0.000000000000000001", 18, 1, nil},
		{"0.000000000000000001", 17, 0, ErrPrecisionLoss},
		{"1", 19, 0, ErrScale},
		// Overflow shapes.
		{"9223372036854775807", 0, math.MaxInt64, nil},
		// Range is magnitude-symmetric: ±(2^63−1). MinInt64 itself rejects —
		// the magnitude accumulates in the positive domain and overflows first.
		{"-9223372036854775807", 0, math.MinInt64 + 1, nil},
		{"-9223372036854775808", 0, 0, ErrOverflow},
		{"9223372036854775.807", 3, 9223372036854775807, nil},
		{"9223372036854775.808", 3, 0, ErrOverflow}, // accumulation overflows
		{"1000000000", 10, 0, ErrOverflow},          // scale-up overflows: 10^19 > MaxInt64
		{"0.000000019", 8, 0, ErrPrecisionLoss},     // '9' beyond scale is nonzero
		{"0.00000001", 8, 1, nil},
	}
	for _, c := range cases {
		got, err := Parse(c.in, c.decimals)
		if c.wantErr != nil {
			if !errors.Is(err, c.wantErr) {
				t.Errorf("Parse(%q,%d) err=%v, want %v", c.in, c.decimals, err, c.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q,%d) unexpected err=%v", c.in, c.decimals, err)
			continue
		}
		if got != c.want {
			t.Errorf("Parse(%q,%d)=%d, want %d", c.in, c.decimals, got, c.want)
		}
	}
}

// TestParseRejectsNeverRounds is the negative validation for the exactness
// rule: with silent truncation (dropping the beyond-scale digit check), the
// ErrPrecisionLoss cases would return a value and this test would fail.
func TestParseRejectsNeverRounds(t *testing.T) {
	for _, s := range []string{"0.000000005", "1.999999999", "0.000000009"} {
		if v, err := Parse(s, 8); err == nil {
			t.Fatalf("Parse(%q,8) silently produced %d; want ErrPrecisionLoss", s, v)
		}
	}
}
