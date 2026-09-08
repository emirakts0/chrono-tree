package price

import (
	"errors"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"testing"
)

func TestFromFloatStrict(t *testing.T) {
	// 0.1's shortest round-trip representation is "0.1": exact at 8 decimals.
	if v, err := FromFloat(0.1, 8); err != nil || v != 10000000 {
		t.Fatalf("FromFloat(0.1,8)=(%d,%v), want (10000000,nil)", v, err)
	}
	if v, err := FromFloat(-0.1, 8); err != nil || v != -10000000 {
		t.Fatalf("FromFloat(-0.1,8)=(%d,%v), want (-10000000,nil)", v, err)
	}
	// Negative zero normalizes to 0.
	if v, err := FromFloat(math.Copysign(0, -1), 8); err != nil || v != 0 {
		t.Fatalf("FromFloat(-0,8)=(%d,%v), want (0,nil)", v, err)
	}
	// A float that is genuinely inexact at the scale rejects — negative
	// validation: with half-even rounding instead, these would succeed.
	for _, c := range []struct {
		f    float64
		d    uint8
		want error
	}{
		{0.05, 1, ErrPrecisionLoss},      // shortest repr "0.05", 2 digits at 1 decimal
		{1.0 / 3.0, 8, ErrPrecisionLoss}, // "0.3333333333333333"
		{math.NaN(), 8, ErrNotFinite},
		{math.Inf(1), 8, ErrNotFinite},
		{math.Inf(-1), 8, ErrNotFinite},
		{1e19, 18, ErrOverflow},
	} {
		if _, err := FromFloat(c.f, c.d); !errors.Is(err, c.want) {
			t.Errorf("FromFloat(%v,%d) err=%v, want %v", c.f, c.d, err, c.want)
		}
	}
	// Large-but-exact: 123456789 is exactly representable as a float and its
	// shortest repr fits any scale.
	if v, err := FromFloat(123456789, 8); err != nil || v != 12345678900000000 {
		t.Fatalf("FromFloat(123456789,8)=(%d,%v)", v, err)
	}
}

func TestFormatExact(t *testing.T) {
	cases := []struct {
		v        int64
		decimals uint8
		want     string
	}{
		{10000000, 8, "0.10000000"}, // canonical: exactly d fraction digits
		{0, 8, "0.00000000"},
		{-125, 2, "-1.25"},
		{1234, 2, "12.34"},
		{5, 0, "5"},
		{-5, 0, "-5"},
		{1, 18, "0.000000000000000001"},
		{math.MinInt64, 0, "-9223372036854775808"},
	}
	for _, c := range cases {
		if got := Format(c.v, c.decimals); got != c.want {
			t.Errorf("Format(%d,%d)=%q, want %q", c.v, c.decimals, got, c.want)
		}
	}
}

// TestRoundTripPins pins both round-trip invariants: Format∘Parse normalizes, and
// Parse∘Format is the identity for every in-range value.
func TestRoundTripPins(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 20000; i++ {
		d := uint8(rng.Intn(19))
		s := randDecStr(rng)
		v, err := Parse(s, d)
		if err != nil {
			continue
		}
		f := Format(v, d)
		v2, err := Parse(f, d)
		if err != nil {
			t.Fatalf("Parse(Format(%q→%d,%d)=%q): %v", s, v, d, f, err)
		}
		if v2 != v {
			t.Fatalf("round trip %q → %d → %q → %d diverged", s, v, f, v2)
		}
		// Direct int64 round trip (skewed toward small magnitudes).
		x := rng.Int63() - rng.Int63n(1<<40)
		if y, err := Parse(Format(x, d), d); err != nil || y != x {
			t.Fatalf("Parse(Format(%d,%d))=(%d,%v)", x, d, y, err)
		}
	}
}

// TestParseAgainstBigInt is the independent oracle: Parse must agree exactly
// with unlimited-precision big.Int arithmetic. math/big is test-only.
func TestParseAgainstBigInt(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50000; i++ {
		s := randDecStr(rng)
		d := rng.Intn(19)
		want, exact := bigParse(s, d)
		got, err := Parse(s, uint8(d))
		if exact {
			if err != nil {
				t.Fatalf("Parse(%q,%d) err=%v, big says exact %d", s, d, err, want)
			}
			if got != want {
				t.Fatalf("Parse(%q,%d)=%d, big says %d", s, d, got, want)
			}
		} else if err == nil {
			t.Fatalf("Parse(%q,%d)=%d, big says not exactly representable", s, d, got)
		}
	}
}

// bigParse mirrors Parse's grammar with unlimited precision. It returns the
// scaled value and whether it is exactly representable (fits int64 within
// Parse's magnitude-symmetric range, no nonzero digit beyond scale).
func bigParse(s string, decimals int) (int64, bool) {
	i := 0
	neg := false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	v := new(big.Int)
	ten := big.NewInt(10)
	fracDigits := 0
	seenDigit, seenDot, exact := false, false, true
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			seenDigit = true
			// Mirror Parse's guard exactly: absorb every integer digit but
			// only the first `decimals` fraction digits; a beyond-scale digit
			// must be zero or the value is not exactly representable.
			if !seenDot || fracDigits < decimals {
				v.Mul(v, ten)
				v.Add(v, big.NewInt(int64(c-'0')))
				if seenDot {
					fracDigits++
				}
			} else if c != '0' {
				exact = false
			}
		case c == '.' && !seenDot:
			seenDot = true
		default:
			return 0, false // syntax error: caller sees "not exact" + Parse errors
		}
	}
	if !seenDigit {
		return 0, false
	}
	for j := fracDigits; j < decimals; j++ {
		v.Mul(v, ten)
	}
	if neg {
		v.Neg(v)
	}
	// Mirror Parse's magnitude-symmetric range: ±(2^63−1). MinInt64 itself
	// rejects in Parse (magnitude accumulates in the positive domain), so the
	// oracle must not call it exact either.
	if !exact || !v.IsInt64() || v.Int64() == math.MinInt64 {
		return 0, false
	}
	return v.Int64(), true
}

// randDecStr emits a random syntactically-valid decimal string.
func randDecStr(rng *rand.Rand) string {
	b := make([]byte, 0, 32)
	if rng.Intn(4) == 0 {
		b = append(b, '-')
	}
	nInt, nFrac := rng.Intn(19), rng.Intn(21)
	for i := 0; i < nInt; i++ {
		b = append(b, byte('0'+rng.Intn(10)))
	}
	if nFrac > 0 {
		b = append(b, '.')
		for i := 0; i < nFrac; i++ {
			b = append(b, byte('0'+rng.Intn(10)))
		}
	}
	if nInt == 0 && nFrac == 0 {
		b = append(b, '7')
	}
	return string(b)
}

// TestFromFloatShortestRepr documents the mechanism: FromFloat's output is
// exactly Parse applied to strconv's shortest round-trip formatting.
func TestFromFloatShortestRepr(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 20000; i++ {
		f := (rng.Float64() - 0.5) * math.Pow(10, float64(rng.Intn(12)-3))
		for _, d := range []uint8{0, 2, 8, 18} {
			want, wantErr := Parse(strconv.FormatFloat(f, 'f', -1, 64), d)
			got, err := FromFloat(f, d)
			if wantErr != nil {
				if err == nil {
					t.Fatalf("FromFloat(%v,%d)=%d, want error %v", f, d, got, wantErr)
				}
				continue
			}
			if err != nil || got != want {
				t.Fatalf("FromFloat(%v,%d)=(%d,%v), want (%d,nil)", f, d, got, err, want)
			}
		}
	}
}
