// Package price converts between decimal prices and fixed-point int64 base
// units. It is the only place in chrono-tree where decimal↔binary conversion
// happens; the engine itself is scale-agnostic and integer-only.
//
// A price of "12.34" at 2 decimals is the int64 1234. Values are never
// rounded: a nonzero digit beyond the requested scale is ErrPrecisionLoss.
package price

import (
	"errors"
	"math"
	"strconv"
)

// Sentinel errors. All conversions are exact or fail loudly.
var (
	ErrSyntax        = errors.New("price: invalid decimal syntax")
	ErrOverflow      = errors.New("price: value overflows int64 at this scale")
	ErrPrecisionLoss = errors.New("price: nonzero digit beyond scale")
	ErrNotFinite     = errors.New("price: float is NaN or infinite")
	ErrScale         = errors.New("price: decimals must be 0..18")
)

// MaxDecimals is the largest supported scale: 10^18 < MaxInt64.
const MaxDecimals = 18

// pow10[i] == 10^i.
var pow10 = [MaxDecimals + 1]int64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
	1_000_000_000, 10_000_000_000, 100_000_000_000, 1_000_000_000_000,
	10_000_000_000_000, 100_000_000_000_000, 1_000_000_000_000_000,
	10_000_000_000_000_000, 100_000_000_000_000_000, 1_000_000_000_000_000_000,
}

// Parse converts the decimal string s to base units at the given scale.
// Grammar: [+-]? digits [ '.' digits ] — at least one digit overall; "1." and
// ".5" are accepted; exponent notation is rejected. Fraction digits beyond
// the scale must be zeros (exact) or Parse fails with ErrPrecisionLoss.
// No floating-point math anywhere: digits accumulate with checked
// multiply-add, then the value is scaled by a power-of-ten factor.
func Parse(s string, decimals uint8) (int64, error) {
	if decimals > MaxDecimals {
		return 0, ErrScale
	}
	i := 0
	neg := false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	var v int64
	fracDigits := 0 // fraction digits consumed into v
	seenDigit, seenDot := false, false
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			seenDigit = true
			d := int64(c - '0')
			// Integer-part digits are unlimited; fraction digits are capped
			// at `decimals`. NOTE: the guard must be exactly this — a bare
			// `fracDigits < int(decimals)` would reject EVERY integer digit
			// when decimals == 0.
			if !seenDot || fracDigits < int(decimals) {
				// v = v*10 + d, overflow-checked.
				if v > (math.MaxInt64-d)/10 {
					return 0, ErrOverflow
				}
				v = v*10 + d
				if seenDot {
					fracDigits++
				}
			} else if d != 0 {
				// A nonzero digit beyond scale: the value is not exactly
				// representable. Never round.
				return 0, ErrPrecisionLoss
			}
		case c == '.' && !seenDot:
			seenDot = true
		default:
			return 0, ErrSyntax
		}
	}
	if !seenDigit {
		return 0, ErrSyntax
	}
	// Scale up: "12" at 3 decimals is 12000; "12.3" at 3 decimals is 12300.
	if m := pow10[decimals] / pow10[uint8(fracDigits)]; m != 1 {
		if v > math.MaxInt64/m {
			return 0, ErrOverflow
		}
		v *= m
	}
	if neg {
		v = -v
	}
	return v, nil
}

// FromFloat converts a float64 via its shortest decimal representation that
// round-trips (strconv 'f' -1). A float whose shortest form needs more
// fractional digits than the scale — or exceeds int64 scaled — is rejected
// with ErrPrecisionLoss / ErrOverflow. NaN and ±Inf are ErrNotFinite.
// This is strict: values are never rounded. Callers with coarser data should
// pre-round on their side, explicitly.
func FromFloat(f float64, decimals uint8) (int64, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, ErrNotFinite
	}
	return Parse(strconv.FormatFloat(f, 'f', -1, 64), decimals)
}

// Format renders base units as a decimal string with exactly `decimals`
// fraction digits (none when decimals == 0). Pure integer math — no float
// round-trip. It is the exact inverse of Parse: Parse(Format(v, d), d) == v.
// Panics on decimals > MaxDecimals: a display helper fed an impossible scale
// is a programmer error.
func Format(v int64, decimals uint8) string {
	if decimals > MaxDecimals {
		panic("price: decimals must be 0..18")
	}
	neg := v < 0
	// uint64(v) wraps negatives; negating in uint64 space yields the true
	// magnitude (and 2^63 for MinInt64).
	u := uint64(v)
	if neg {
		u = -u
	}
	scale := uint64(pow10[decimals])
	whole, frac := u/scale, u%scale
	b := make([]byte, 0, 24)
	if neg {
		b = append(b, '-')
	}
	b = strconv.AppendUint(b, whole, 10)
	if decimals == 0 {
		return string(b)
	}
	b = append(b, '.')
	fs := strconv.AppendUint(make([]byte, 0, MaxDecimals), frac, 10)
	for i := len(fs); i < int(decimals); i++ {
		b = append(b, '0')
	}
	b = append(b, fs...)
	return string(b)
}
