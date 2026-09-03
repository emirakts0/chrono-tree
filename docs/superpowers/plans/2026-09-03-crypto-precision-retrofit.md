# Crypto-Precision Retrofit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the engine's float64 price representation with exact scale-agnostic int64 base units (`engine.Price`) and ship the audited `price/` boundary package (Parse / FromFloat / Format) that is the only place decimal↔binary conversion happens.

**Architecture:** A representation swap on the compare path — no structural change to COW publication, snapshot retirement, slot generations, reaper, or the trigger ring. The new `price` package is standalone (imports only stdlib); the engine does not import it. Spec: `docs/superpowers/specs/2026-09-03-crypto-precision-retrofit.md`.

**Tech Stack:** Go 1.27, stdlib only for new code (`math`, `strconv`, `math/big` test-only). No new dependencies.

## Global Constraints

- Zero per-tick heap allocations: `TestMatchZeroAllocs` (`AllocsPerRun` == 0) must still pass unchanged after the swap.
- After Task 3, `grep -rn "float64" engine/ --include="*.go"` must return **zero hits in non-test files** (the engine package contains no float arithmetic; `btype_conformance_test.go` keeps its own `confItem` float64 — it tests btype, not engine prices).
- `btype` remains the only index structure; no changes to any `btype` call site's semantics (only the comparator's operand type changes; ordering operators are identical for int64).
- `entry` stays 48 bytes (`TestEntrySize` must pass unchanged).
- Exactness rules are pinned by the spec's §4 table and must be enforced exactly: never silently round; nonzero digit beyond scale → `ErrPrecisionLoss`; zeros beyond scale are accepted.
- `decimals` cap is 18 (`10¹⁸ < MaxInt64`); larger → `ErrScale`.
- Error sentinels are compared with `errors.Is`-compatible equality (plain `==` on returned values, matching existing engine test idiom).
- Random-test idiom: `math/rand` with fixed seeds (`rand.New(rand.NewSource(N))`), matching existing tests.
- Full suite must pass under `-race` (`go test ./... -race -count=1`) at every task boundary.
- Commit style: conventional commits, one logical change per commit.

---

### Task 1: `price` package — sentinels and `Parse`

**Files:**
- Create: `price/price.go`
- Test: `price/price_test.go`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces: `package price` with sentinels `ErrSyntax`, `ErrOverflow`, `ErrPrecisionLoss`, `ErrScale` and `func Parse(s string, decimals uint8) (int64, error)`. Task 2 adds `FromFloat`, `Format`, `ErrNotFinite` in the same file.

- [ ] **Step 1: Write the failing tests**

Create `price/price_test.go`:

```go
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
		{"0.100000000", 8, 10000000, nil},   // zeros beyond scale are exact
		{"0.000000005", 8, 0, ErrPrecisionLoss}, // never silently rounded
		{"9223372036854775808", 0, 0, ErrOverflow}, // MaxInt64+1
		{"-0", 8, 0, nil},
		{"", 8, 0, ErrSyntax},
		{"1e5", 8, 0, ErrSyntax},  // exponent notation rejected
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
		{"123456789012345678", 18, 123456789012345678, nil},
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./price/ -run TestParse -v`
Expected: FAIL — `undefined: Parse` (package doesn't compile yet; create a stub `price/price.go` with `package price` only if needed to see the failure cleanly).

- [ ] **Step 3: Write the implementation**

Create `price/price.go`:

```go
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
```

The overflow check `v > (MaxInt64-d)/10` is the standard checked multiply-add: `v*10+d ≤ MaxInt64 ⟺ v ≤ (MaxInt64-d)/10` in integer arithmetic. Negation is always safe because the magnitude never exceeds MaxInt64 — which is also why the accepted range is ±(2⁶³−1) and MinInt64 itself is `ErrOverflow` (pinned by the test table).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./price/ -run TestParse -v`
Expected: PASS (both TestParseExact and TestParseRejectsNeverRounds).

- [ ] **Step 5: Commit**

```bash
git add price/price.go price/price_test.go
git commit -m "feat(price): exact fixed-point decimal parser"
```

---

### Task 2: `price` — `FromFloat` and `Format` + property tests

**Files:**
- Modify: `price/price.go`
- Test: `price/property_test.go`

**Interfaces:**
- Consumes: `Parse`, `pow10`, `MaxDecimals`, sentinels from Task 1.
- Produces: `func FromFloat(f float64, decimals uint8) (int64, error)`, `func Format(v int64, decimals uint8) string` (panics on `decimals > MaxDecimals` — programmer error on a display helper). Task 3's engine tests and Task 4's oracle tests rely on `Parse` and `Format` only.

- [ ] **Step 1: Write the failing tests**

Create `price/property_test.go`:

```go
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
		{0.05, 1, ErrPrecisionLoss},           // shortest repr "0.05", 2 digits at 1 decimal
		{1.0 / 3.0, 8, ErrPrecisionLoss},      // "0.3333333333333333"
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
		{10000000, 8, "0.10000000"},   // canonical: exactly d fraction digits
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

// TestRoundTripPins pins both spec invariants: Format∘Parse normalizes, and
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
		// Direct int64 round trip.
		x := rng.Int63() - rng.Int63n(1<<40) // skewed toward small magnitudes
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
// scaled value and whether it is exactly representable (fits int64, no
// nonzero digit beyond scale).
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
			v.Mul(v, ten)
			v.Add(v, big.NewInt(int64(c-'0')))
			if seenDot {
				if fracDigits < decimals {
					fracDigits++
				} else if c != '0' {
					exact = false
				}
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

// randDecStr emits a random syntactically-valid-ish decimal string,
// including forms Parse must reject (handled by the oracle comparison).
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./price/ -v`
Expected: FAIL — `undefined: FromFloat`, `undefined: Format`.

- [ ] **Step 3: Write the implementation**

Append to `price/price.go`:

```go
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
	u := uint64(v)
	if neg {
		u = -u // uint64(-MinInt64) is fine in two's complement
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./price/ -v`
Expected: PASS — all of Task 1's and Task 2's tests.
Run: `go vet ./price/`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add price/price.go price/property_test.go
git commit -m "feat(price): strict FromFloat and exact Format with big.Int oracle"
```

---

### Task 3: Engine `Price` swap + test rebase

A representation refactor, not a behavior change: the oracle of record is the existing suite (unchanged semantics) plus `TestEntrySize`. Rebase tests to integer base units; relationships between literals are preserved exactly (×10 where halves were involved).

**Files:**
- Modify: `engine/alert.go`, `engine/trigger.go`, `engine/match.go`, `engine/engine.go`
- Modify: `engine/engine_test.go` (rebase)
- Unchanged: `engine/index.go` (`price: a.TargetPrice` compiles as-is), `engine/shard.go`, `engine/reaper.go`, `engine/symbol.go`, `engine/btype_conformance_test.go`

**Interfaces:**
- Consumes: nothing new (engine does NOT import price).
- Produces: `type Price int64` in package engine; `entry.price`, `Tick.Bid/Ask/Mid/Last`, `Trigger.Price`, `AlertSpec.TargetPrice`, `AlertMeta.TargetPrice` all become `Price`. `entryKey`/`entryKeyMax` take `Price`; `priceOf` returns `Price`; `fire` takes `price Price`.

- [ ] **Step 1: Change the engine types**

`engine/alert.go` — after the `AlertID` declaration, add:

```go
// Price is a price in base units of the instrument's smallest quoted tick
// (fixed-point integer). Scale-agnostic: the engine never knows where the
// decimal point is; conversion between decimal text/floats and base units
// happens only in the price package, at the ingestion boundary.
type Price int64
```

Change the `entry` field (line 50):

```go
	price     Price   // target price in base units, primary sort key
```

Change the probe signatures (operators in `compareEntry` are unchanged — `<`/`>` on int64 is the identical total-order logic, minus float NaN/−0 subtleties):

```go
func entryKey(price Price) entry { return entry{price: price} }

func entryKeyMax(price Price) entry {
	var maxID AlertID
	for i := range maxID {
		maxID[i] = 0xff
	}
	return entry{price: price, id: maxID}
}
```

`engine/trigger.go` — line 8:

```go
	Price Price // fired price in base units
```

`engine/match.go` — the Tick struct and helpers:

```go
type Tick struct {
	Symbol  string
	Bid     Price
	Ask     Price
	Mid     Price
	Last    Price
	Present uint8
	TS      int64 // unix nanos
}

func priceOf(t *Tick, pt PriceType) Price {
	switch pt {
	case PriceBid:
		return t.Bid
	case PriceAsk:
		return t.Ask
	case PriceMid:
		return t.Mid
	default:
		return t.Last
	}
}
```

and the fire signature: `func (e *Engine) fire(sid SymbolID, en *entry, price Price, ts int64)`.

`engine/engine.go` — lines 58 and 96:

```go
	TargetPrice    Price // base units; scale is the caller's contract
```

(in `AlertSpec`; same field change in `AlertMeta`, comment optional there).

- [ ] **Step 2: Rebase `engine/engine_test.go`**

Apply these exact edits (line numbers from the current file; `go build ./engine/` will catch any the table misses — fix by the same ×10 rule, preserving relative magnitudes):

| Line(s) | From | To |
|---|---|---|
| 44–47 | `price: 1.5` / `2.5` | `price: 15` / `price: 25` |
| 108, 130, 142 | `Trigger{Price: float64(i)}`, `Trigger{Price: 9}` | `Trigger{Price: Price(i)}`, `Trigger{Price: 9}` |
| 118, 126 | `var got []float64`, `[]float64{0, 1, 2, 3}` | `var got []Price`, `[]Price{0, 1, 2, 3}` |
| 167 | `Trigger{Price: float64(p*each + i)}` | `Trigger{Price: Price(p*each + i)}` |
| 171 | `delivered := make(map[float64]bool)` | `delivered := make(map[Price]bool)` |
| 249, 251 | `price: 42.5`, `price: 10` | `price: 425`, `price: 100` |
| 283 | `price: 42.5` | `price: 425` |
| 316, 321 | `price: float64(i)` (both) | `price: Price(i)` |
| 368 | `testSpec(... price float64)` | `testSpec(... price Price)` |
| 380, 392, 403–404 | `42.5`, `43`, `en.price != 43`, `want 43` | `425`, `430`, unchanged (`43` → `430` in the want string) |
| 441, 448, 453, 456 | `42.5`, `1` | `425`, `1` (unchanged) |
| 477, 479 | `42.5`, `Bid: 43` | `425`, `Bid: 430` |
| 483 | `got[0].Price != 43` | `got[0].Price != 430` |
| 498, 500 | `50`, `Ask: 50` | `500`, `Ask: 500` |
| 512–528 | `42.5` ×4, `Ask: 100`, `Bid: 100` | `425` ×4, `Ask: 1000`, `Bid: 1000` |
| 546 | `float64(i)*10` | `Price(i) * 10` |
| 549 | `Bid: 1e9, Ask: 1e9, Mid: 1e9, Last: 1e9` | unchanged (integer literals) |
| 561 | `42.5` | `425` |
| 598, 627–628 | `42.5` | `425` |
| 641 | `TargetPrice: 50`, later `Ask: 49` (line 658) | `TargetPrice: 500`, `Ask: 490` |
| 686, 717, 724, 729 | `price: 42.5`, `42.5`, `Bid: 43`, `99` | `price: 425`, `425`, `Bid: 430`, `990` |
| 784 | `Bid: 1, Ask: 1, Mid: 1, Last: 1` | unchanged |
| 802, 877, 891 | `42.5`, `42.5`, `43` | `425`, `425`, `430` |

- [ ] **Step 3: Verify the swap compiles and the full suite passes**

Run: `go build ./... && go vet ./...`
Expected: clean.
Run: `go test ./... -race -count=1`
Expected: `ok github.com/emir/chrono-tree/engine`, `ok github.com/emir/chrono-tree/price` — including `TestEntrySize` (still 48 bytes: float64 → int64 is layout-neutral) and `TestMatchZeroAllocs` (0 allocs — integer compares allocate nothing).

- [ ] **Step 4: Verify no float remains in engine non-test code**

Run: `grep -rn "float64" engine/ --include="*.go" | grep -v "_test.go"`
Expected: **no output** (exit code 1 from grep). `btype_conformance_test.go` keeps its float64 `confItem` — it pins btype's comparator semantics, not engine prices.

- [ ] **Step 5: Commit**

```bash
git add engine/
git commit -m "refactor(engine): exact int64 Price representation replaces float64"
```

---

### Task 4: Oracle/bench rebase + string-price parity + final verification

**Files:**
- Modify: `engine/oracle_test.go`, `engine/bench_test.go`

**Interfaces:**
- Consumes: `engine.Price` (Task 3); `price.Parse`, `price.Format` (Tasks 1–2).
- Produces: no new production interfaces — verification only.

- [ ] **Step 1: Rebase the oracle to integer prices**

In `engine/oracle_test.go`:

Line 41 (quarter-tick targets gone; integer base units with the same hit density — targets and ticks draw from the same range):

```go
			TargetPrice:    Price(rng.Intn(1600)),
```

Lines 64–67 (tick prices):

```go
			Bid:     Price(rng.Intn(1600)),
			Ask:     Price(rng.Intn(1600)),
			Mid:     Price(rng.Intn(1600)),
			Last:    Price(rng.Intn(1600)),
```

Lines 75, 100 (oracle maps):

```go
	expected := map[AlertID]Price{}
```
```go
	got := map[AlertID]Price{}
```

`TestStressRace` — line 141:

```go
			Direction: Direction(i % 2), TargetPrice: Price(100 + i%400),
```

lines 158–168 (the float random walk becomes an integer walk with the same shape and clamp):

```go
			price := Price(300)
			for {
				select {
				case <-stop:
					return
				default:
				}
				price += Price(rng.Intn(21) - 10)
				if price < 1 {
					price = 1
				}
				e.Match(&Tick{Symbol: "HOT", Bid: price, Ask: price, Mid: price,
					Last: price, Present: TickAllPresent(), TS: time.Now().UnixNano()})
			}
```

line 190:

```go
						TargetPrice: Price(100 + rng.Intn(400)),
```

Add the import `"github.com/emir/chrono-tree/price"` (used by the parity test below; no import cycle — `price` does not import `engine`).

- [ ] **Step 2: Write the failing parity test**

Append to `engine/oracle_test.go`:

```go
// TestOracleStringPriceParity proves the engine has no hidden conversion
// layer: running the same scenario with direct int64 base-unit prices and
// with prices that traveled through price.Format → price.Parse (as the Phase
// B ingestion boundary will route them) must produce identical trigger sets
// and identical fired prices.
func TestOracleStringPriceParity(t *testing.T) {
	defer goleak.VerifyNone(t)
	const d = 8
	rng := rand.New(rand.NewSource(7))
	type spec struct {
		id     AlertID
		sym    string
		pt     PriceType
		dir    Direction
		target Price
	}
	var specs []spec
	for i := 0; i < 200; i++ {
		specs = append(specs, spec{
			id:     mkID(uint32(i + 1)),
			sym:    fmt.Sprintf("P%d", rng.Intn(8)),
			pt:     PriceType(rng.Intn(int(priceTypeCount))),
			dir:    Direction(rng.Intn(2)),
			target: Price(rng.Intn(2000)),
		})
	}
	ticks := make([]Tick, 150)
	for k := range ticks {
		ticks[k] = Tick{
			Symbol:  fmt.Sprintf("P%d", rng.Intn(8)),
			Bid:     Price(rng.Intn(2000)),
			Ask:     Price(rng.Intn(2000)),
			Mid:     Price(rng.Intn(2000)),
			Last:    Price(rng.Intn(2000)),
			Present: TickAllPresent(),
			TS:      int64(1e9 + k*1e9),
		}
	}

	// viaString routes every price through decimal text at scale d, exactly
	// as an ingestion boundary would.
	viaString := func(p Price) Price {
		s := price.Format(int64(p), d)
		v, err := price.Parse(s, d)
		if err != nil {
			t.Fatalf("parity conversion %d → %q → %v", p, s, err)
		}
		return Price(v)
	}

	run := func(convert bool) map[AlertID]Price {
		e := New(DefaultConfig())
		defer e.Close()
		for _, s := range specs {
			tp := s.target
			if convert {
				tp = viaString(s.target)
			}
			if err := e.Upsert(AlertSpec{
				ID: s.id, Symbol: s.sym, PriceType: s.pt, Direction: s.dir,
				TargetPrice: tp, ValidFrom: 1, AutoDeactivate: true,
			}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
		}
		e.Sync()
		ct := make([]Tick, len(ticks))
		for k, tk := range ticks {
			ct[k] = tk
			if convert {
				ct[k].Bid = viaString(tk.Bid)
				ct[k].Ask = viaString(tk.Ask)
				ct[k].Mid = viaString(tk.Mid)
				ct[k].Last = viaString(tk.Last)
			}
		}
		for k := range ct {
			e.Match(&ct[k])
		}
		got := map[AlertID]Price{}
		for _, tr := range drainTriggers(e) {
			if _, dup := got[tr.ID]; dup {
				t.Fatalf("alert %v fired twice", tr.ID)
			}
			got[tr.ID] = tr.Price
		}
		return got
	}

	direct, str := run(false), run(true)
	if len(direct) == 0 {
		t.Fatal("scenario fired nothing; parity test is vacuous — widen the ranges")
	}
	if len(direct) != len(str) {
		t.Fatalf("fired %d via direct int64, %d via string round-trip", len(direct), len(str))
	}
	for id, p := range direct {
		if sp, ok := str[id]; !ok || sp != p {
			t.Fatalf("alert %v: direct %d, string %d (ok=%v) — hidden conversion layer?", id, p, sp, ok)
		}
	}
}
```

- [ ] **Step 3: Rebase the benchmarks**

In `engine/bench_test.go` — scale ×1000, preserving every firing relationship:

`benchSparse` (lines 23, 26, 30):

```go
				TargetPrice: Price(1_000_000 + i), ValidFrom: 1, AutoDeactivate: true})
```
```go
				TargetPrice: Price(100_000 - i), ValidFrom: 1, AutoDeactivate: true})
```
```go
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40}
```

(Sparse shape preserved: GTE targets 1,000,000+ sit above the 500,000 tick — never fire; LTE targets 100,000− sit below it — `500000 ≤ 100000−i` is false — never fire.)

`BenchmarkMatchDenseSkip` (lines 58, 61, 64):

```go
			TargetPrice: Price(100_000 + i), ValidFrom: 1, AutoDeactivate: true})
```
```go
			TargetPrice: Price(900_000 - i), ValidFrom: 1, AutoDeactivate: true})
```
```go
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40}
```

(Dense shape preserved: GTE 100,000+i ≤ 500,000 all fire; LTE 900,000−i ≥ 500,000 all fire.)

- [ ] **Step 4: Run the full verification battery**

Run: `go test ./... -race -count=1`
Expected: all PASS — rebased oracle, rebased stress, and the new parity test.

Run: `go test ./engine/ -run 'TestOracle|TestMatchZeroAllocs' -race -count=1 -v`
Expected: PASS, `TestMatchZeroAllocs` reports 0 allocs.

Run: `go test ./engine/ -bench . -benchtime 1s -count=1`
Expected: PASS with `BenchmarkMatchSparse1M` ~equal-or-better than the float64 baseline (Phase A measured ~540–590 ns/op on this machine; integer compares should not regress it — treat a regression beyond run-to-run noise as a blocker and investigate before merge). `BenchmarkMatchDenseSkip` ~185 µs/op baseline. `BenchmarkMatchSparse10M` skips (env-gated, unchanged).

- [ ] **Step 5: Commit**

```bash
git add engine/oracle_test.go engine/bench_test.go
git commit -m "test(engine): integer-price oracle, string-price parity, rebased benches"
```

---

## Post-plan obligations (recorded, not implemented here)

- Phase B service layer owns the symbol→decimals config table and calls `price.Parse`/`FromFloat` at the ingestion boundary; negative-price rejection is a service-layer business rule (spec §4).
- Spec §5 of the original design (2026-09-02) documents the sparse/dense cost model; benchmark baselines above refresh those numbers for the integer representation if they shift.
