# Live Catalog Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The service learns symbols, venues, and tiers from the feed at runtime instead of rejecting anything outside the compiled-in `catalog.Default()`.

**Architecture:** `internal/catalog.Catalog` becomes a concurrent registry (`Empty()` + `EnsureSymbol`/`EnsureValue`, first-sight interning under an RWMutex; `Default()` unchanged as the simulator dataset). Scale comes from the wire string itself via a new `price.ScaleOf`. The service flips every "unknown → reject" branch to "unknown → intern and continue"; replay re-derives from each record's persisted `Decimals`. Spec: `docs/superpowers/specs/2026-09-06-live-catalog-registry-design.md`.

**Tech Stack:** Go 1.27, standard `go test` (bufconn gRPC env in `internal/service`).

## Global Constraints

- `engine/` is frozen: no engine files change.
- No proto/wire or `internal/alertstore` changes (the store already persists `Decimals` per record).
- `catalog.Default()` must keep producing the byte-identical dataset — chronofeed and the simulator depend on it; only *add* concurrency and learning.
- Never rescale silently: a symbol pinned at N decimals stays at N for the boot; anything needing more precision is an error.
- Dim values never reach `0xFFFF` (engine `DimSentinel`); `EnsureValue` fails at 65,534 values per dim.
- Every commit leaves the tree green: `go vet ./... && go test ./... -count=1`.

---

### Task 1: `price.ScaleOf`

**Files:**
- Modify: `internal/price/price.go` (add `ScaleOf` after `Parse`)
- Test: `internal/price/price_test.go` (add `TestScaleOf`)

**Interfaces:**
- Produces: `func ScaleOf(s string) (uint8, error)` — count of fractional digits in a decimal string using Parse's grammar (`[+-]? digits [ '.' digits ]`); `ErrSyntax` on garbage, `ErrScale` when the fraction exceeds `MaxDecimals` (18). Task 3 calls this before interning a symbol.

- [ ] **Step 1: Write the failing test**

Append to `internal/price/price_test.go`:

```go
func TestScaleOf(t *testing.T) {
	cases := []struct {
		in   string
		want uint8
		err  error
	}{
		{"65000", 0, nil},
		{"65000.00", 2, nil},
		{".5", 1, nil},
		{"1.", 0, nil},
		{"0.00001234", 8, nil},
		{"-12.3400", 4, nil},
		{"abc", 0, ErrSyntax},
		{"1.2.3", 0, ErrSyntax},
		{"", 0, ErrSyntax},
		{"0.1234567890123456789", 0, ErrScale},
	}
	for _, tc := range cases {
		got, err := ScaleOf(tc.in)
		if tc.err != nil {
			if !errors.Is(err, tc.err) {
				t.Fatalf("ScaleOf(%q) err = %v, want %v", tc.in, err, tc.err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ScaleOf(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("ScaleOf(%q) = %d, want %d", tc.in, got, tc.want)
		}
		// Invariant: Parse at the derived scale is exact.
		if _, err := Parse(tc.in, got); err != nil {
			t.Fatalf("Parse(%q, %d): %v", tc.in, got, err)
		}
	}
}
```

Check the file's import block already has `errors` (it does — sentinel-error assertions exist there); if not, add it.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/price/ -run TestScaleOf -count=1`
Expected: FAIL with `undefined: ScaleOf`.

- [ ] **Step 3: Implement**

Add to `internal/price/price.go` after `Parse`:

```go
// ScaleOf returns the number of fractional digits in the decimal string s
// — the smallest scale at which Parse(s, scale) is exact. Same grammar as
// Parse (sign, digits, one optional dot); a fraction longer than
// MaxDecimals is ErrScale, anything else malformed is ErrSyntax.
func ScaleOf(s string) (uint8, error) {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	seenDigit, seenDot := false, false
	frac := 0
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			seenDigit = true
			if seenDot {
				frac++
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
	if frac > MaxDecimals {
		return 0, ErrScale
	}
	return uint8(frac), nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/price/ -count=1`
Expected: PASS (existing table/property tests included).

- [ ] **Step 5: Commit**

```bash
git add internal/price/price.go internal/price/price_test.go
git commit -m "feat(price): ScaleOf derives scale from the wire string"
```

---

### Task 2: Catalog becomes a learnable registry

**Files:**
- Modify: `internal/catalog/catalog.go`
- Test: `internal/catalog/catalog_test.go` (append registry tests)

**Interfaces:**
- Produces:
  - `func Empty() *Catalog` — venue/tier dim vocabulary only; zero symbols, zero dim values.
  - `func (c *Catalog) EnsureSymbol(name string, decimals uint8) (Symbol, bool)` — interns on first sight; `ok=false` ⇔ already pinned at different decimals (returned Symbol carries the pinned entry).
  - `func (c *Catalog) EnsureValue(dim, valueName string) (uint16, bool)` — interns at the next free uint16; `ok=false` ⇔ dim exhausted.
  - Existing `Symbol`/`Symbols`/`Value`/`Name`/`DimValues`/`Dims` keep their signatures, now safe under concurrent `Ensure*`.
- `Default()` output is unchanged byte-for-byte.

- [ ] **Step 1: Write the failing tests**

Append to `internal/catalog/catalog_test.go` (white-box: same `package catalog`; add `"strconv"` and `"sync"` to imports):

```go
func TestEnsureSymbolFirstSightWins(t *testing.T) {
	c := Empty()
	s, ok := c.EnsureSymbol("NEW", 4)
	if !ok || s.Name != "NEW" || s.Decimals != 4 {
		t.Fatalf("first sight: %+v ok=%v", s, ok)
	}
	s, ok = c.EnsureSymbol("NEW", 2)
	if ok || s.Decimals != 4 {
		t.Fatalf("conflict not detected: %+v ok=%v", s, ok)
	}
	if _, ok = c.Symbol("NEW"); !ok {
		t.Fatal("interned symbol not visible to Symbol")
	}
	if got := len(c.Symbols()); got != 1 {
		t.Fatalf("len(Symbols()) = %d, want 1", got)
	}
	if s.Reference != "" {
		t.Fatalf("learned symbol Reference = %q, want empty", s.Reference)
	}
}

func TestEnsureValueInternsNextFree(t *testing.T) {
	c := Empty()
	a, ok := c.EnsureValue(DimVenue, "AAA")
	if !ok || a != 0 {
		t.Fatalf("first value = %d ok=%v, want 0", a, ok)
	}
	b, ok := c.EnsureValue(DimVenue, "BBB")
	if !ok || b != 1 {
		t.Fatalf("second value = %d ok=%v, want 1", b, ok)
	}
	if again, ok := c.EnsureValue(DimVenue, "AAA"); !ok || again != 0 {
		t.Fatalf("re-ensure = %d ok=%v, want 0", again, ok)
	}
	if n, ok := c.Name(DimVenue, 1); !ok || n != "BBB" {
		t.Fatalf("Name(1) = %q ok=%v", n, ok)
	}
	if vals := c.DimValues(DimVenue); len(vals) != 2 || vals[0] != "AAA" {
		t.Fatalf("DimValues = %v", vals)
	}
}

func TestEnsureValueDimExhaustion(t *testing.T) {
	c := Empty()
	for i := 0; i < 0xFFFF; i++ {
		name := "v" + strconv.Itoa(i)
		c.values[DimVenue][name] = uint16(i)
		c.names[DimVenue][uint16(i)] = name
	}
	if _, ok := c.EnsureValue(DimVenue, "overflow"); ok {
		t.Fatal("EnsureValue should fail at 65534 values (DimSentinel reserved)")
	}
}

func TestEnsureConcurrent(t *testing.T) {
	c := Empty()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.EnsureSymbol("SHARED", 2)
				c.EnsureValue(DimVenue, "V"+strconv.Itoa(g))
				c.Symbol("SHARED")
				c.Value(DimVenue, "V0")
			}
		}(g)
	}
	wg.Wait()
	if s, ok := c.Symbol("SHARED"); !ok || s.Decimals != 2 {
		t.Fatalf("SHARED = %+v ok=%v", s, ok)
	}
	if got := len(c.Symbols()); got != 1 {
		t.Fatalf("symbols = %d, want exactly 1 registration", got)
	}
	if got := len(c.DimValues(DimVenue)); got != 8 {
		t.Fatalf("venue values = %d, want 8", got)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/catalog/ -count=1`
Expected: FAIL with `undefined: Empty` (and `undefined: c.EnsureSymbol` etc.).

- [ ] **Step 3: Implement**

In `internal/catalog/catalog.go`:

1. Add `"sync"` to imports.

2. Change the `Catalog` struct and its doc comment (around line 33) from:

```go
// Catalog is immutable after construction; safe for concurrent use.
type Catalog struct {
	symbols map[string]Symbol
	order   []Symbol
	values  map[string]map[string]uint16
	names   map[string]map[uint16]string
	dims    []string
}
```

to:

```go
// Catalog is safe for concurrent use. Reads (Symbol, Value, Name, …) take
// a shared lock; Ensure* intern on first sight under the write lock. dims
// is fixed after construction and read without locking.
type Catalog struct {
	mu      sync.RWMutex
	symbols map[string]Symbol
	order   []Symbol
	values  map[string]map[string]uint16
	names   map[string]map[uint16]string
	dims    []string
}
```

3. Guard the read methods (bodies unchanged, wrapped):

```go
func (c *Catalog) Symbol(name string) (Symbol, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.symbols[name]
	return s, ok
}

func (c *Catalog) Symbols() []Symbol {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order
}

func (c *Catalog) Value(dim, valueName string) (uint16, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[dim][valueName]
	return v, ok
}

func (c *Catalog) Name(dim string, value uint16) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.names[dim][value]
	return n, ok
}
```

(`Decimals` and `DimValues` read via `c.symbols`/`c.names` — wrap `Decimals` in RLock the same way; `DimValues` loops `c.names[dim]`, wrap in RLock too. `Dims()` returns the immutable slice — leave unlocked.)

4. Append after `Default()`:

```go
// Empty returns a registry with only the positional dim vocabulary:
// zero symbols, zero dim values. It learns from the feed at runtime;
// Default() is the compiled-in simulator dataset, not service law.
func Empty() *Catalog {
	c := &Catalog{
		symbols: map[string]Symbol{},
		values:  map[string]map[string]uint16{},
		names:   map[string]map[uint16]string{},
		dims:    []string{DimVenue, DimTier},
	}
	for _, dim := range c.dims {
		c.values[dim] = map[string]uint16{}
		c.names[dim] = map[uint16]string{}
	}
	return c
}

// EnsureSymbol returns the entry for name, interning it at decimals on
// first sight. ok=false ⇔ name is already pinned at different decimals
// (the returned Symbol carries the pinned entry; the caller must fail,
// never rescale).
func (c *Catalog) EnsureSymbol(name string, decimals uint8) (Symbol, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.symbols[name]; ok {
		return s, s.Decimals == decimals
	}
	s := Symbol{Name: name, Decimals: decimals}
	c.symbols[name] = s
	c.order = append(c.order, s)
	return s, true
}

// EnsureValue returns the engine uint16 for a dim value, interning it at
// the next free slot on first sight. ok=false ⇔ the dim is exhausted
// (65,534 values; DimSentinel 0xFFFF is reserved by the engine).
func (c *Catalog) EnsureValue(dim, valueName string) (uint16, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.values[dim][valueName]; ok {
		return v, true
	}
	v := uint16(len(c.names[dim]))
	if v == 0xFFFF {
		return 0, false
	}
	c.values[dim][valueName] = v
	c.names[dim][v] = valueName
	return v, true
}
```

Note: `Default()` constructs without locks — fine, the catalog is not shared until returned.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/catalog/ -race -count=1`
Expected: PASS — the new tests plus every existing `Default()` test (determinism, majors anchored, ~500 count) unchanged and green.

- [ ] **Step 5: Commit**

```bash
git add internal/catalog/catalog.go internal/catalog/catalog_test.go
git commit -m "feat(catalog): concurrent registry with first-sight interning"
```

---

### Task 3: The service learns from the feed

**Files:**
- Modify: `internal/service/core.go` — `ingestTick` (~line 412), `StreamTicks` (~line 468), `UpsertAlert` (~lines 277, 293-300), `specFrom` (~line 194), `replay` (~line 147), `NewCore` (~line 135), `venueTicks` field (~line 96), `VenueTicks` (~line 505)
- Modify: `internal/service/testenv_test.go:93` — env builds on `catalog.Empty()`
- Modify: `internal/service/replay_test.go:21` — restart env builds on `catalog.Empty()`
- Modify: `internal/service/alerts_test.go` — rewrite `TestUpsertValidation` (~line 57)
- Modify: `internal/service/feed_test.go` — replace `TestStreamTicksRejectsUnknownRefdata` (~line 107), fix comment in `TestStreamTicksDropsBadPriceOnly` (~line 90)

**Interfaces:**
- Consumes: Task 1 `price.ScaleOf`, Task 2 `catalog.Empty`/`EnsureSymbol`/`EnsureValue`.
- Produces: `specFrom(a alertstore.Alert) engine.AlertSpec` (bool return deleted — its only false path was catalog drift); `Core.venueTicks` becomes `sync.Map`. No signature otherwise changes.

- [ ] **Step 1: Write the failing tests**

In `internal/service/feed_test.go`, replace `TestStreamTicksRejectsUnknownRefdata` entirely with:

```go
func TestStreamTicksAcceptsUnknownRefdata(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("NOSUCH", "1.00", "1.01", "ATLAS", "TOP"),
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 2 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %+v, want 2 accepted 0 dropped", fs)
	}
	fs, err = runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "1.00", "1.01", "BINANCE", "TOP"),
	}})
	if err != nil {
		t.Fatalf("unknown venue rejected: %v", err)
	}
	if fs.GetAccepted() != 1 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %+v, want 1 accepted 0 dropped", fs)
	}
}
```

Append to `internal/service/feed_test.go`:

```go
// TestLearnedSymbolFiresAlert is the headline scenario: an alert on a
// never-seen symbol, then the symbol's first tick — interned, matched,
// fired. The trailing feed also interns an unseen venue (BINANCE).
func TestLearnedSymbolFiresAlert(t *testing.T) {
	e := newEnv(t)
	_, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "NEWPAIR", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "12.34", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatalf("upsert on unknown symbol: %v", err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("NEWPAIR", "12.30", "12.40", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	waitTriggers(t, e, 1)
	tr := e.rec.triggers()
	if len(tr) != 1 || tr[0].Symbol != "NEWPAIR" {
		t.Fatalf("triggers = %+v, want 1 NEWPAIR", tr)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("TICKFIRST", "1.500", "1.600", "BINANCE", "DEEP"),
	}}); err != nil {
		t.Fatal(err)
	}
	if vt := e.core.VenueTicks(); vt["BINANCE"] != 1 {
		t.Fatalf("venue ticks = %v, want BINANCE=1", vt)
	}
}
```

In `internal/service/alerts_test.go`, rewrite `TestUpsertValidation` — the shared env now needs a seed that pins BTCUSDT at 2 decimals, the three "unknown …" cases become valid input, and a scale-conflict case replaces them:

```go
func TestUpsertValidation(t *testing.T) {
	e := newEnv(t)
	// Pin BTCUSDT at 2 decimals so the precision-loss and conflict cases
	// below fail price conversion, not first-sight interning.
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		mut  func(*chronov1.UpsertAlertRequest)
		want codes.Code
	}{
		{"no price type", func(r *chronov1.UpsertAlertRequest) { r.PriceType = chronov1.PriceType_PRICE_TYPE_UNSPECIFIED }, codes.InvalidArgument},
		{"no direction", func(r *chronov1.UpsertAlertRequest) { r.Direction = chronov1.Direction_DIRECTION_UNSPECIFIED }, codes.InvalidArgument},
		{"precision loss", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "65000.005" }, codes.InvalidArgument},
		{"bad number", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "abc" }, codes.InvalidArgument},
		{"overflow", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "99999999999999999.00" }, codes.InvalidArgument},
		{"symbol scale conflict", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "65000.000" }, codes.InvalidArgument},
		{"expires before valid", func(r *chronov1.UpsertAlertRequest) { r.ValidFromUnixNanos = 200; r.ExpiresUnixNanos = 100 }, codes.InvalidArgument},
		{"empty symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "" }, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validUpsert()
			tc.mut(req)
			_, err := e.alerts().UpsertAlert(context.Background(), req)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v, want %v (err %v)", status.Code(err), tc.want, err)
			}
			if got := e.core.AlertCount(); got != 1 { // the seed only
				t.Fatalf("rejected upsert left %d alerts behind", got)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/service/ -run 'TestStreamTicksAcceptsUnknownRefdata|TestLearnedSymbolFiresAlert|TestUpsertValidation' -count=1`
Expected: FAIL — unknown symbol/venue rejected (`unknown symbol "NOSUCH"` / batch-reject), so acceptance counts and trigger assertions don't hold; the rewritten `TestUpsertValidation` subtests may pass by coincidence of old errors, the two new tests must not.

- [ ] **Step 3: Switch the test envs to Empty()**

In `internal/service/testenv_test.go` `newEnvWithPub` (~line 93): `cat := catalog.Default()` → `cat := catalog.Empty()`.
In `internal/service/replay_test.go` `newEnvAtPath` (~line 21): `cat := catalog.Default()` → `cat := catalog.Empty()`.
(`stress_test.go`/`oracle_test.go` keep calling `catalog.Default()` — that is generator input for symbol lists, not the service's registry.)

Run: `go test ./internal/service/ -count=1` — expect failures ONLY in the paths this task rewrites (unknown-refdata rejections, replay skips). Any other failure means an unexpected dependency on the pre-seeded catalog — report it, don't paper over it.

- [ ] **Step 4: Rewrite core.go**

1. **Field** (~line 96): `venueTicks map[string]*atomic.Uint64 // venue → accepted ticks; fixed keys, written at construction` →

```go
	venueTicks sync.Map // venue name → *atomic.Uint64, interned on first sight
```

2. **NewCore** (~lines 135-137): delete both lines

```go
	c.venueTicks = make(map[string]*atomic.Uint64, len(cat.DimValues(catalog.DimVenue)))
	for _, v := range cat.DimValues(catalog.DimVenue) {
		c.venueTicks[v] = &atomic.Uint64{}
	}
```

3. **specFrom** (~lines 194-218) — drop the bool, intern from the record (the store's `Decimals` is authoritative):

```go
// specFrom rebuilds the engine submission for a persisted record. The
// record itself re-interns its symbol (at the store's persisted decimals)
// and its venue/tier — replay can never drift from the registry.
func (c *Core) specFrom(a alertstore.Alert) engine.AlertSpec {
	sym, _ := c.Cat.EnsureSymbol(a.Symbol, a.Decimals)
	vv, _ := c.Cat.EnsureValue(catalog.DimVenue, a.Venue)
	tv, _ := c.Cat.EnsureValue(catalog.DimTier, a.Tier)
	return engine.AlertSpec{
		ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
		TargetPrice:    a.TargetPrice,
		ValidFrom:      a.ValidFrom,
		Expires:        a.Expires,
		AutoDeactivate: a.AutoDeactivate,
		Dims:           engine.Dims(vv, tv),
	}
}
```

4. **replay** (~lines 147-190): the caller changes with it —

```go
	var restored, expired int
```

(delete `skipped`), the loop body drops the drift branch:

```go
		spec := c.specFrom(a)
		if err := c.eng.Upsert(spec); err != nil {
			return fmt.Errorf("replay upsert %s: %w", alertIDString(a.ID), err)
		}
```

and the log line becomes:

```go
	slog.Info("alert store replay",
		"restored", restored, "expired", expired)
```

5. **UpsertAlert** (~lines 277-300): replace the symbol and venue/tier lookups (price-type/direction checks, parse, expiry check, store-first logic all unchanged):

```go
	sym, ok := c.Cat.Symbol(req.GetSymbol())
	if !ok {
		dec, serr := price.ScaleOf(req.GetTargetPrice())
		if serr != nil {
			return nil, invalidf("target price %q: %v", req.GetTargetPrice(), serr)
		}
		if sym, ok = c.Cat.EnsureSymbol(req.GetSymbol(), dec); !ok {
			return nil, invalidf("symbol %q quotes %d decimals", req.GetSymbol(), sym.Decimals)
		}
	}
```

and:

```go
	vv, ok := c.Cat.EnsureValue(catalog.DimVenue, req.GetVenue())
	if !ok {
		return nil, invalidf("venue vocabulary exhausted")
	}
	tv, ok := c.Cat.EnsureValue(catalog.DimTier, req.GetTier())
	if !ok {
		return nil, invalidf("tier vocabulary exhausted")
	}
```

6. **errUnknownRefdata + ingestTick** (~lines 400-447): delete the `errUnknownRefdata` var and its comment block; `ingestTick` becomes lookup-first so the hot path stays read-locked:

```go
// ingestTick converts one wire tick to an engine tick and Matches it.
// Unknown symbol/venue/tier are interned on first sight — the feed
// defines the vocabulary. Errors: price/scale problems (drop this tick
// only) or dim exhaustion (also per-tick).
func (c *Core) ingestTick(t *chronov1.Tick, now time.Time) error {
	sym, ok := c.Cat.Symbol(t.GetSymbol())
	if !ok {
		dec, serr := price.ScaleOf(t.GetBid())
		if serr != nil {
			return fmt.Errorf("bid %q: %v", t.GetBid(), serr)
		}
		if sym, ok = c.Cat.EnsureSymbol(t.GetSymbol(), dec); !ok {
			return fmt.Errorf("symbol %q quotes %d decimals", t.GetSymbol(), sym.Decimals)
		}
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, t.GetVenue())
	if !ok {
		if vv, ok = c.Cat.EnsureValue(catalog.DimVenue, t.GetVenue()); !ok {
			return fmt.Errorf("venue vocabulary exhausted")
		}
	}
	tv, ok := c.Cat.Value(catalog.DimTier, t.GetTier())
	if !ok {
		if tv, ok = c.Cat.EnsureValue(catalog.DimTier, t.GetTier()); !ok {
			return fmt.Errorf("tier vocabulary exhausted")
		}
	}
```

(the `price.Parse` bid/ask block, engine `Match`, stats/metrics lines are unchanged). The counter write at the end becomes:

```go
	ctr, _ := c.venueTicks.LoadOrStore(t.GetVenue(), &atomic.Uint64{})
	ctr.(*atomic.Uint64).Add(1)
	return nil
```

7. **StreamTicks** (~line 468): the error branch simplifies — delete

```go
			if errors.Is(err, errUnknownRefdata) {
				return status.Errorf(codes.InvalidArgument, "batch rejected: %v", err)
			}
```

leaving the drop-and-count path. Remove `errors` from imports only if nothing else in the file still uses it (grep first — `errors.Is` appears elsewhere, e.g. CancelAlert).

8. **VenueTicks** (~line 505):

```go
// VenueTicks reports accepted ticks per venue.
func (c *Core) VenueTicks() map[string]uint64 {
	out := map[string]uint64{}
	c.venueTicks.Range(func(v, ctr any) {
		out[v.(string)] = ctr.(*atomic.Uint64).Load()
	})
	return out
}
```

9. In `feed_test.go` `TestStreamTicksDropsBadPriceOnly`, update the trailing comment `// precision loss` → `// scale conflict: BTCUSDT pinned at 2 by the first tick`.

- [ ] **Step 5: Run the service suite**

Run: `go vet ./... && go test ./internal/service/ -race -count=1`
Expected: PASS — including `TestRestartReplayAndFire` (now on `Empty()`, proving replay re-derives from persisted `Decimals`), the oracle and stress suites, and the three rewritten/new tests. If a test still asserts old reject behavior (search: `unknown symbol`, `unknown venue`, `valid:`, `RejectsUnknownRefdata`, `skipped`), rewrite it to the acceptance semantics per the spec — the grep must come back empty for `internal/`.

- [ ] **Step 6: Commit**

```bash
git add internal/service/
git commit -m "feat(service): intern unknown symbols and dim values from the feed"
```

---

### Task 4: chronod runs on the empty registry

**Files:**
- Modify: `cmd/chronod/main.go:78`
- Test: `internal/service/catalog_serve_test.go` (new file)

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: production boots `catalog.Empty()`; `GetCatalog` serves the live registry (proto unchanged).

- [ ] **Step 1: Write the failing test**

Create `internal/service/catalog_serve_test.go`:

```go
package service

import (
	"context"
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
	rep, err := e.alerts().GetCatalog(context.Background(), &chronov1.CatalogRequest{})
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
	for _, d := range rep.GetDims() {
		if d.GetName() == "venue" && len(d.GetValues()) == 0 {
			t.Fatal("venue dim served empty after an ATLAS upsert")
		}
	}
}
```

Check `GetCatalog`'s registration: it is served by whichever of `e.alerts()`/`e.feed()` the proto wires it to — if `e.alerts()` does not compile against it, use `e.feed()`. Run first: `go test ./internal/service/ -run TestGetCatalogLearns -count=1`. This passes already if `newEnv` is on `Empty()` (Task 3) — its role is regression-pinning the served view; if it passes on the first run, that is the expected outcome, note it and move on.

- [ ] **Step 2: Switch production to the empty registry**

In `cmd/chronod/main.go` line 78:

```go
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, publisher, store)
```

→

```go
	core := service.NewCore(engine.DefaultConfig(), catalog.Empty(), pm, st, publisher, store)
```

(chronofeed keeps `catalog.Default()` — it is the simulator's dataset.)

- [ ] **Step 3: Full verification**

Run: `go vet ./... && go test ./... -race -count=1`
Expected: PASS, all packages — including `engine/` conformance/oracle/parity (untouched) and `internal/server` (its tests keep a `Default()`-seeded core, which now doubles as the proof that a pre-seeded registry still works).

Optional smoke (manual): `go build -o ./chronod ./cmd/chronod && go build -o ./chronofeed ./scripts/chronofeed`, run both, and watch `/stats` — `symbol_count` and the venues list grow as chronofeed streams.

- [ ] **Step 4: Commit**

```bash
git add cmd/chronod/main.go internal/service/catalog_serve_test.go
git commit -m "feat(chronod): boot on the empty catalog, learn from the feed"
```
