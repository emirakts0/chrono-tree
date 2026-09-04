# Service Layer (chronod / chronofeed / chronoctl) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the gRPC service layer around the existing engine: `chronod` (engine host with gRPC + HTTP status), `chronofeed` (crypto market simulator streaming ~20k ticks/s), and `chronoctl` (demo client that registers alerts and watches triggers).

**Architecture:** `chronod` wraps `engine.Engine` in a `service.Core` that owns the reference-data catalog (symbols → decimals, dim vocabulary), a service-side alert catalog, a trigger pump that fans engine triggers out to `WatchTriggers` streams, and a separate `net/http` status server. `chronofeed` and `chronoctl` are gRPC clients. Canonical `google.golang.org/grpc`, protobuf codegen via `buf` (generated code committed).

**Tech Stack:** Go 1.27, google.golang.org/grpc, google.golang.org/protobuf, bufbuild/buf (codegen only), bufbuild/protovalidate-go, prometheus/client_golang, stdlib `uuid` (UUIDv7), `encoding/json/v2`, `math/rand/v2` (ChaCha8), `log/slog`, go.uber.org/goleak (already indirect).

## Global Constraints

- **Engine and price packages are FROZEN:** zero diffs to `engine/` and `price/` (spec §12). All new code lives in `cmd/`, `api/`, `internal/`.
- **Prices on the wire are decimal strings** (`"65000.12"`), converted only via `price.Parse`/`price.Format` at the service boundary; float64 never touches a price after conversion (the simulator's internal random walk may use float64 to *generate* synthetic data, but every emitted price is an exact integer in base units formatted by `price.Format`).
- **Engine dims:** `Config.Dims = []string{"venue", "tier"}` (width 2); dim arrays built with `engine.Dims(venueVal, tierVal)`. Venues `ATLAS, NOVA, ZENITH`; tiers `TOP, MID` (fictional).
- **Proto package** `chrono.v1`; generated Go at `api/gen/chrono/v1`, package `chronov1`, import alias `chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"`. Generated code is COMMITTED (builds must not require buf).
- **Metrics names** exactly: `chrono_ticks_total{venue,tier}`, `chrono_triggers_fired_total{symbol,venue,tier}`, `chrono_triggers_delivered_total`, `chrono_trigger_drops_total`, `chrono_ticks_dropped_total`, `chrono_alerts_active`, `chrono_watchers`, `chrono_feed_connected`, `chrono_tick_batch_size`, `chrono_tick_latency_seconds`.
- **HTTP endpoints** exactly: `GET /healthz`, `GET /readyz`, `GET /stats` (json/v2), `GET /metrics`, `/debug/pprof/...`.
- **Ports default:** gRPC `-grpc-addr :9090`, HTTP `-http-addr :8080`.
- **Alert IDs:** stdlib `uuid` (`import "uuid"`), `uuid.NewV7()`, stored as `engine.AlertID` (uuid.UUID is [16]byte — convert directly). On the wire: canonical UUID string form.
- **Batch reject vs tick drop** (spec §8): a tick with unknown symbol/venue/tier → whole `StreamTicks` batch rejected `InvalidArgument`; a syntactically valid tick whose price fails `price.Parse` → that tick dropped and counted, batch proceeds.
- **Feed rate gate:** sustained `-rate 20000` must show ≥19.5k ticks/s in `/stats`, zero unexplained watcher drops.
- Every task: `go build ./... && go vet ./...` clean; tests green before commit. Suite runs with `-race`.
- Commit messages: `feat(...)`, `test(...)`, `docs(...)` style used in this repo.

## Engine API cheat sheet (frozen, do not modify)

```go
engine.New(engine.Config{... DefaultConfig() fields ..., Dims: []string{"venue","tier"}}) *Engine
(*Engine).Upsert(engine.AlertSpec) error      // queued; call Sync() after for visibility
(*Engine).Cancel(engine.AlertID) error        // ErrNotFound, ErrInvalidTransition
(*Engine).Sync()
(*Engine).Match(*engine.Tick)                 // fire-and-forget, never blocks
(*Engine).Triggers() *TriggerQueue            // .Pop() (Trigger, bool), .PopBatch([]Trigger) int, .Dropped() uint64
(*Engine).Stats() engine.Stats                // {Live, DroppedTriggers uint64}
(*Engine).Close()                             // idempotent; call AFTER pumps stop submitting

engine.AlertSpec{ID AlertID /*[16]byte*/; Symbol string; PriceType; Direction; TargetPrice Price /*int64*/;
                 ValidFrom int64; Expires int64; AutoDeactivate bool; Dims [8]uint16; Meta engine.AlertMeta}
engine.Tick{Symbol string; Bid, Ask, Mid, Last Price; Present uint8; TS int64; Dims [8]uint16}
engine.Dims(values ...uint16) [8]uint16       // sentinel-pads; panics >8
engine.Trigger{ID AlertID; Price Price; TS int64}
engine.PriceBid/PriceAsk/PriceMid/PriceLast (PriceType: 0,1,2,3); engine.DirGTE/DirLTE
engine.TickAllPresent() uint8
engine errors: ErrClosed, ErrNotFound, ErrInvalidStatus, ErrInvalidTransition, ErrSymbolLimit, ErrAlertLimit, ErrDims
price.Parse(s string, decimals uint8) (int64, error)   // ErrPrecisionLoss, ErrOverflow (both wrapped errors)
price.Format(v int64, decimals uint8) string
```

---

### Task 1: Proto definitions, buf codegen, dependencies

**Files:**
- Create: `api/proto/chrono/v1/chrono.proto`
- Create: `buf.yaml`
- Create: `buf.gen.yaml`
- Create: `api/gen/chrono/v1/chrono.pb.go`, `api/gen/chrono/v1/chrono_grpc.pb.go` (generated, committed)
- Create: `api/gen/chrono/v1/doc_test.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: package `chronov1` with messages `UpsertAlertRequest{Symbol, PriceType, Direction, TargetPrice, Venue, Tier, ValidFromUnixNanos, ExpiresUnixNanos, AutoDeactivate, AlertId}`, `UpsertAlertResponse{AlertId}`, `CancelAlertRequest{AlertId}`, `CancelAlertResponse`, `WatchTriggersRequest`, `Trigger{AlertId, Symbol, Venue, Tier, FiredPrice, FiredAtUnixNanos, Direction, TargetPrice}`, `Tick{Symbol, Bid, Ask, Venue, Tier, TsUnixNanos}`, `TickBatch{Ticks}`, `FeedStatus{Accepted, Dropped}`, `SymbolInfo{Symbol, Decimals, ReferencePrice}`, `DimInfo{Name, Values}`, `CatalogRequest`, `CatalogReply{Symbols, Dims}`; enums `PriceType` (`PRICE_TYPE_BID/ASK/MID/LAST`), `Direction` (`DIRECTION_ABOVE` = market ≥ target, `DIRECTION_BELOW` = market ≤ target); server interfaces `AlertServiceServer` (incl. `WatchTriggers(*WatchTriggersRequest, AlertService_WatchTriggersServer) error`), `FeedServiceServer` (incl. `StreamTicks(FeedService_StreamTicksServer) error`), and registration funcs `RegisterAlertServiceServer`/`RegisterFeedServiceServer`; client stubs `NewAlertServiceClient`/`NewFeedServiceClient`. Later tasks rely on exactly these names.

- [ ] **Step 1: Write `api/proto/chrono/v1/chrono.proto`**

```protobuf
syntax = "proto3";

package chrono.v1;

import "buf/validate/validate.proto";

option go_package = "github.com/emir/chrono-tree/api/gen/chrono/v1;chronov1";

// PriceType selects which quote field an alert watches.
enum PriceType {
  PRICE_TYPE_UNSPECIFIED = 0;
  PRICE_TYPE_BID = 1;
  PRICE_TYPE_ASK = 2;
  PRICE_TYPE_MID = 3;
  PRICE_TYPE_LAST = 4;
}

// Direction of the alert. ABOVE fires when market >= target (engine DirGTE),
// BELOW fires when market <= target (engine DirLTE).
enum Direction {
  DIRECTION_UNSPECIFIED = 0;
  DIRECTION_ABOVE = 1;
  DIRECTION_BELOW = 2;
}

service AlertService {
  // UpsertAlert inserts an alert or atomically replaces the one with the
  // same id. The server generates a UUIDv7 id when alert_id is empty.
  // The alert is guaranteed visible to Match before the response returns.
  rpc UpsertAlert(UpsertAlertRequest) returns (UpsertAlertResponse);
  rpc CancelAlert(CancelAlertRequest) returns (CancelAlertResponse);
  // WatchTriggers streams every trigger fired by the engine. A watcher that
  // falls behind is disconnected with ResourceExhausted; it may reconnect.
  rpc WatchTriggers(WatchTriggersRequest) returns (stream Trigger);
}

service FeedService {
  // StreamTicks is a client-streaming feed of market ticks. Batches of up
  // to 64. A batch containing a tick with unknown symbol/venue/tier is
  // rejected with InvalidArgument; a tick whose price string is not exactly
  // representable is dropped and counted, the rest of the batch proceeds.
  rpc StreamTicks(stream TickBatch) returns (FeedStatus);
  // GetCatalog returns the symbol table (with decimals and a reference
  // price) and the dimension vocabulary.
  rpc GetCatalog(CatalogRequest) returns (CatalogReply);
}

message UpsertAlertRequest {
  string symbol = 1 [(buf.validate.field).string.pattern = "^[A-Z0-9]{5,20}$"]; // e.g. "BTCUSDT"; must be in the catalog
  PriceType price_type = 2;
  Direction direction = 3;
  string target_price = 4 [(buf.validate.field).string.pattern = "^[0-9]+(\\.[0-9]+)?$"]; // decimal string; exact at the symbol's decimals
  string venue = 5;          // dim 0 value; must be in the catalog
  string tier = 6;           // dim 1 value; must be in the catalog
  int64 valid_from_unix_nanos = 7; // 0 = immediately
  int64 expires_unix_nanos = 8;    // 0 = never
  bool auto_deactivate = 9;
  bytes alert_id = 10 [(buf.validate.field).bytes = {max_len: 16}]; // optional 16-byte id; generated when empty
}

message UpsertAlertResponse { string alert_id = 1; } // UUID string form

message CancelAlertRequest {
  string alert_id = 1 [(buf.validate.field).string = {len: 36, pattern: "^[0-9a-fA-F-]{36}$"}]; // UUID string form
}
message CancelAlertResponse {}

message WatchTriggersRequest {}

message Trigger {
  string alert_id = 1;       // UUID string form
  string symbol = 2;
  string venue = 3;
  string tier = 4;
  string fired_price = 5;    // decimal string at the symbol's decimals
  int64 fired_at_unix_nanos = 6;
  Direction direction = 7;
  string target_price = 8;   // decimal string, the alert's target
}

message Tick {
  string symbol = 1;
  string bid = 2;            // decimal string at the symbol's decimals
  string ask = 3;
  string venue = 4;
  string tier = 5;
  int64 ts_unix_nanos = 6;
}

message TickBatch { repeated Tick ticks = 1; }

message FeedStatus {
  uint64 accepted = 1;
  uint64 dropped = 2;
}

message SymbolInfo {
  string symbol = 1;
  uint32 decimals = 2;
  string reference_price = 3; // decimal string; anchor for simulators/clients
}

message DimInfo {
  string name = 1;            // e.g. "venue", "tier"
  repeated string values = 2; // ordered, positionally significant
}

message CatalogRequest {}
message CatalogReply {
  repeated SymbolInfo symbols = 1;
  repeated DimInfo dims = 2;
}
```

- [ ] **Step 2: Write `buf.yaml` and `buf.gen.yaml`**

`buf.yaml`:
```yaml
version: v2
modules:
  - path: api/proto
deps:
  - buf.build/bufbuild/protovalidate
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

`buf.gen.yaml`:
```yaml
version: v2
plugins:
  - local: protoc-gen-go
    out: api/gen
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: api/gen
    opt: paths=source_relative
```

- [ ] **Step 3: Install toolchain and generate**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/bufbuild/buf/cmd/buf@latest
export PATH="$HOME/go/bin:$PATH"
buf dep update   # fetches buf.build/bufbuild/protovalidate (buf.validate annotations)
buf lint && buf generate
```

Expected: `api/gen/chrono/v1/chrono.pb.go` and `chrono_grpc.pb.go` exist, plus `api/gen/buf/validate/validate.pb.go` (generated from the protovalidate module — commit it too).

- [ ] **Step 4: Add dependencies**

```bash
go get google.golang.org/grpc@latest google.golang.org/protobuf@latest \
  buf.build/go/protovalidate@latest \
  github.com/prometheus/client_golang@latest
go mod tidy
```

- [ ] **Step 5: Write compile smoke test `api/gen/chrono/v1/doc_test.go`**

```go
package chronov1_test

import (
	"testing"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// The generated code is committed; this pins that both services and the
// streaming methods exist with the shapes later tasks call.
func TestGeneratedStubs(t *testing.T) {
	var _ = chronov1.PriceType_PRICE_TYPE_BID
	var _ = chronov1.Direction_DIRECTION_ABOVE
	// Interface satisfaction is checked at registration in cmd/chronod;
	// here we only pin the constructor symbols exist.
	_ = chronov1.RegisterAlertServiceServer
	_ = chronov1.RegisterFeedServiceServer
}
```

- [ ] **Step 6: Run test, build, vet**

Run: `go test ./api/... && go build ./... && go vet ./...`
Expected: PASS, clean.

- [ ] **Step 7: Commit**

```bash
git add api/ buf.yaml buf.gen.yaml go.mod go.sum
git commit -m "feat(api): chrono.v1 proto definitions and generated stubs"
```

---

### Task 2: `internal/catalog` — reference data

**Files:**
- Create: `internal/catalog/catalog.go`
- Test: `internal/catalog/catalog_test.go`

**Interfaces:**
- Consumes: `price.Parse`, `price.Format`.
- Produces (used by Tasks 4–11):

```go
type Symbol struct { Name string; Decimals uint8; Reference string } // Reference = decimal string anchor
type Catalog struct{ ... }
func Default() *Catalog                                        // ~500 symbols, deterministic
func (c *Catalog) Symbol(name string) (Symbol, bool)
func (c *Catalog) Symbols() []Symbol                           // deterministic order
func (c *Catalog) Decimals(name string) (uint8, bool)
func (c *Catalog) Dims() []string                              // ["venue", "tier"]
func (c *Catalog) Value(dim, valueName string) (uint16, bool)  // name → dim value
func (c *Catalog) Name(dim string, value uint16) (string, bool) // value → name
func (c *Catalog) DimValues(dim string) []string
const DimVenue = "venue"; const DimTier = "tier"
var Venues = []string{"ATLAS", "NOVA", "ZENITH"}
var Tiers = []string{"TOP", "MID"}
```

- [ ] **Step 1: Write the failing test `internal/catalog/catalog_test.go`**

```go
package catalog

import (
	"slices"
	"testing"

	"github.com/emir/chrono-tree/price"
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/catalog/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `internal/catalog/catalog.go`**

```go
// Package catalog owns the service's reference data: the symbol table
// (pair → decimals + reference price) and the dimension vocabulary
// (dim name → value name → uint16). It is compiled-in and shared by all
// three binaries; GetCatalog is its wire view. Values are 0..N-1 per dim;
// the engine's DimSentinel (0xFFFF) never appears here.
package catalog

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"

	"github.com/emir/chrono-tree/price"
)

// Dim names, positionally significant: slot 0 = venue, slot 1 = tier
// (engine.Config.Dims order).
const (
	DimVenue = "venue"
	DimTier  = "tier"
)

// Venues are deliberately fictional exchanges. Tiers are book levels.
var (
	Venues = []string{"ATLAS", "NOVA", "ZENITH"}
	Tiers  = []string{"TOP", "MID"}
)

// Symbol is one tradable pair. Reference is a decimal-string anchor price
// the simulator walks from and clients price alerts against.
type Symbol struct {
	Name      string
	Decimals  uint8
	Reference string
}

// Catalog is immutable after construction; safe for concurrent use.
type Catalog struct {
	symbols map[string]Symbol
	order   []Symbol
	values  map[string]map[string]uint16
	names   map[string]map[uint16]string
	dims    []string
}

// bases are the real currencies the synthetic market is built from.
var bases = []string{
	"BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOGE", "DOT", "LINK",
	"MATIC", "LTC", "UNI", "ATOM", "NEAR", "APT", "ARB", "OP", "TON", "TRX",
	"SUI", "SHIB", "PEPE", "WIF", "BONK", "FLOKI", "ORDI", "INJ",
}

// quotes: pairs are listed base/quote.
var quotes = []string{"USDT", "BTC", "ETH"}

// anchored: majors and memecoins get hard-coded realistic prices at the
// pair's decimals; everything else derives a deterministic anchor from
// its name.
var anchored = map[string]string{
	"BTCUSDT": "65000.00", "ETHUSDT": "3400.00", "BNBUSDT": "580.00",
	"SOLUSDT": "150.0000", "XRPUSDT": "0.5200", "DOGEBTC": "0.00000225",
	"ADAUSDT": "0.4500", "PEPEUSDT": "0.00001234", "SHIBUSDT": "0.00001850",
}

const synthTail = 420 // synthetic TKNnnn pairs fill the long tail to ~500

func Default() *Catalog {
	c := &Catalog{
		symbols: make(map[string]Symbol),
		values:  map[string]map[string]uint16{},
		names:   map[string]map[uint16]string{},
		dims:    []string{DimVenue, DimTier},
	}
	for dim, vals := range map[string][]string{DimVenue: Venues, DimTier: Tiers} {
		c.values[dim] = make(map[string]uint16, len(vals))
		c.names[dim] = make(map[uint16]string, len(vals))
		for i, v := range vals {
			c.values[dim][v] = uint16(i)
			c.names[dim][uint16(i)] = v
		}
	}
	add := func(name, ref string) {
		xf, err := strconv.ParseFloat(ref, 64)
		if err != nil {
			panic("catalog: anchor " + ref + " not a number: " + err.Error())
		}
		dec := decimalsFor(xf)
		base, err := price.Parse(ref, dec)
		if err != nil {
			panic(fmt.Sprintf("catalog: anchor %s unparseable at %d decimals: %v", ref, dec, err))
		}
		s := Symbol{Name: name, Decimals: dec, Reference: price.Format(base, dec)}
		c.symbols[name] = s
		c.order = append(c.order, s)
	}
	for _, b := range bases {
		for _, q := range quotes {
			if b == q {
				continue
			}
			name := b + q
			if _, ok := c.symbols[name]; ok {
				continue
			}
			if ref, ok := anchored[name]; ok {
				add(name, ref)
			} else {
				add(name, synthAnchor(name))
			}
		}
	}
	for i := 1; i <= synthTail; i++ {
		name := fmt.Sprintf("TKN%03d", i)
		if _, ok := c.symbols[name]; ok {
			continue
		}
		add(name, synthAnchor(name))
	}
	return c
}

// decimalsFor picks quote precision by price magnitude: >= 1000 → 2,
// >= 1 → 4, < 1 → 8. Sub-cent prices need the room; 8 decimals caps at
// ~92 billion, which price.Parse enforces rather than rounding.
func decimalsFor(x float64) uint8 {
	switch {
	case x >= 1000:
		return 2
	case x >= 1:
		return 4
	default:
		return 8
	}
}

// synthAnchor derives a deterministic log-uniform price in [0.00001, 100]
// from the symbol name. Synthetic data generation only — the value becomes
// an exact base-unit integer before it ever leaves the catalog.
func synthAnchor(name string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	u := h.Sum64()
	x := math.Exp(float64(u%10_000) / 10_000 * (math.Log(100) - math.Log(0.00001)) + math.Log(0.00001))
	dec := decimalsFor(x)
	base := int64(math.Round(x * math.Pow10(int(dec))))
	return price.Format(base, dec)
}

// Symbol looks up one pair.
func (c *Catalog) Symbol(name string) (Symbol, bool) {
	s, ok := c.symbols[name]
	return s, ok
}

// Symbols returns all pairs in deterministic (construction) order.
func (c *Catalog) Symbols() []Symbol { return c.order }

// Decimals returns the quote precision of a pair.
func (c *Catalog) Decimals(name string) (uint8, bool) {
	s, ok := c.symbols[name]
	return s.Decimals, ok
}

// Dims returns the positional dim names, matching engine.Config.Dims.
func (c *Catalog) Dims() []string { return c.dims }

// Value maps a dim value name to its engine uint16.
func (c *Catalog) Value(dim, valueName string) (uint16, bool) {
	v, ok := c.values[dim][valueName]
	return v, ok
}

// Name maps an engine uint16 back to its dim value name.
func (c *Catalog) Name(dim string, value uint16) (string, bool) {
	n, ok := c.names[dim][value]
	return n, ok
}

// DimValues returns the ordered value names of a dim.
func (c *Catalog) DimValues(dim string) []string {
	out := make([]string, 0, len(c.names[dim]))
	for i := 0; ; i++ {
		n, ok := c.names[dim][uint16(i)]
		if !ok {
			return out
		}
		out = append(out, n)
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/catalog/ -race -count=1`
Expected: PASS (all seven tests). Decimal-rule expectations to spot-check on failure: BTCUSDT (65000)→2, SOLUSDT (150)→4, XRPUSDT (0.52)→8, PEPEUSDT→8, DOGEBTC (0.00000225)→8, ETHUSDT (3400)→2. Total symbol count: 28 bases × 3 quotes minus 3 self-pairs (BTCBTC etc. — `b == q` skips exactly BTC/BTC, ETH/ETH, SOL... no: `quotes` contains BTC and ETH, so skip pairs where base == BTC and quote == BTC, and base == ETH and quote == ETH → 28×3 − 2 = 82, plus 420 tail = ~502 (a couple of anchored names may already exist — the `ok` check dedupes).

- [ ] **Step 5: Commit**

```bash
git add internal/catalog/
git commit -m "feat(catalog): symbol table with decimals/anchors and dim vocabulary"
```

---

### Task 3: `internal/stats` — counters and 60s rates

**Files:**
- Create: `internal/stats/stats.go`
- Test: `internal/stats/stats_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:

```go
type Rate struct{ ... }                 // mutex-guarded 60×1s ring, multi-writer safe
func (r *Rate) Add(n uint64, now time.Time)
func (r *Rate) PerSecond(now time.Time) float64 // mean over the last ≤60 complete seconds
type Stats struct{ ... }
func New(now time.Time) *Stats
// Atomic counters (atomic.Uint64): Ticks, TicksDropped, TriggersFired, TriggersDelivered, WatcherDrops
// Rates: TickRate, FireRate (Rate)
func (s *Stats) Snapshot(now time.Time) Snapshot
type Snapshot struct {
    UptimeSec         float64 `json:"uptime_sec"`
    Ticks             uint64  `json:"ticks"`
    TicksPerSec       float64 `json:"ticks_per_sec"`
    TicksDropped      uint64  `json:"ticks_dropped"`
    TriggersFired     uint64  `json:"triggers_fired"`
    TriggersPerSec    float64 `json:"triggers_per_sec"`
    TriggersDelivered uint64  `json:"triggers_delivered"`
    WatcherDrops      uint64  `json:"watcher_drops"`
}
```

The JSON tags matter: `/stats` output shape is pinned by Task 7's tests.

- [ ] **Step 1: Write the failing test `internal/stats/stats_test.go`**

```go
package stats

import (
	"math"
	"testing"
	"time"
)

// base builds a Rate with two completed seconds of history. now must be
// second-truncated so the "current" second stays empty.
func base(now time.Time) *Rate {
	var r Rate
	r.Add(10, now.Add(-2*time.Second))
	r.Add(15, now.Add(-2*time.Second)) // bucket t-2 holds 25
	r.Add(25, now.Add(-1*time.Second)) // bucket t-1 holds 25
	return &r
}

func TestRateBasics(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r := base(now)
	if got := r.PerSecond(now); got != 25 { // (25+25)/2 complete seconds
		t.Fatalf("PerSecond = %v, want 25", got)
	}
	r.Add(100, now.Add(-1 * time.Second)) // completes into t-1 → 150/2
	if got := r.PerSecond(now); got != 75 {
		t.Fatalf("PerSecond after add = %v, want 75", got)
	}
}

func TestRateRolloverClearsOldBuckets(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r := base(now)
	// Jump far ahead: everything old must be cleared, not summed.
	future := now.Add(5 * time.Minute)
	r.Add(7, future)
	if got := r.PerSecond(future); got > 0.001 {
		t.Fatalf("PerSecond after 5min gap = %v, want ~0 (the 7 landed in future's own, excluded second)", got)
	}
}

func TestSnapshotFields(t *testing.T) {
	now := time.Now()
	s := New(now)
	s.Ticks.Add(3)
	s.TicksDropped.Add(1)
	s.TriggersFired.Add(2)
	s.TriggersDelivered.Add(2)
	s.WatcherDrops.Add(1)
	snap := s.Snapshot(now.Add(2 * time.Second))
	if snap.Ticks != 3 || snap.TicksDropped != 1 || snap.TriggersFired != 2 ||
		snap.TriggersDelivered != 2 || snap.WatcherDrops != 1 {
		t.Fatalf("snapshot counters wrong: %+v", snap)
	}
	if math.Abs(snap.UptimeSec-2) > 0.01 {
		t.Fatalf("uptime = %v, want 2", snap.UptimeSec)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/stats/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `internal/stats/stats.go`**

```go
// Package stats holds the service-level counters backing /stats and
// /metrics: plain atomics for totals and a fixed 60-bucket ring for
// per-second rates. All methods are safe for concurrent use.
package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Rate counts events per second over a 60-second sliding window. Adds take
// a mutex — at the feed's ~20k ticks/s this is nanoseconds of work and it
// keeps rollover bookkeeping trivially correct. The ring holds 61 slots
// (index = unix second % 61): with 60 the in-progress second's bucket would
// alias the oldest second in the read window, leaking the current count
// into PerSecond.
type Rate struct {
	mu      sync.Mutex
	started int64 // unix second of first Add, 0 = never
	curSec  int64
	buckets [61]uint64 // index = unix second % 61
}

func (r *Rate) Add(n uint64, now time.Time) {
	sec := now.Unix()
	r.mu.Lock()
	r.advance(sec)
	if r.started == 0 {
		r.started = sec
	}
	r.buckets[sec%61] += n
	r.mu.Unlock()
}

// advance zeroes every bucket between curSec and sec (inclusive of sec),
// so stale counts from >60s ago are never read. Caller holds mu.
func (r *Rate) advance(sec int64) {
	switch {
	case sec == r.curSec:
		return
	case sec-r.curSec >= 61:
		for i := range r.buckets {
			r.buckets[i] = 0
		}
	default:
		for s := r.curSec + 1; s <= sec; s++ {
			r.buckets[s%61] = 0
		}
	}
	r.curSec = sec
}

// PerSecond returns the mean rate over the complete seconds in
// [started, now), clamped to the last 60. The current second is excluded
// so the value is stable across scrapes.
func (r *Rate) PerSecond(now time.Time) float64 {
	sec := now.Unix()
	r.mu.Lock()
	r.advance(sec)
	start := r.started
	if start == 0 || sec <= start {
		r.mu.Unlock()
		return 0
	}
	lo := sec - 60
	if lo < start {
		lo = start
	}
	var total uint64
	for s := lo; s < sec; s++ {
		total += r.buckets[s%61]
	}
	r.mu.Unlock()
	return float64(total) / float64(sec-lo)
}

// Stats is the service's counter set. Totals are lock-free atomics; rates
// use the Rate ring.
type Stats struct {
	started time.Time

	Ticks             atomic.Uint64
	TicksDropped      atomic.Uint64
	TriggersFired     atomic.Uint64
	TriggersDelivered atomic.Uint64
	WatcherDrops      atomic.Uint64

	TickRate Rate
	FireRate Rate
}

func New(now time.Time) *Stats { return &Stats{started: now} }

// Snapshot is the /stats JSON shape (json/v2 marshals the tags).
type Snapshot struct {
	UptimeSec         float64 `json:"uptime_sec"`
	Ticks             uint64  `json:"ticks"`
	TicksPerSec       float64 `json:"ticks_per_sec"`
	TicksDropped      uint64  `json:"ticks_dropped"`
	TriggersFired     uint64  `json:"triggers_fired"`
	TriggersPerSec    float64 `json:"triggers_per_sec"`
	TriggersDelivered uint64  `json:"triggers_delivered"`
	WatcherDrops      uint64  `json:"watcher_drops"`
}

func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		UptimeSec:         now.Sub(s.started).Seconds(),
		Ticks:             s.Ticks.Load(),
		TicksPerSec:       s.TickRate.PerSecond(now),
		TicksDropped:      s.TicksDropped.Load(),
		TriggersFired:     s.TriggersFired.Load(),
		TriggersPerSec:    s.FireRate.PerSecond(now),
		TriggersDelivered: s.TriggersDelivered.Load(),
		WatcherDrops:      s.WatcherDrops.Load(),
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/stats/ -race -count=1`
Expected: PASS (all four tests; `PerSecond` excludes the current, still-accumulating second, which is why `TestRateBasics` adds land in `t-2`/`t-1`).

- [ ] **Step 5: Commit**

```bash
git add internal/stats/
git commit -m "feat(stats): service counters and 60s sliding rates"
```

---

### Task 4: `internal/service` Core — alert ops, error mapping, engine wiring

**Files:**
- Create: `internal/service/core.go`
- Create: `internal/service/errors.go`
- Create: `internal/service/validate.go`
- Create: `internal/service/uuid.go`
- Create: `internal/service/testenv_test.go`
- Test: `internal/service/alerts_test.go`

**Interfaces:**
- Consumes: `engine.*` (cheat sheet), `catalog.Default()` and its methods, `stats.New`, Task 1's `chronov1` messages and server interfaces.
- Produces (Tasks 5–8 build on):

```go
type Metrics interface {
	Tick(venue, tier string)
	TickDropped()
	TickBatch(n int)
	TickLatency(d time.Duration)
	TriggerFired(symbol, venue, tier string)
	TriggerDelivered()
	WatcherDrop()
	AlertsActive(n int)
	Watchers(n int)
	FeedConnected(b bool)
}
type NoopMetrics struct{}
func (NoopMetrics) // all methods, empty bodies

type AlertState string // "active" | "triggered" | "cancelled"
type Alert struct {
	ID          engine.AlertID
	Symbol      string
	Decimals    uint8
	Venue, Tier string
	PriceType   engine.PriceType
	Direction   engine.Direction
	TargetPrice engine.Price
	State       AlertState
	CreatedAt   time.Time
}

type Core struct{ ... }
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, now time.Time) *Core
// cfg.Dims is OVERWRITTEN with cat.Dims() — the catalog owns the vocabulary.
func (c *Core) Close()
func (c *Core) UpsertAlert(ctx, *chronov1.UpsertAlertRequest) (*chronov1.UpsertAlertResponse, error)
func (c *Core) CancelAlert(ctx, *chronov1.CancelAlertRequest) (*chronov1.CancelAlertResponse, error)
func (c *Core) AlertsByState() map[string]int
func (c *Core) AlertCount() int
func (c *Core) Engine() *engine.Engine // tests and /stats only
func newAlertID() engine.AlertID        // uuid.NewV7
func alertIDString(id engine.AlertID) string
func parseAlertID(s string) (engine.AlertID, error)
```

`Core` embeds `chronov1.UnimplementedAlertServiceServer` and `chronov1.UnimplementedFeedServiceServer` (forward compatibility), so it satisfies both server interfaces once the Feed RPCs land in Task 5.

- [ ] **Step 1: Write the shared test harness `internal/service/testenv_test.go`**

```go
package service

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/stats"
)

type testEnv struct {
	core *Core
	cc   *grpc.ClientConn
	srv  *grpc.Server
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	cat := catalog.Default()
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), time.Now())
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(NewValidateInterceptor()))
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = cc.Close()
		srv.Stop()
		core.Close()
	})
	return &testEnv{core: core, cc: cc, srv: srv}
}

func (e *testEnv) alerts() chronov1.AlertServiceClient { return chronov1.NewAlertServiceClient(e.cc) }
func (e *testEnv) feed() chronov1.FeedServiceClient    { return chronov1.NewFeedServiceClient(e.cc) }
```

- [ ] **Step 2: Write the failing tests `internal/service/alerts_test.go`**

```go
package service

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
)

func validUpsert() *chronov1.UpsertAlertRequest {
	return &chronov1.UpsertAlertRequest{
		Symbol:      "BTCUSDT",
		PriceType:   chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00",
		Venue:       "ATLAS",
		Tier:        "TOP",
	}
}

func TestUpsertGeneratesUUID(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	id := resp.GetAlertId()
	if len(id) != 36 || !strings.Contains(id, "-") {
		t.Fatalf("alert id %q not UUID string form", id)
	}
	// 0x70 nibble: UUIDv7 version marker.
	if id[14] != '7' {
		t.Fatalf("alert id %q not v7", id)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("alert count = %d, want 1", got)
	}
}

func TestUpsertVisibleImmediately(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live = %d, want 1 (Sync before return)", got)
	}
}

func TestUpsertValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name string
		mut  func(*chronov1.UpsertAlertRequest)
		want codes.Code
	}{
		{"unknown symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "NOSUCH" }, codes.InvalidArgument},
		{"no price type", func(r *chronov1.UpsertAlertRequest) { r.PriceType = chronov1.PriceType_PRICE_TYPE_UNSPECIFIED }, codes.InvalidArgument},
		{"no direction", func(r *chronov1.UpsertAlertRequest) { r.Direction = chronov1.Direction_DIRECTION_UNSPECIFIED }, codes.InvalidArgument},
		{"precision loss", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "65000.005" }, codes.InvalidArgument},
		{"bad number", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "abc" }, codes.InvalidArgument},
		{"overflow", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "99999999999999.00" }, codes.InvalidArgument},
		{"unknown venue", func(r *chronov1.UpsertAlertRequest) { r.Venue = "BINANCE" }, codes.InvalidArgument},
		{"unknown tier", func(r *chronov1.UpsertAlertRequest) { r.Tier = "DEEP" }, codes.InvalidArgument},
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
			if got := e.core.AlertCount(); got != 0 {
				t.Fatalf("rejected upsert left %d alerts behind", got)
			}
		})
	}
}

func TestUpsertReplaceCancelsOld(t *testing.T) {
	e := newEnv(t)
	first, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	req := validUpsert()
	req.TargetPrice = "64000.00"
	req.AlertId = mustBytes(t, first.GetAlertId())
	if _, err := e.alerts().UpsertAlert(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("count after replace = %d, want 1", got)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live after replace = %d, want 1", got)
	}
}

func TestCancel(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); err != nil {
		t.Fatalf("CancelAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 0 {
		t.Fatalf("engine live after cancel = %d, want 0", got)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("double cancel code = %v, want NotFound", status.Code(err))
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad uuid code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestAlertsByState(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertsByState()["active"]; got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

func mustBytes(t *testing.T, s string) []byte {
	t.Helper()
	id, err := parseAlertID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id[:]
}
```

Note: the "overflow" case uses `"99999999999999.00"` — at 2 decimals that is 9.9999…e12 base units, which FITS int64. Real overflow at BTC's 2 decimals needs 93 billion on the integer side (`"93000000000000.00"` → ErrOverflow since int64 max/100 = 92,023,372,036,854). Use `"93000000000000.00"` in the test. (Both the request mutator and expectation stay the same.)

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/service/`
Expected: FAIL — package does not exist.

- [ ] **Step 4: Write `internal/service/errors.go`**

```go
package service

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/emir/chrono-tree/engine"
)

// invalidf builds an InvalidArgument status — the catch-all for boundary
// validation failures.
func invalidf(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// mapEngineErr maps control-plane engine errors to gRPC statuses. One
// place, so handlers stay one-liners.
func mapEngineErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, engine.ErrClosed):
		return status.Error(codes.Unavailable, "engine shutting down")
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, "alert not found")
	case errors.Is(err, engine.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, "alert already in a terminal state")
	case errors.Is(err, engine.ErrAlertLimit), errors.Is(err, engine.ErrSymbolLimit):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, engine.ErrDims):
		return status.Error(codes.InvalidArgument, "invalid dimension combination")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
```

- [ ] **Step 5: Write `internal/service/validate.go`**

```go
package service

import (
	"context"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NewValidateInterceptor returns a unary interceptor enforcing the proto's
// buf.validate rules (symbol pattern, price format, UUID shapes) before
// service-level catalog validation runs.
func NewValidateInterceptor() grpc.UnaryServerInterceptor {
	v, err := protovalidate.New()
	if err != nil {
		panic("protovalidate: " + err.Error())
	}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(proto.Message); ok {
			if err := v.Validate(msg); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "validation: %v", err)
			}
		}
		return handler(ctx, req)
	}
}
```

(Streaming RPCs bypass unary interceptors; tick validation happens in `ingestTick` — Task 5.)

- [ ] **Step 6: Write `internal/service/uuid.go`**

```go
package service

import (
	"fmt"

	"uuid"

	"github.com/emir/chrono-tree/engine"
)

// newAlertID mints a UUIDv7 — time-ordered, matching the engine's
// AlertID tie-break expectations.
func newAlertID() engine.AlertID {
	u := uuid.NewV7()
	return engine.AlertID(u)
}

func alertIDString(id engine.AlertID) string {
	u := uuid.UUID(id)
	return u.String()
}

func parseAlertID(s string) (engine.AlertID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return engine.AlertID{}, fmt.Errorf("alert id %q: %w", s, err)
	}
	return engine.AlertID(u), nil
}
```

(`uuid.UUID` is `[16]byte` in the new stdlib package — the conversions to/from `engine.AlertID` are direct.)

- [ ] **Step 7: Write `internal/service/core.go`**

```go
// Package service implements the chrono.v1 gRPC services on top of the
// engine: reference-data validation, price conversion at the boundary,
// a service-side alert catalog, and (Task 6) trigger fan-out.
package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/stats"
	"github.com/emir/chrono-tree/price"
)

// Metrics is the observability hook; implementations must be safe for
// concurrent use. NoopMetrics covers tests.
type Metrics interface {
	Tick(venue, tier string)
	TickDropped()
	TickBatch(n int)
	TickLatency(d time.Duration)
	TriggerFired(symbol, venue, tier string)
	TriggerDelivered()
	WatcherDrop()
	AlertsActive(n int)
	Watchers(n int)
	FeedConnected(b bool)
}

// NoopMetrics discards everything.
type NoopMetrics struct{}

func (NoopMetrics) Tick(string, string)      {}
func (NoopMetrics) TickDropped()             {}
func (NoopMetrics) TickBatch(int)            {}
func (NoopMetrics) TickLatency(time.Duration) {}
func (NoopMetrics) TriggerFired(string, string, string) {}
func (NoopMetrics) TriggerDelivered()        {}
func (NoopMetrics) WatcherDrop()             {}
func (NoopMetrics) AlertsActive(int)         {}
func (NoopMetrics) Watchers(int)             {}
func (NoopMetrics) FeedConnected(bool)       {}

// AlertState is the service-side lifecycle view.
type AlertState string

const (
	StateActive    AlertState = "active"
	StateTriggered AlertState = "triggered"
	StateCancelled AlertState = "cancelled"
)

// Alert is the service-side catalog entry: everything needed to enrich a
// raw engine Trigger into a chrono.v1.Trigger, kept AFTER the engine drops
// its own refs (fire removes them) — that removal race is why this map
// exists.
type Alert struct {
	ID          engine.AlertID
	Symbol      string
	Decimals    uint8
	Venue, Tier string
	PriceType   engine.PriceType
	Direction   engine.Direction
	TargetPrice engine.Price
	State       AlertState
	CreatedAt   time.Time
}

// Core implements both chrono.v1 services.
type Core struct {
	chronov1.UnimplementedAlertServiceServer
	chronov1.UnimplementedFeedServiceServer

	Cat     *catalog.Catalog
	eng     *engine.Engine
	metrics Metrics
	stats   *stats.Stats
	now     func() time.Time

	mu     sync.RWMutex
	alerts map[engine.AlertID]*Alert
}

// NewCore builds the engine with the catalog's dim vocabulary.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, now time.Time) *Core {
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	return &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		metrics: m,
		stats:   st,
		now:     func() time.Time { return time.Now() },
		alerts:  make(map[engine.AlertID]*Alert),
	}
}

// Close shuts the engine down. Call only after all servers have stopped
// (the engine contract requires submitters to stop first).
func (c *Core) Close() { c.eng.Close() }

// Engine exposes the engine for tests and /stats.
func (c *Core) Engine() *engine.Engine { return c.eng }

func (c *Core) AlertCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.alerts)
}

func (c *Core) AlertsByState() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[string]int{}
	for _, a := range c.alerts {
		out[string(a.State)]++
	}
	return out
}

func toEnginePriceType(pt chronov1.PriceType) (engine.PriceType, bool) {
	switch pt {
	case chronov1.PriceType_PRICE_TYPE_BID:
		return engine.PriceBid, true
	case chronov1.PriceType_PRICE_TYPE_ASK:
		return engine.PriceAsk, true
	case chronov1.PriceType_PRICE_TYPE_MID:
		return engine.PriceMid, true
	case chronov1.PriceType_PRICE_TYPE_LAST:
		return engine.PriceLast, true
	default:
		return 0, false
	}
}

func toEngineDirection(d chronov1.Direction) (engine.Direction, bool) {
	switch d {
	case chronov1.Direction_DIRECTION_ABOVE:
		return engine.DirGTE, true
	case chronov1.Direction_DIRECTION_BELOW:
		return engine.DirLTE, true
	default:
		return 0, false
	}
}

// UpsertAlert validates against the catalog, converts the price exactly,
// records the alert service-side BEFORE engine.Upsert (so the trigger pump
// can always enrich), then submits and Syncs — the alert is visible to
// Match when the response returns.
func (c *Core) UpsertAlert(ctx context.Context, req *chronov1.UpsertAlertRequest) (*chronov1.UpsertAlertResponse, error) {
	sym, ok := c.Cat.Symbol(req.GetSymbol())
	if !ok {
		return nil, invalidf("unknown symbol %q", req.GetSymbol())
	}
	pt, ok := toEnginePriceType(req.GetPriceType())
	if !ok {
		return nil, invalidf("price_type is required")
	}
	dir, ok := toEngineDirection(req.GetDirection())
	if !ok {
		return nil, invalidf("direction is required")
	}
	base, err := price.Parse(req.GetTargetPrice(), sym.Decimals)
	if err != nil {
		return nil, invalidf("target price %q: %v (symbol quotes %d decimals)", req.GetTargetPrice(), err, sym.Decimals)
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, req.GetVenue())
	if !ok {
		return nil, invalidf("unknown venue %q (valid: %v)", req.GetVenue(), c.Cat.DimValues(catalog.DimVenue))
	}
	tv, ok := c.Cat.Value(catalog.DimTier, req.GetTier())
	if !ok {
		return nil, invalidf("unknown tier %q (valid: %v)", req.GetTier(), c.Cat.DimValues(catalog.DimTier))
	}
	if req.GetExpiresUnixNanos() != 0 && req.GetExpiresUnixNanos() <= req.GetValidFromUnixNanos() {
		return nil, invalidf("expires at or before valid_from")
	}

	var id engine.AlertID
	if raw := req.GetAlertId(); len(raw) > 0 {
		if len(raw) != 16 {
			return nil, invalidf("alert_id must be 16 bytes, got %d", len(raw))
		}
		copy(id[:], raw)
	} else {
		id = newAlertID()
	}

	// Service catalog first (pump enrichment must never miss), remembering
	// the previous entry so an engine failure restores it.
	entry := &Alert{
		ID: id, Symbol: sym.Name, Decimals: sym.Decimals,
		Venue: req.GetVenue(), Tier: req.GetTier(),
		PriceType: pt, Direction: dir, TargetPrice: engine.Price(base),
		State: StateActive, CreatedAt: c.now(),
	}
	c.mu.Lock()
	prev := c.alerts[id]
	c.alerts[id] = entry
	c.mu.Unlock()

	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice: engine.Price(base),
		ValidFrom:   req.GetValidFromUnixNanos(),
		Expires:     req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:        engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
			TargetPrice: engine.Price(base), CreatedAt: entry.CreatedAt.UnixNano(),
		},
	}
	if err := c.eng.Upsert(spec); err != nil {
		c.mu.Lock()
		if prev != nil {
			c.alerts[id] = prev
		} else {
			delete(c.alerts, id)
		}
		c.mu.Unlock()
		return nil, mapEngineErr(err)
	}
	c.eng.Sync()
	c.metrics.AlertsActive(c.countState(StateActive))
	return &chronov1.UpsertAlertResponse{AlertId: alertIDString(id)}, nil
}

func (c *Core) countState(s AlertState) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := 0
	for _, a := range c.alerts {
		if a.State == s {
			n++
		}
	}
	return n
}

// CancelAlert retires an alert. Terminal alerts answer NotFound (the
// engine has already dropped its refs).
func (c *Core) CancelAlert(ctx context.Context, req *chronov1.CancelAlertRequest) (*chronov1.CancelAlertResponse, error) {
	id, err := parseAlertID(req.GetAlertId())
	if err != nil {
		return nil, invalidf("%v", err)
	}
	if err := c.eng.Cancel(id); err != nil {
		return nil, mapEngineErr(err)
	}
	c.mu.Lock()
	if a, ok := c.alerts[id]; ok {
		a.State = StateCancelled
	}
	c.mu.Unlock()
	c.metrics.AlertsActive(c.countState(StateActive))
	return &chronov1.CancelAlertResponse{}, nil
}
```

- [ ] **Step 8: Run tests**

Run: `go test ./internal/service/ -race -count=1`
Expected: PASS. The `TestUpsertValidation` "expires before valid" case: `ValidFrom=200, Expires=100` — `ValidFrom=200` is in the far past, which is fine (the validity gate only skips ticks BEFORE validFrom). Also note the protovalidate interceptor runs ahead of the handler, so pattern failures (e.g. lowercase symbol) surface as `InvalidArgument` from the interceptor, not the service — either way the tests only assert the code.

- [ ] **Step 9: Commit**

```bash
git add internal/service/
git commit -m "feat(service): Core with catalog-validated alert upsert/cancel"
```

---

### Task 5: `StreamTicks` + `GetCatalog` — feed ingestion

**Files:**
- Modify: `internal/service/core.go` (add ingest + two RPCs + feed-liveness fields)
- Test: `internal/service/feed_test.go`

**Interfaces:**
- Consumes: Task 4's `Core`, `Metrics.Tick/TickDropped/TickBatch/TickLatency/FeedConnected`, `stats.Ticks/TickRate/TicksDropped`.
- Produces:

```go
func (c *Core) StreamTicks(ss chronov1.FeedService_StreamTicksServer) error // returns FeedStatus on client EOF
func (c *Core) GetCatalog(ctx, *chronov1.CatalogRequest) (*chronov1.CatalogReply, error)
func (c *Core) FeedLastSeen() time.Time  // zero before first tick
func (c *Core) FeedEverConnected() bool
// readiness (Task 7): feed healthy ⇔ FeedEverConnected() && time.Since(FeedLastSeen()) < 30s
var errUnknownRefdata error // sentinel: unknown symbol/venue/tier (batch-reject class)
```

- [ ] **Step 1: Write the failing tests `internal/service/feed_test.go`**

```go
package service

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
)

type tickStream struct {
	chronov1.FeedService_StreamTicksServer
	recv chan *chronov1.TickBatch
	sent chan *chronov1.FeedStatus
}

func (s *tickStream) Recv() (*chronov1.TickBatch, error) {
	b, ok := <-s.recv
	if !ok {
		return nil, io.EOF
	}
	return b, nil
}

func (s *tickStream) SendAndClose(fs *chronov1.FeedStatus) error {
	s.sent <- fs
	return nil
}

func (s *tickStream) Context() context.Context { return context.Background() }

func runTicks(t *testing.T, e *testEnv, batches ...*chronov1.TickBatch) (*chronov1.FeedStatus, error) {
	t.Helper()
	st := &tickStream{recv: make(chan *chronov1.TickBatch, len(batches)), sent: make(chan *chronov1.FeedStatus, 1)}
	for _, b := range batches {
		st.recv <- b
	}
	close(st.recv)
	err := e.core.StreamTicks(st)
	var fs *chronov1.FeedStatus
	select {
	case fs = <-st.sent:
	default:
	}
	return fs, err
}

func tick(symbol, bid, ask, venue, tier string) *chronov1.Tick {
	return &chronov1.Tick{Symbol: symbol, Bid: bid, Ask: ask, Venue: venue, Tier: tier, TsUnixNanos: time.Now().UnixNano()}
}

func TestStreamTicksAccepts(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("PEPEUSDT", "0.00001234", "0.00001240", "NOVA", "MID"),
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 2 || fs.GetDropped() != 0 {
		t.Fatalf("FeedStatus = %v", fs)
	}
	if !e.core.FeedEverConnected() {
		t.Fatal("feed not marked connected")
	}
	if e.core.FeedLastSeen().IsZero() {
		t.Fatal("feed last-seen not set")
	}
}

func TestStreamTicksDropsBadPriceOnly(t *testing.T) {
	e := newEnv(t)
	fs, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("BTCUSDT", "65000.005", "65000.10", "ATLAS", "TOP"), // precision loss
	}})
	if err != nil {
		t.Fatalf("StreamTicks: %v", err)
	}
	if fs.GetAccepted() != 1 || fs.GetDropped() != 1 {
		t.Fatalf("FeedStatus = %+v, want 1 accepted 1 dropped", fs)
	}
	if e.core.stats.TicksDropped.Load() != 1 {
		t.Fatal("TicksDropped counter not bumped")
	}
}

func TestStreamTicksRejectsUnknownRefdata(t *testing.T) {
	e := newEnv(t)
	_, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65000.10", "ATLAS", "TOP"),
		tick("NOSUCH", "1.00", "1.01", "ATLAS", "TOP"),
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	_, err = runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "1.00", "1.01", "BINANCE", "TOP"),
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown venue code = %v", status.Code(err))
	}
}

func TestIngestFiresAlert(t *testing.T) {
	e := newEnv(t)
	_, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	// Pump runs asynchronously (Task 6 drains the ring); for this test,
	// drain the engine ring directly.
	tr, ok := e.core.Engine().Triggers().Pop()
	if !ok {
		t.Fatal("alert did not fire")
	}
	if tr.Price != engine.Price(6500100) { // 65001.00 at 2 decimals
		t.Fatalf("fired price = %d, want 6500100", tr.Price)
	}
}

func TestIngestDimsScoping(t *testing.T) {
	e := newEnv(t)
	up := &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	}
	if _, err := e.alerts().UpsertAlert(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	up.Venue = "NOVA" // different dim combo: must NOT fire on ATLAS ticks
	if _, err := e.alerts().UpsertAlert(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.core.Engine().Triggers().Pop(); !ok {
		t.Fatal("ATLAS alert did not fire")
	}
	if _, ok := e.core.Engine().Triggers().Pop(); ok {
		t.Fatal("NOVA alert fired on an ATLAS tick")
	}
}

func TestGetCatalog(t *testing.T) {
	e := newEnv(t)
	reply, err := e.feed().GetCatalog(context.Background(), &chronov1.CatalogRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.GetSymbols()) < 490 {
		t.Fatalf("symbols = %d", len(reply.GetSymbols()))
	}
	if len(reply.GetDims()) != 2 || reply.GetDims()[0].GetName() != "venue" {
		t.Fatalf("dims = %v", reply.GetDims())
	}
	var btc *chronov1.SymbolInfo
	for _, s := range reply.GetSymbols() {
		if s.GetSymbol() == "BTCUSDT" {
			btc = s
		}
	}
	if btc == nil || btc.GetDecimals() != 2 || btc.GetReferencePrice() != "65000.00" {
		t.Fatalf("BTCUSDT info = %+v", btc)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/service/ -run 'Ticks|Ingest|GetCatalog'`
Expected: FAIL — StreamTicks/GetCatalog not implemented.

- [ ] **Step 3: Extend `internal/service/core.go`**

Add to the `Core` struct:

```go
	feedEver     atomic.Bool
	feedLastSeen atomic.Int64 // unix nanos
```

Add to file:

```go
// errUnknownRefdata marks the batch-reject class (spec §8): a tick naming
// a symbol/venue/tier outside the catalog means the feed is buggy — the
// whole batch is rejected loudly.
var errUnknownRefdata = errors.New("unknown reference data")

// presentBidAskMid is the Present mask for ticks carrying bid, ask and a
// derived mid.
func presentBidAskMid() uint8 {
	return uint8(1)<<uint(engine.PriceBid) | uint8(1)<<uint(engine.PriceAsk) | uint8(1)<<uint(engine.PriceMid)
}

// ingestTick converts one wire tick to an engine tick and Matches it.
// Errors: wrapped errUnknownRefdata (batch reject) or a price.Parse error
// (drop this tick only).
func (c *Core) ingestTick(t *chronov1.Tick, now time.Time) error {
	sym, ok := c.Cat.Symbol(t.GetSymbol())
	if !ok {
		return fmt.Errorf("%q: %w", t.GetSymbol(), errUnknownRefdata)
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, t.GetVenue())
	if !ok {
		return fmt.Errorf("venue %q: %w", t.GetVenue(), errUnknownRefdata)
	}
	tv, ok := c.Cat.Value(catalog.DimTier, t.GetTier())
	if !ok {
		return fmt.Errorf("tier %q: %w", t.GetTier(), errUnknownRefdata)
	}
	bid, err := price.Parse(t.GetBid(), sym.Decimals)
	if err != nil {
		return fmt.Errorf("bid %q: %v", t.GetBid(), err)
	}
	ask, err := price.Parse(t.GetAsk(), sym.Decimals)
	if err != nil {
		return fmt.Errorf("ask %q: %v", t.GetAsk(), err)
	}
	ts := t.GetTsUnixNanos()
	c.eng.Match(&engine.Tick{
		Symbol:  sym.Name,
		Bid:     engine.Price(bid),
		Ask:     engine.Price(ask),
		Mid:     engine.Price((bid + ask) / 2),
		Present: presentBidAskMid(),
		TS:      ts,
		Dims:    engine.Dims(vv, tv),
	})
	c.stats.Ticks.Add(1)
	c.stats.TickRate.Add(1, now)
	c.metrics.Tick(t.GetVenue(), t.GetTier())
	if ts > 0 {
		c.metrics.TickLatency(now.Sub(time.Unix(0, ts)))
	}
	return nil
}

// StreamTicks ingests a client-stream of tick batches until EOF, then
// reports per-stream totals.
func (c *Core) StreamTicks(ss chronov1.FeedService_StreamTicksServer) error {
	var accepted, dropped uint64
	first := true
	for {
		batch, err := ss.Recv()
		if err == io.EOF {
			return ss.SendAndClose(&chronov1.FeedStatus{Accepted: accepted, Dropped: dropped})
		}
		if err != nil {
			return err
		}
		now := c.now()
		if first {
			c.feedEver.Store(true)
			c.metrics.FeedConnected(true)
			first = false
		}
		c.feedLastSeen.Store(now.UnixNano())
		c.metrics.TickBatch(len(batch.GetTicks()))
		for _, tk := range batch.GetTicks() {
			if err := c.ingestTick(tk, now); err != nil {
				if errors.Is(err, errUnknownRefdata) {
					return status.Errorf(codes.InvalidArgument, "batch rejected: %v", err)
				}
				dropped++
				c.stats.TicksDropped.Add(1)
				c.metrics.TickDropped()
				continue
			}
			accepted++
		}
	}
}

// FeedLastSeen reports the last accepted-batch time (zero before any).
func (c *Core) FeedLastSeen() time.Time {
	ns := c.feedLastSeen.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *Core) FeedEverConnected() bool { return c.feedEver.Load() }

// GetCatalog serves the reference data.
func (c *Core) GetCatalog(ctx context.Context, _ *chronov1.CatalogRequest) (*chronov1.CatalogReply, error) {
	syms := make([]*chronov1.SymbolInfo, 0, len(c.Cat.Symbols()))
	for _, s := range c.Cat.Symbols() {
		syms = append(syms, &chronov1.SymbolInfo{Symbol: s.Name, Decimals: uint32(s.Decimals), ReferencePrice: s.Reference})
	}
	dims := make([]*chronov1.DimInfo, 0, len(c.Cat.Dims()))
	for _, d := range c.Cat.Dims() {
		dims = append(dims, &chronov1.DimInfo{Name: d, Values: c.Cat.DimValues(d)})
	}
	return &chronov1.CatalogReply{Symbols: syms, Dims: dims}, nil
}
```

Add `errors`, `fmt`, `io` to imports. Note `feedLastSeen` is also touched by `ingestTick` callers only via `StreamTicks` — no update per tick (batch granularity is the right freshness for a 30s readiness window).

- [ ] **Step 4: Run tests**

Run: `go test ./internal/service/ -race -count=1`
Expected: PASS (new + Task 4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/service/
git commit -m "feat(service): StreamTicks ingestion and GetCatalog"
```

---

### Task 6: Trigger pump + `WatchTriggers` — fan-out

**Files:**
- Modify: `internal/service/core.go` (watchHub, pump, WatchTriggers, lifecycle)
- Test: `internal/service/watch_test.go`

**Interfaces:**
- Consumes: Tasks 4–5 (`Core`, `ingestTick`, `Alert` catalog), `engine.Triggers().PopBatch`, `price.Format`.
- Produces:

```go
func (c *Core) WatchTriggers(*chronov1.WatchTriggersRequest, chronov1.AlertService_WatchTriggersServer) error
func (c *Core) WatcherCount() int
// Core.Close now also stops the pump BEFORE engine.Close (engine contract:
// submitters stop first). NewCore starts the pump goroutine.
```

- [ ] **Step 1: Write the failing tests `internal/service/watch_test.go`**

```go
package service

import (
	"context"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// watchOne opens a WatchTriggers stream, runs a tick batch, and returns the
// first trigger received or nil after the timeout.
func watchOne(t *testing.T, e *testEnv, batches ...*chronov1.TickBatch) *chronov1.Trigger {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}
	if got := e.core.WatcherCount(); got != 1 {
		t.Fatalf("watcher count = %d, want 1", got)
	}
	// Wait for the watcher to be registered server-side before firing.
	deadline := time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := runTicks(t, e, batches...); err != nil {
		t.Fatal(err)
	}
	done := make(chan *chronov1.Trigger, 1)
	go func() {
		tr, err := stream.Recv()
		if err == nil {
			done <- tr
		}
		close(done)
	}()
	select {
	case tr := <-done:
		return tr
	case <-time.After(3 * time.Second):
		return nil
	}
}

func TestWatchReceivesEnrichedTrigger(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := watchOne(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}})
	if tr == nil {
		t.Fatal("no trigger received")
	}
	if tr.GetAlertId() != resp.GetAlertId() {
		t.Fatalf("id = %q, want %q", tr.GetAlertId(), resp.GetAlertId())
	}
	if tr.GetSymbol() != "BTCUSDT" || tr.GetVenue() != "ATLAS" || tr.GetTier() != "TOP" {
		t.Fatalf("dims wrong: %v", tr)
	}
	if tr.GetFiredPrice() != "65001.00" {
		t.Fatalf("fired price = %q, want 65001.00", tr.GetFiredPrice())
	}
	if tr.GetTargetPrice() != "65000.00" {
		t.Fatalf("target price = %q", tr.GetTargetPrice())
	}
	if tr.GetDirection() != chronov1.Direction_DIRECTION_ABOVE {
		t.Fatalf("direction = %v", tr.GetDirection())
	}
	if tr.GetFiredAtUnixNanos() == 0 {
		t.Fatal("fired_at not set")
	}
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
	if e.core.stats.TriggersDelivered.Load() != 1 {
		t.Fatal("delivery not counted")
	}
}

func TestWatchCancelRemovesWatcher(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream should end on cancel")
	}
	deadline = time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.WatcherCount(); got != 0 {
		t.Fatalf("watcher count after cancel = %d, want 0", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/service/ -run 'Watch'`
Expected: FAIL — WatchTriggers is Unimplemented.

- [ ] **Step 3: Extend `internal/service/core.go`**

Add fields to `Core`:

```go
	pumpCtx    context.Context
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}

	hubMu    sync.RWMutex
	watchers map[*watcher]struct{}
```

Change `NewCore` (pump start) and `Close` (pump stop before engine), and add the hub, pump, and RPC:

```go
// watcher is one connected WatchTriggers stream. ch is buffered; a full
// channel at broadcast time disconnects the watcher (drop-and-log — the
// pump must never block, same philosophy as the engine's trigger ring).
type watcher struct {
	ch chan *chronov1.Trigger
}

func newWatcher() *watcher { return &watcher{ch: make(chan *chronov1.Trigger, 256)} }
```

In `NewCore`, after building the struct:

```go
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	c.watchers = make(map[*watcher]struct{})
	go c.pump()
	return c
```

Replace `Close`:

```go
// Close stops the pump first (it is the only Triggers() consumer, and the
// engine contract requires submitters to stop before Close), then the engine.
func (c *Core) Close() {
	c.pumpCancel()
	<-c.pumpDone
	c.eng.Close()
}
```

Add:

```go
func (c *Core) WatcherCount() int {
	c.hubMu.RLock()
	defer c.hubMu.RUnlock()
	return len(c.watchers)
}

// pump drains the engine's trigger ring and fans enriched triggers out to
// every watcher. It is the ONLY Triggers() consumer.
func (c *Core) pump() {
	defer close(c.pumpDone)
	buf := make([]engine.Trigger, 64)
	for {
		n := c.eng.Triggers().PopBatch(buf)
		if n == 0 {
			select {
			case <-c.pumpCtx.Done():
				return
			case <-time.After(500 * time.Microsecond):
			}
			continue
		}
		now := c.now()
		for i := range buf[:n] {
			c.deliver(&buf[i], now)
		}
	}
}

// deliver enriches one engine trigger from the service catalog and
// broadcasts it. The catalog entry survives the engine's own fire-time
// refs cleanup — that removal race is why the service catalog exists.
func (c *Core) deliver(tr *engine.Trigger, now time.Time) {
	c.mu.RLock()
	a := c.alerts[tr.ID]
	var state AlertState
	if a != nil {
		state = a.State
	}
	c.mu.RUnlock()
	c.stats.TriggersFired.Add(1)
	c.stats.FireRate.Add(1, now)
	if a == nil || state != StateActive {
		// Fired for an alert we no longer consider active (cancelled
		// concurrently, or a terminal replacement race). Count, don't ship.
		return
	}
	c.mu.Lock()
	a.State = StateTriggered
	c.mu.Unlock()
	out := &chronov1.Trigger{
		AlertId:          alertIDString(tr.ID),
		Symbol:           a.Symbol,
		Venue:            a.Venue,
		Tier:             a.Tier,
		FiredPrice:       price.Format(int64(tr.Price), a.Decimals),
		FiredAtUnixNanos: tr.TS,
		Direction:        fromEngineDirection(a.Direction),
		TargetPrice:      price.Format(int64(a.TargetPrice), a.Decimals),
	}
	c.metrics.TriggerFired(a.Symbol, a.Venue, a.Tier)
	c.broadcast(out)
}

// broadcast fans out to all watchers; a full watcher is disconnected, not
// blocking the pump.
func (c *Core) broadcast(tr *chronov1.Trigger) {
	c.hubMu.RLock()
	targets := make([]*watcher, 0, len(c.watchers))
	for w := range c.watchers {
		targets = append(targets, w)
	}
	c.hubMu.RUnlock()
	for _, w := range targets {
		select {
		case w.ch <- tr:
		default:
			c.hubMu.Lock()
			delete(c.watchers, w)
			n := len(c.watchers)
			c.hubMu.Unlock()
			close(w.ch)
			c.stats.WatcherDrops.Add(1)
			c.metrics.WatcherDrop()
			c.metrics.Watchers(n)
		}
	}
}

func fromEngineDirection(d engine.Direction) chronov1.Direction {
	if d == engine.DirLTE {
		return chronov1.Direction_DIRECTION_BELOW
	}
	return chronov1.Direction_DIRECTION_ABOVE
}

// WatchTriggers registers a watcher and streams until the client goes or
// the watcher is evicted for being too slow.
func (c *Core) WatchTriggers(_ *chronov1.WatchTriggersRequest, ss chronov1.AlertService_WatchTriggersServer) error {
	w := newWatcher()
	c.hubMu.Lock()
	c.watchers[w] = struct{}{}
	n := len(c.watchers)
	c.hubMu.Unlock()
	c.metrics.Watchers(n)
	defer func() {
		c.hubMu.Lock()
		if _, ok := c.watchers[w]; ok {
			delete(c.watchers, w)
			n = len(c.watchers)
		} else {
			n = -1 // already evicted by broadcast
		}
		c.hubMu.Unlock()
		if n >= 0 {
			c.metrics.Watchers(n)
		}
	}()
	for {
		select {
		case <-ss.Context().Done():
			return nil
		case tr, ok := <-w.ch:
			if !ok {
				return status.Error(codes.ResourceExhausted, "watcher too slow; disconnected")
			}
			if err := ss.Send(tr); err != nil {
				return err
			}
			c.stats.TriggersDelivered.Add(1)
			c.metrics.TriggerDelivered()
		}
	}
}
```

Add `"context"` and `"google.golang.org/grpc/status"`/`codes` imports as needed.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/service/ -race -count=1`
Expected: PASS (all tests so far). The pump's 500µs idle sleep bounds trigger delivery latency well inside the 3s test timeout.

- [ ] **Step 5: Commit**

```bash
git add internal/service/
git commit -m "feat(service): trigger pump and WatchTriggers fan-out"
```

---

### Task 7: `internal/server` — HTTP status + Prometheus metrics

**Files:**
- Create: `internal/server/server.go`
- Create: `internal/server/prom.go`
- Test: `internal/server/server_test.go`

**Interfaces:**
- Consumes: `service.Core` (Tasks 4–6: `AlertsByState`, `WatcherCount`, `FeedEverConnected`, `FeedLastSeen`, `VenueTicks`, `Engine()`), `stats.Snapshot`, `engine.Stats`.
- Produces:

```go
type Server struct{ ... }
func New(core *service.Core, st *stats.Stats, reg *prometheus.Registry, now time.Time) *Server
func (s *Server) Handler() http.Handler
func (s *Server) SetShuttingDown()
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics // implements service.Metrics
```

Spec deviation to carry into the README pass (Task 12): the spec's "/stats … engine queue depths (mutation backlog, …)" is not implementable against the frozen engine — `engine.Stats` exposes `Live` and `DroppedTriggers` only, and we do not touch `engine/`. `/stats` therefore reports `engine: {live, dropped_triggers}`.

Also: Task 5's `Core` needs a `VenueTicks() map[string]uint64` for this task — it is added HERE (small, cohesive with its only consumer):

- [ ] **Step 0: Add per-venue tick counters to Core (Task 5 file)**

In `internal/service/core.go`, add to the `Core` struct:

```go
	venueTicks map[string]*atomic.Uint64 // venue → accepted ticks; fixed keys, written at construction
```

In `NewCore`, before returning:

```go
	c.venueTicks = make(map[string]*atomic.Uint64, len(cat.DimValues(catalog.DimVenue)))
	for _, v := range cat.DimValues(catalog.DimVenue) {
		c.venueTicks[v] = &atomic.Uint64{}
	}
```

In `ingestTick`, right after the catalog venue lookup succeeds:

```go
	c.venueTicks[t.GetVenue()].Add(1)
```

And the accessor:

```go
// VenueTicks reports accepted ticks per venue.
func (c *Core) VenueTicks() map[string]uint64 {
	out := make(map[string]uint64, len(c.venueTicks))
	for v, ctr := range c.venueTicks {
		out[v] = ctr.Load()
	}
	return out
}
```

- [ ] **Step 1: Write the failing tests `internal/server/server_test.go`**

```go
package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// fakeFeedStream drives StreamTicks without a gRPC server.
type fakeFeedStream struct {
	chronov1.FeedService_StreamTicksServer
	batches []*chronov1.TickBatch
}

func (f *fakeFeedStream) Recv() (*chronov1.TickBatch, error) {
	if len(f.batches) == 0 {
		return nil, io.EOF
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b, nil
}

func (f *fakeFeedStream) SendAndClose(*chronov1.FeedStatus) error { return nil }

func newServer(t *testing.T) (*Server, *service.Core) {
	t.Helper()
	now := time.Now()
	reg := prometheus.NewRegistry()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), now)
	t.Cleanup(core.Close)
	return New(core, stats.New(now), reg), core
}

func TestHealthz(t *testing.T) {
	s, _ := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	s.SetShuttingDown()
	resp, err = http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("healthz while shutting down = %d", resp.StatusCode)
	}
}

func TestReadyzFlipsWithFeed(t *testing.T) {
	s, core := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz before feed = %d", resp.StatusCode)
	}

	fs := &fakeFeedStream{batches: []*chronov1.TickBatch{{Ticks: []*chronov1.Tick{
		{Symbol: "BTCUSDT", Bid: "65000.00", Ask: "65000.10", Venue: "ATLAS", Tier: "TOP"},
	}}}}
	if err := core.StreamTicks(fs); err != nil {
		t.Fatal(err)
	}
	resp, _ = http.Get(ts.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("readyz after feed = %d", resp.StatusCode)
	}
}

// The 30s staleness window, deterministically: inside a synctest bubble the
// fake clock jumps 31s in no wall-clock time, against the in-memory test
// server (Go 1.27 httptest.NewTestServer).
func TestReadyzStaleUnderSynctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		reg := prometheus.NewRegistry()
		core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), now)
		defer core.Close()
		s := New(core, stats.New(now), reg)
		ts := httptest.NewTestServer(s.Handler())
		defer ts.Close()

		fs := &fakeFeedStream{batches: []*chronov1.TickBatch{{Ticks: []*chronov1.Tick{{
			Symbol: "BTCUSDT", Bid: "65000.00", Ask: "65000.10", Venue: "ATLAS", Tier: "TOP",
		}}}}}
		if err := core.StreamTicks(fs); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(31 * time.Second)
		resp, err := http.Get(ts.URL + "/readyz")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("readyz after 31s silence = %d, want 503", resp.StatusCode)
		}
	})
}

func TestStatsJSON(t *testing.T) {
	s, _ := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content type = %q", ct)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"uptime_sec", "alerts_by_state", "watchers", "ticks", "ticks_per_sec",
		"triggers_fired", "venue_ticks", "engine"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("stats missing %q: %v", key, body)
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	now := time.Now()
	reg := prometheus.NewRegistry()
	pm := NewPromMetrics(reg)
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, stats.New(now), now)
	t.Cleanup(core.Close)
	s := New(core, stats.New(now), reg)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	pm.Tick("ATLAS", "TOP")
	pm.TriggerFired("BTCUSDT", "ATLAS", "TOP")
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	text := string(b)
	for _, name := range []string{
		"chrono_ticks_total", "chrono_triggers_fired_total", "chrono_triggers_delivered_total",
		"chrono_trigger_drops_total", "chrono_ticks_dropped_total", "chrono_alerts_active",
		"chrono_watchers", "chrono_feed_connected", "chrono_tick_batch_size", "chrono_tick_latency_seconds",
	} {
		if !strings.Contains(text, name) {
			t.Fatalf("metrics missing %s", name)
		}
	}
	if !strings.Contains(text, `chrono_ticks_total{tier="TOP",venue="ATLAS"}`) {
		t.Fatalf("tick labels wrong:\n%s", text)
	}
}
```

(`metadata` import in the test file list is unneeded — dropped above; the final import set for `server_test.go` is: `context`, `encoding/json`, `io`, `net/http`, `net/http/httptest`, `strings`, `testing`, `testing/synctest`, `time`, `prometheus`, `chronov1`, `engine`, `catalog`, `service`, `stats`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/server/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `internal/server/prom.go`**

```go
package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/emir/chrono-tree/internal/service"
)

// PromMetrics implements service.Metrics on a Prometheus registry.
type PromMetrics struct {
	ticks        *prometheus.CounterVec
	fired        *prometheus.CounterVec
	delivered    prometheus.Counter
	triggerDrops prometheus.Counter
	ticksDropped prometheus.Counter
	batch        prometheus.Histogram
	latency      prometheus.Histogram
	active       prometheus.Gauge
	watchers     prometheus.Gauge
	feed         prometheus.Gauge
}

// NewPromMetrics registers and returns the metric set. The names are
// pinned by the spec.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		ticks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_ticks_total", Help: "Ticks accepted by the engine, by venue and tier.",
		}, []string{"venue", "tier"}),
		fired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_triggers_fired_total", Help: "Alerts fired by the engine, by symbol/venue/tier.",
		}, []string{"symbol", "venue", "tier"}),
		delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_triggers_delivered_total", Help: "Triggers delivered to watchers.",
		}),
		triggerDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_trigger_drops_total", Help: "Watchers evicted for falling behind.",
		}),
		ticksDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_ticks_dropped_total", Help: "Ticks dropped for unrepresentable prices.",
		}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_batch_size", Help: "Ticks per StreamTicks batch.",
			Buckets: []float64{1, 4, 8, 16, 32, 48, 64, 96, 128},
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_latency_seconds", Help: "Tick timestamp-to-ingest latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12), // 100µs .. ~400ms
		}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_alerts_active", Help: "Alerts currently active service-side.",
		}),
		watchers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_watchers", Help: "Connected WatchTriggers streams.",
		}),
		feed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_feed_connected", Help: "1 once a feed has ever connected.",
		}),
	}
	reg.MustRegister(m.ticks, m.fired, m.delivered, m.triggerDrops, m.ticksDropped,
		m.batch, m.latency, m.active, m.watchers, m.feed)
	return m
}

func (m *PromMetrics) Tick(venue, tier string) { m.ticks.WithLabelValues(venue, tier).Inc() }
func (m *PromMetrics) TickDropped()            { m.ticksDropped.Inc() }
func (m *PromMetrics) TickBatch(n int)         { m.batch.Observe(float64(n)) }
func (m *PromMetrics) TickLatency(d time.Duration) { m.latency.Observe(d.Seconds()) }
func (m *PromMetrics) TriggerFired(symbol, venue, tier string) {
	m.fired.WithLabelValues(symbol, venue, tier).Inc()
}
func (m *PromMetrics) TriggerDelivered() { m.delivered.Inc() }
func (m *PromMetrics) WatcherDrop()      { m.triggerDrops.Inc() }
func (m *PromMetrics) AlertsActive(n int) { m.active.Set(float64(n)) }
func (m *PromMetrics) Watchers(n int)     { m.watchers.Set(float64(n)) }
func (m *PromMetrics) FeedConnected(b bool) {
	if b {
		m.feed.Set(1)
	}
}
```

Compile-time check at the bottom of the file:

```go
var _ service.Metrics = (*PromMetrics)(nil)
```

- [ ] **Step 4: Write `internal/server/server.go`**

```go
// Package server is chronod's HTTP status surface: liveness, readiness,
// a json/v2 stats snapshot, Prometheus metrics and pprof.
package server

import (
	"encoding/json/v2"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// feedStaleAfter is how long the feed may be silent before readyz flips.
const feedStaleAfter = 30 * time.Second

type Server struct {
	core        *service.Core
	stats       *stats.Stats
	reg         *prometheus.Registry
	shutting    atomic.Bool
}

func New(core *service.Core, st *stats.Stats, reg *prometheus.Registry) *Server {
	return &Server{core: core, stats: st, reg: reg}
}

// SetShuttingDown flips /healthz and /readyz to 503.
func (s *Server) SetShuttingDown() { s.shutting.Store(true) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s.shutting.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// ready: not shutting down, and a feed has connected within the stale
// window. Engine and catalogs are construction-order facts (NewCore
// panics or succeeds before the server ever listens).
func (s *Server) ready() bool {
	if s.shutting.Load() || !s.core.FeedEverConnected() {
		return false
	}
	last := s.core.FeedLastSeen()
	if last.IsZero() || time.Since(last) > feedStaleAfter {
		return false
	}
	return true
}

// statusView is the /stats JSON document.
type statusView struct {
	UptimeSec         float64           `json:"uptime_sec"`
	AlertsByState     map[string]int    `json:"alerts_by_state"`
	Watchers          int               `json:"watchers"`
	FeedEverConnected bool              `json:"feed_ever_connected"`
	FeedLastSeenMsAgo int64             `json:"feed_last_seen_ms_ago"` // -1 = never
	VenueTicks        map[string]uint64 `json:"venue_ticks"`
	Ticks             uint64            `json:"ticks"`
	TicksPerSec       float64           `json:"ticks_per_sec"`
	TicksDropped      uint64            `json:"ticks_dropped"`
	TriggersFired     uint64            `json:"triggers_fired"`
	TriggersPerSec    float64           `json:"triggers_per_sec"`
	TriggersDelivered uint64            `json:"triggers_delivered"`
	WatcherDrops      uint64            `json:"watcher_drops"`
	Engine            engineView        `json:"engine"`
}

type engineView struct {
	Live            uint64 `json:"live"`
	DroppedTriggers uint64 `json:"dropped_triggers"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	snap := s.stats.Snapshot(now)
	es := s.core.Engine().Stats()
	view := statusView{
		UptimeSec:         snap.UptimeSec,
		AlertsByState:     s.core.AlertsByState(),
		Watchers:          s.core.WatcherCount(),
		FeedEverConnected: s.core.FeedEverConnected(),
		FeedLastSeenMsAgo: -1,
		VenueTicks:        s.core.VenueTicks(),
		Ticks:             snap.Ticks,
		TicksPerSec:       snap.TicksPerSec,
		TicksDropped:      snap.TicksDropped,
		TriggersFired:     snap.TriggersFired,
		TriggersPerSec:    snap.TriggersPerSec,
		TriggersDelivered: snap.TriggersDelivered,
		WatcherDrops:      snap.WatcherDrops,
		Engine:            engineView{Live: es.Live, DroppedTriggers: es.DroppedTriggers},
	}
	if last := s.core.FeedLastSeen(); !last.IsZero() {
		view.FeedLastSeenMsAgo = time.Since(last).Milliseconds()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, view); err != nil {
		// Headers are sent; nothing to do but log-shape the error.
		_ = err
	}
}
```

Note: `New` takes `now` for symmetry with `stats.New` but does not use it — drop the parameter entirely (`New(core, st, reg)`) and update the test's `New(core, stats.New(now), reg, now)` calls to `New(core, stats.New(now), reg)`. Also `ready(r *http.Request)` doesn't use `r` — make it `func (s *Server) ready() bool` and call `s.ready()`. In `handleStats`, `now` is used by `Snapshot` — keep. The json/v2 import is `encoding/json/v2` (its `MarshalWrite` returns only `error`).

- [ ] **Step 5: Run tests**

Run: `go test ./internal/server/ -race -count=1 && go build ./... && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/server/ internal/service/
git commit -m "feat(server): HTTP status endpoints and Prometheus metrics"
```

---

### Task 8: `cmd/chronod` — wiring, keepalive, graceful shutdown

**Files:**
- Create: `cmd/chronod/main.go`
- Test: `cmd/chronod/main_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: the `chronod` binary. Entry: `main()` → `run(ctx)`. Testable seam:

```go
func run(ctx context.Context, grpcAddr, httpAddr string) error // blocks until ctx is done
```

- [ ] **Step 1: Write the failing test `cmd/chronod/main_test.go`**

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRunServesHTTP(t *testing.T) {
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, grpcAddr, httpAddr) }()

	deadline := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ok = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("healthz never became ready")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not shut down within 15s")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
```

(`net` imported; both `run` listeners come from `freeAddr`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronod/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `cmd/chronod/main.go`**

```go
// Command chronod hosts the chrono-tree engine: the chrono.v1 gRPC
// services on one listener, HTTP status on another.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/server"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

// shutdownGrace bounds GracefulStop; connections that outlive it are cut.
const shutdownGrace = 10 * time.Second

func main() {
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":8080", "HTTP status listen address")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *grpcAddr, *httpAddr); err != nil {
		slog.Error("chronod exit", "err", err)
		os.Exit(1)
	}
	slog.Info("chronod stopped")
}

func run(ctx context.Context, grpcAddr, httpAddr string) error {
	now := time.Now()
	reg := prometheus.NewRegistry()
	pm := server.NewPromMetrics(reg)
	st := stats.New(now)
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, now)
	defer core.Close()

	statusSrv := server.New(core, st, reg)
	httpServer := &http.Server{Addr: httpAddr, Handler: statusSrv.Handler()}
	httpLn, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpServer.Serve(httpLn) }()
	slog.Info("http status listening", "addr", httpAddr)

	kaep := keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     time.Minute,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
		Time:                  30 * time.Second,
		Timeout:               5 * time.Second,
	}
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(service.NewValidateInterceptor()),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	chronov1.RegisterAlertServiceServer(gs, core)
	chronov1.RegisterFeedServiceServer(gs, core)
	reflection.Register(gs)
	grpcLn, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- gs.Serve(grpcLn) }()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	slog.Info("grpc listening", "addr", grpcAddr)

	<-ctx.Done()
	slog.Info("shutting down")
	statusSrv.SetShuttingDown()

	// Bound GracefulStop: watchers get their in-flight triggers, then we
	// stop accepting; stuck connections are cut at shutdownGrace.
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		gs.Stop()
	}

	core.Close() // stops the pump, then the engine (idempotent; the defer is a backstop)
	shCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = httpServer.Shutdown(shCtx)

	if err := <-httpErr; err != nil && err != http.ErrServerClosed {
		return err
	}
	if err := <-grpcErr; err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronod/ -race -count=1 -timeout 60s`
Expected: PASS, goleak clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronod/
git commit -m "feat(chronod): engine host with gRPC and HTTP status"
```

---

### Task 9: `internal/feed` — market simulator

**Files:**
- Create: `internal/feed/feed.go`
- Test: `internal/feed/feed_test.go`

**Interfaces:**
- Consumes: `catalog.Symbol`/`Venues`/`Tiers`, `price.Format`.
- Produces:

```go
type TickMsg struct {
	Symbol        string
	Bid, Ask      string // decimal strings at the symbol's decimals
	Venue, Tier   string
	TS            time.Time // zero = "caller stamps" (chronofeed stamps at send)
}
type Market struct{ ... }
func NewMarket(syms []catalog.Symbol, venues, tiers []string, seed uint64) *Market
func (m *Market) Tick(ts time.Time) TickMsg // thread-safe; picks pair by heat, walks, rotates venue/tier
type Emitter struct{ ... }
func NewEmitter(targetPerSec float64) *Emitter
func (e *Emitter) Take(interval time.Duration) int // ticks to emit this interval (rate shaping)
```

Simulation model: each tick advances ONE pair (chosen by heat-weighted roulette) with a geometric random walk in float64 over its integer base-unit mid; the walk is synthetic-data generation, so float64 is acceptable HERE and only here — the value becomes an exact integer and is emitted via `price.Format`. Volatility by quote precision: 2-decimal majors σ=0.05%/tick, 4-decimal σ=0.1%, 8-decimal alt/memecoins σ=0.3%. Spread by tier: TOP half-spread 0.005%, MID 0.05%. Heat: picked pair's weight is boosted (+0.1) and all heat decays ×0.98 — bursty, like real feeds. Venue and tier rotate positionally so all combos interleave.

- [ ] **Step 1: Write the failing tests `internal/feed/feed_test.go`**

```go
package feed

import (
	"testing"
	"time"

	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

var testSyms = []catalog.Symbol{
	{Name: "BTCUSDT", Decimals: 2, Reference: "65000.00"},
	{Name: "SOLUSDT", Decimals: 4, Reference: "150.0000"},
	{Name: "PEPEUSDT", Decimals: 8, Reference: "0.00001234"},
}

func TestMarketDeterministic(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0)
	a := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 42)
	b := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 42)
	for i := 0; i < 1000; i++ {
		ta, tb := a.Tick(ts), b.Tick(ts)
		if ta != tb {
			t.Fatalf("divergence at tick %d: %v vs %v", i, ta, tb)
		}
	}
}

func TestTicksWellFormed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 7)
	seen := map[string]bool{}
	for i := 0; i < 5000; i++ {
		msg := m.Tick(now)
		seen[msg.Venue+"/"+msg.Tier] = true

		sym := catalog.Symbol{}
		for _, s := range testSyms {
			if s.Name == msg.Symbol {
				sym = s
			}
		}
		bid, err := price.Parse(msg.Bid, sym.Decimals)
		if err != nil {
			t.Fatalf("bid %q: %v", msg.Bid, err)
		}
		ask, err := price.Parse(msg.Ask, sym.Decimals)
		if err != nil {
			t.Fatalf("ask %q: %v", msg.Ask, err)
		}
		if bid <= 0 || ask <= bid {
			t.Fatalf("bad quote: bid=%d ask=%d (%v)", bid, ask, msg)
		}
		if got := price.Format(bid, sym.Decimals); got != msg.Bid {
			t.Fatalf("bid %q does not round-trip: %q", msg.Bid, got)
		}
		// Sanity: prices stay within two orders of magnitude of the anchor.
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		if bid > ref*100 || bid < ref/100 {
			t.Fatalf("%s drifted: bid=%d ref=%d", msg.Symbol, bid, ref)
		}
	}
	// All six dim combos must interleave over 5000 ticks.
	for _, v := range catalog.Venues {
		for _, ti := range catalog.Tiers {
			if !seen[v+"/"+ti] {
				t.Fatalf("combo %s/%s never emitted", v, ti)
			}
		}
	}
}

func TestEmitterRate(t *testing.T) {
	e := NewEmitter(100) // 100/s
	interval := 10 * time.Millisecond
	total := 0
	for i := 0; i < 100; i++ { // 1 simulated second
		total += e.Take(interval)
	}
	if total < 95 || total > 105 {
		t.Fatalf("emitted %d over 1s at rate 100, want ~100", total)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/feed/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `internal/feed/feed.go`**

```go
// Package feed is the synthetic crypto market simulator: a seeded
// geometric random walk over the catalog's symbols with venue/tier
// rotation, tiered spreads, and bursty per-symbol heat. The walk happens
// in float64 over integer base units — synthetic data generation only;
// every emitted price is an exact integer rendered by price.Format, and
// float never crosses into the engine.
package feed

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

// TickMsg is one simulated tick, ready for the wire.
type TickMsg struct {
	Symbol      string
	Bid, Ask    string // decimal strings at the symbol's decimals
	Venue, Tier string
	TS          time.Time
}

type pair struct {
	sym   catalog.Symbol
	base  int64 // current mid in base units
	sigma float64
	heat  float64 // burst weight multiplier, ≥0
}

// Market is a thread-safe tick source.
type Market struct {
	mu     sync.Mutex
	rng    *rand.Rand
	pairs  []pair
	venues []string
	tiers  []string
	vi, ti int // rotation cursors
}

// sigmaFor maps quote precision to per-tick volatility: majors are calm,
// memecoins are not.
func sigmaFor(dec uint8) float64 {
	switch dec {
	case 2:
		return 0.0005
	case 4:
		return 0.001
	default:
		return 0.003
	}
}

// halfSpreadFrac: TOP quotes top-of-book (0.5bp), MID quotes a deeper
// level (5bp).
func halfSpreadFrac(tier string) float64 {
	if tier == catalog.Tiers[0] { // TOP
		return 0.00005
	}
	return 0.0005
}

func NewMarket(syms []catalog.Symbol, venues, tiers []string, seed uint64) *Market {
	m := &Market{
		rng:    rand.New(rand.NewChaCha8(*SeedBytes(seed))),
		venues: venues,
		tiers:  tiers,
	}
	for _, s := range syms {
		base, err := price.Parse(s.Reference, s.Decimals)
		if err != nil {
			panic("feed: reference unparseable: " + err.Error())
		}
		m.pairs = append(m.pairs, pair{sym: s, base: base, sigma: sigmaFor(s.Decimals)})
	}
	return m
}

// SeedBytes spreads a uint64 seed into a 32-byte ChaCha8 key. Shared with
// cmd/chronoctl so demo and feed derive identical RNG streams from a flag.
func SeedBytes(seed uint64) *[32]byte {
	var b [32]byte
	for i := 0; i < 32; i++ {
		b[i] = byte(seed >> (uint(i%8) * 8))
	}
	seed = seed*6364136223846793005 + 1442695040888963407 // cheap avalanche
	for i := 0; i < 32; i++ {
		b[i] ^= byte(seed >> (uint(i%8) * 8))
	}
	return &b
}

// Tick advances one heat-weighted pair and returns a wire-ready quote.
// TS is stamped by the caller (chronofeed sets send time); pass zero for
// "stamp later".
func (m *Market) Tick(ts time.Time) TickMsg {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Heat-weighted roulette: hotter symbols tick more often.
	total := 0.0
	for i := range m.pairs {
		total += 1 + m.pairs[i].heat
	}
	roll := m.rng.Float64() * total
	idx := 0
	for i := range m.pairs {
		roll -= 1 + m.pairs[i].heat
		if roll <= 0 {
			idx = i
			break
		}
	}
	p := &m.pairs[idx]
	factor := math.Exp(p.sigma * m.rng.NormFloat64())
	nb := int64(math.Round(float64(p.base) * factor))
	if nb < 1 {
		nb = 1
	}
	p.base = nb

	// Heat dynamics: boost the picked pair, decay everyone.
	p.heat += 0.1
	for i := range m.pairs {
		m.pairs[i].heat *= 0.98
	}

	// Rotate venue and tier positionally so all combos interleave.
	venue := m.venues[m.vi%len(m.venues)]
	m.vi++
	tier := m.tiers[m.ti%len(m.tiers)]
	m.ti++

	half := int64(math.Round(float64(p.base) * halfSpreadFrac(tier)))
	if half < 1 {
		half = 1 // sub-cent symbols: a fraction-of-a-base-unit spread rounds to zero
	}
	if half >= p.base {
		half = p.base / 2
	}
	bid, ask := p.base-half, p.base+half
	return TickMsg{
		Symbol: p.sym.Name,
		Bid:    price.Format(bid, p.sym.Decimals),
		Ask:    price.Format(ask, p.sym.Decimals),
		Venue:  venue,
		Tier:   tier,
		TS:     ts,
	}
}

// Emitter shapes emission toward a target tick rate: accumulate the
// fractional budget, hand out whole ticks per interval.
type Emitter struct {
	target float64
	acc    float64
}

func NewEmitter(targetPerSec float64) *Emitter { return &Emitter{target: targetPerSec} }

// Take returns how many ticks to emit over the next interval.
func (e *Emitter) Take(interval time.Duration) int {
	e.acc += e.target * interval.Seconds()
	n := int(e.acc)
	e.acc -= float64(n)
	return n
}
```

(`rand.NewChaCha8` takes a `[32]byte` seed — `SeedBytes` above builds one deterministically from the uint64 flag, and is shared with `cmd/chronoctl`. `TestMarketDeterministic` compares TickMsg structs: TS must be passed identically; the test passes the same `ts` value, and `TS` is a field so `==` covers it.)

- [ ] **Step 4: Run tests**

Run: `go test ./internal/feed/ -race -count=1`
Expected: PASS. The drift bound (±100×) is loose on purpose: over 5000 single-pair steps σ=0.3% the walk can reach ~e^{0.003·√5000} ≈ 1.23× — well inside.

- [ ] **Step 5: Commit**

```bash
git add internal/feed/
git commit -m "feat(feed): seeded crypto market simulator with heat and tier spreads"
```

---

### Task 10: `cmd/chronofeed` — the rate feed

**Files:**
- Create: `cmd/chronofeed/main.go`
- Test: `cmd/chronofeed/main_test.go`

**Interfaces:**
- Consumes: `internal/feed.Market/Emitter`, `chronov1.NewFeedServiceClient`, gRPC health client, `internal/catalog`.
- Produces: the `chronofeed` binary; testable seam:

```go
func run(ctx context.Context, conn *grpc.ClientConn, rate float64, seed uint64, streams int) error
// blocks until ctx done; one client-stream per venue, batches ≤64 flushed
// every ≤10ms; per-stream reconnect with capped backoff; no replay.
```

- [ ] **Step 1: Write the failing test `cmd/chronofeed/main_test.go`**

```go
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newCore(t *testing.T) (*service.Core, *grpc.ClientConn) {
	t.Helper()
	now := time.Now()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), now)
	t.Cleanup(core.Close)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return core, conn
}

func TestRunFeedsTicks(t *testing.T) {
	core, conn := newCore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	_ = conn // silence unused if run signature evolves
	done := make(chan error, 1)
	go func() { done <- run(ctx, conn, 5000, 1, 3) }()
	<-ctx.Done()
	// run returns shortly after ctx cancellation.
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if got := core.Engine().Stats().Live; got != 0 {
		t.Fatalf("feed created alerts? live=%d", got)
	}
	if core.FeedEverConnected() == false {
		t.Fatal("feed never connected")
	}
	// At 5000/s over ~1.2s (minus startup) several thousand ticks must land.
	if n := core.VenueTicks()["ATLAS"] + core.VenueTicks()["NOVA"] + core.VenueTicks()["ZENITH"]; n < 1000 {
		t.Fatalf("only %d ticks ingested", n)
	}
}
```

(`prometheus` import is unused — drop it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronofeed/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `cmd/chronofeed/main.go`**

```go
// Command chronofeed is a synthetic crypto market-data feed: it walks the
// catalog's symbols and streams ticks into chronod over gRPC, one
// client-stream per venue, at a configurable target rate.
package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/feed"
)

const (
	batchMax     = 64
	flushEvery   = 10 * time.Millisecond
	backoffStart = 100 * time.Millisecond
	backoffMax   = 2 * time.Second
)

func main() {
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	rate := flag.Float64("rate", 20000, "target ticks per second")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	streams := flag.Int("venue-streams", 3, "parallel gRPC streams (one per venue, 1..3)")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if *streams < 1 || *streams > len(catalog.Venues) {
		slog.Error("venue-streams out of range", "got", *streams)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial", "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	// Wait for chronod health before generating.
	hc := healthpb.NewHealthClient(conn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		check, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && check.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			slog.Error("chronod never became healthy", "err", err)
			os.Exit(1)
		}
		time.Sleep(250 * time.Millisecond)
	}

	slog.Info("feeding", "server", *server, "rate", *rate, "seed", *seed, "streams", *streams)
	if err := run(ctx, conn, *rate, *seed, *streams); err != nil && ctx.Err() == nil {
		slog.Error("feed exit", "err", err)
		os.Exit(1)
	}
	slog.Info("feed stopped")
}

// run drives the market and the per-venue streams until ctx is done.
func run(ctx context.Context, conn *grpc.ClientConn, rate float64, seed uint64, streams int) error {
	fc := chronov1.NewFeedServiceClient(conn)

	// Cross-check the server's reference data against ours — both are
	// compiled from the same table; drift means version skew.
	if err := verifyCatalog(ctx, fc); err != nil {
		return err
	}

	market := feed.NewMarket(catalog.Default().Symbols(), catalog.Venues[:streams], catalog.Tiers, seed)
	emitter := feed.NewEmitter(rate)

	// One buffered channel per venue; the dispatcher routes ticks by msg.Venue.
	type venueCh struct {
		name string
		ch   chan *chronov1.Tick
	}
	chs := make([]venueCh, streams)
	for i := range chs {
		chs[i] = venueCh{name: catalog.Venues[i], ch: make(chan *chronov1.Tick, batchMax*2)}
	}
	byVenue := make(map[string]chan *chronov1.Tick, streams)
	for _, vc := range chs {
		byVenue[vc.name] = vc.ch
	}

	// Venue stream workers.
	errCh := make(chan error, streams)
	for i := range chs {
		go func(name string, ch chan *chronov1.Tick) {
			errCh <- streamVenue(ctx, fc, name, ch)
		}(chs[i].name, chs[i].ch)
	}

	// Producer: emit at the target rate, stamp, route.
	tick := time.NewTicker(flushEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, vc := range chs { // drain signal: close inputs, workers finish
				close(vc.ch)
			}
			for range chs { // wait for workers; they return nil on closed input
				if err := <-errCh; err != nil && err != context.Canceled {
					return err
				}
			}
			return ctx.Err()
		case <-tick.C:
			now := time.Now()
			for n := emitter.Take(flushEvery); n > 0; n-- {
				msg := market.Tick(now)
				byVenue[msg.Venue] <- &chronov1.Tick{
					Symbol: msg.Symbol, Bid: msg.Bid, Ask: msg.Ask,
					Venue: msg.Venue, Tier: msg.Tier, TsUnixNanos: now.UnixNano(),
				}
			}
		}
	}
}

// streamVenue maintains one StreamTicks RPC: batch ≤64, flush on full or
// every flushEvery, reconnect with capped backoff on error. Live-only —
// nothing is replayed after a reconnect.
func streamVenue(ctx context.Context, fc chronov1.FeedServiceClient, venue string, ch chan *chronov1.Tick) error {
	backoff := backoffStart
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := pumpStream(ctx, fc, venue, ch)
		if ctx.Err() != nil {
			return nil
		}
		slog.Warn("stream ended; reconnecting", "venue", venue, "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

func pumpStream(ctx context.Context, fc chronov1.FeedServiceClient, venue string, ch chan *chronov1.Tick) error {
	stream, err := fc.StreamTicks(ctx)
	if err != nil {
		return err
	}
	batch := make([]*chronov1.Tick, 0, batchMax)
	flush := time.NewTicker(flushEvery)
	defer flush.Stop()
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := stream.Send(&chronov1.TickBatch{Ticks: batch}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			_, _ = stream.CloseAndRecv()
			return nil
		case <-flush.C:
			if err := send(); err != nil {
				return err
			}
		case tk, ok := <-ch:
			if !ok {
				return stream.CloseAndRecv() // report FeedStatus
			}
			batch = append(batch, tk)
			if len(batch) >= batchMax {
				if err := send(); err != nil {
					return err
				}
			}
		}
	}
}

// verifyCatalog fails fast if server and feed disagree on reference data.
func verifyCatalog(ctx context.Context, fc chronov1.FeedServiceClient) error {
	reply, err := fc.GetCatalog(ctx, &chronov1.CatalogRequest{})
	if err != nil {
		return err
	}
	want := catalog.Default()
	if got := len(reply.GetSymbols()); got != len(want.Symbols()) {
		return fmt.Errorf("catalog skew: server has %d symbols, local has %d", got, len(want.Symbols()))
	}
	for _, s := range reply.GetSymbols() {
		w, ok := want.Symbol(s.GetSymbol())
		if !ok || uint8(s.GetDecimals()) != w.Decimals || s.GetReferencePrice() != w.Reference {
			return fmt.Errorf("catalog skew at %s: server=%d/%s local=%d/%s",
				s.GetSymbol(), s.GetDecimals(), s.GetReferencePrice(), w.Decimals, w.Reference)
		}
	}
	return nil
}
```

Imports for `main.go`: `context`, `flag`, `fmt`, `log/slog`, `os`, `os/signal`, `syscall`, `time`, `grpc`, `insecure`, `healthpb`, `chronov1`, `catalog`, `feed` (`io` is NOT needed). Design note baked in: `run`'s producer may block writing to a venue channel whose worker is in backoff — the buffer is 2×batch and backoff is capped, so bursts self-limit; this is intentional pressure shaping, and the comment in the producer says so. `pumpStream`'s graceful-close `CloseAndRecv()` returns the FeedStatus, which we discard.

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronofeed/ -race -count=1 -timeout 60s`
Expected: PASS, goleak clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronofeed/
git commit -m "feat(chronofeed): synthetic rate feed over venue streams"
```

---

### Task 11: `cmd/chronoctl` — alert, watch, demo

**Files:**
- Create: `cmd/chronoctl/main.go`
- Test: `cmd/chronoctl/demo_test.go`

**Interfaces:**
- Consumes: `chronov1` clients, `catalog.Default()` (for anchors), `price.Parse/Format`.
- Produces: the `chronoctl` binary with subcommands `alert`, `watch`, `demo`; testable core:

```go
type Clients struct {
	Alerts chronov1.AlertServiceClient
	Feed   chronov1.FeedServiceClient
}
func runDemo(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts int) error
// seeds nAlerts dim-scoped ABOVE alerts at reference ±2%, then watches and
// prints triggers until ctx is done; returns on ctx cancellation.
func printTrigger(w io.Writer, tr *chronov1.Trigger)
```

- [ ] **Step 1: Write the failing test `cmd/chronoctl/demo_test.go`**

```go
package main

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestRunDemoFiresAndPrints(t *testing.T) {
	now := time.Now()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), now)
	t.Cleanup(core.Close)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := Clients{Alerts: chronov1.NewAlertServiceClient(conn), Feed: chronov1.NewFeedServiceClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var buf guardedBuffer
	done := make(chan error, 1)
	go func() { done <- runDemo(ctx, c, &buf, 7, 30) }()

	// Give the demo a moment to register its alerts, then push prices
	// through every venue×tier combo at +10% of each major's reference.
	time.Sleep(500 * time.Millisecond)
	for _, sym := range []catalog.Symbol{
		{Name: "BTCUSDT", Decimals: 2, Reference: "65000.00"},
		{Name: "ETHUSDT", Decimals: 2, Reference: "3400.00"},
	} {
		for _, v := range catalog.Venues {
			for _, ti := range catalog.Tiers {
				base := parseRef(t, sym.Reference, sym.Decimals)
				ask := base * 110 / 100
				stream, err := c.Feed.StreamTicks(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_ = stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
					Symbol: sym.Name,
					Bid:    fmtPrice(base*109/100, sym.Decimals),
					Ask:    fmtPrice(ask, sym.Decimals),
					Venue:  v, Tier: ti, TsUnixNanos: time.Now().UnixNano(),
				}}})
				if _, err := stream.CloseAndRecv(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && strings.Count(buf.String(), "FIRED") == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	out := buf.String()
	if !strings.Contains(out, "FIRED") {
		t.Fatalf("no trigger printed:\n%s", out)
	}
}

// guardedBuffer is a mutex-guarded bytes.Buffer (the demo writes from its
// watcher goroutine).
type guardedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (g *guardedBuffer) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *guardedBuffer) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}
```

The test also needs these two small helpers (define them in the test file):

```go
func parseRef(t *testing.T, ref string, dec uint8) int64 {
	t.Helper()
	v, err := price.Parse(ref, dec)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func fmtPrice(base int64, dec uint8) string { return price.Format(base, dec) }
```

and imports `sync` and `"github.com/emir/chrono-tree/price"`; drop the `math/rand/v2` import (unused).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronoctl/`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write `cmd/chronoctl/main.go`**

```go
// Command chronoctl is the demo client: register alerts, watch triggers,
// or run the full demo loop.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/feed"
	"github.com/emir/chrono-tree/price"
)

type Clients struct {
	Alerts chronov1.AlertServiceClient
	Feed   chronov1.FeedServiceClient
}

func dial(server string) (*grpc.ClientConn, error) {
	return grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func main() {
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	// Subcommand flags.
	alertCmd := flag.NewFlagSet("alert", flag.ExitOnError)
	alertPair := alertCmd.String("pair", "BTCUSDT", "symbol")
	alertVenue := alertCmd.String("venue", "ATLAS", "venue dim value")
	alertTier := alertCmd.String("tier", "TOP", "tier dim value")
	alertDir := alertCmd.String("dir", "above", "above|below")
	alertType := alertCmd.String("type", "ask", "bid|ask|mid|last")
	alertPrice := alertCmd.String("price", "", "target price (decimal string)")
	demoCmd := flag.NewFlagSet("demo", flag.ExitOnError)
	demoN := demoCmd.Int("n", 1000, "alerts to seed")
	demoSeed := demoCmd.Uint64("seed", 1, "demo RNG seed")

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: chronoctl [flags] <alert|watch|demo> ...")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := dial(*server)
	if err != nil {
		slog.Error("dial", "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()
	c := Clients{Alerts: chronov1.NewAlertServiceClient(conn), Feed: chronov1.NewFeedServiceClient(conn)}

	switch os.Args[1] {
	case "alert":
		if err := alertCmd.Parse(os.Args[2:]); err != nil {
			os.Exit(2)
		}
		if err := runAlert(ctx, c, *alertPair, *alertVenue, *alertTier, *alertDir, *alertType, *alertPrice); err != nil {
			slog.Error("alert", "err", err)
			os.Exit(1)
		}
	case "watch":
		if err := runWatch(ctx, c, os.Stdout); err != nil && ctx.Err() == nil {
			slog.Error("watch", "err", err)
			os.Exit(1)
		}
	case "demo":
		if err := demoCmd.Parse(os.Args[2:]); err != nil {
			os.Exit(2)
		}
		if err := runDemo(ctx, c, os.Stdout, *demoSeed, *demoN); err != nil && ctx.Err() == nil {
			slog.Error("demo", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand", os.Args[1])
		os.Exit(2)
	}
}

func priceTypeOf(s string) (chronov1.PriceType, error) {
	switch s {
	case "bid":
		return chronov1.PriceType_PRICE_TYPE_BID, nil
	case "ask":
		return chronov1.PriceType_PRICE_TYPE_ASK, nil
	case "mid":
		return chronov1.PriceType_PRICE_TYPE_MID, nil
	case "last":
		return chronov1.PriceType_PRICE_TYPE_LAST, nil
	}
	return 0, fmt.Errorf("unknown price type %q", s)
}

func dirOf(s string) (chronov1.Direction, error) {
	switch s {
	case "above":
		return chronov1.Direction_DIRECTION_ABOVE, nil
	case "below":
		return chronov1.Direction_DIRECTION_BELOW, nil
	}
	return 0, fmt.Errorf("unknown direction %q", s)
}

func runAlert(ctx context.Context, c Clients, pair, venue, tier, dir, ptype, priceStr string) error {
	pt, err := priceTypeOf(ptype)
	if err != nil {
		return err
	}
	d, err := dirOf(dir)
	if err != nil {
		return err
	}
	resp, err := c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
		Symbol: pair, PriceType: pt, Direction: d, TargetPrice: priceStr,
		Venue: venue, Tier: tier,
	})
	if err != nil {
		return err
	}
	fmt.Println(resp.GetAlertId())
	return nil
}

func printTrigger(w io.Writer, tr *chronov1.Trigger) {
	fmt.Fprintf(w, "FIRED %s %s %s/%s price=%s target=%s dir=%s at=%s\n",
		tr.GetAlertId(), tr.GetSymbol(), tr.GetVenue(), tr.GetTier(),
		tr.GetFiredPrice(), tr.GetTargetPrice(),
		tr.GetDirection().String(),
		time.Unix(0, tr.GetFiredAtUnixNanos()).UTC().Format(time.RFC3339Nano))
}

func runWatch(ctx context.Context, c Clients, out io.Writer) error {
	stream, err := c.Alerts.WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		return err
	}
	for {
		tr, err := stream.Recv()
		if err != nil {
			return err
		}
		printTrigger(out, tr)
	}
}

// runDemo seeds nAlerts dim-scoped ABOVE alerts at reference ±2% across
// random pairs/venues/tiers (plus one deliberate per-venue fan-out set on
// BTCUSDT), then watches and prints triggers until ctx is done.
func runDemo(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts int) error {
	cat := catalog.Default()
	syms := cat.Symbols()
	rng := rand.New(rand.NewChaCha8(*feed.SeedBytes(seed)))

	seedOne := func(sym catalog.Symbol, venue, tier string) error {
		ref, err := price.Parse(sym.Reference, sym.Decimals)
		if err != nil {
			return err
		}
		f := 0.98 + 0.04*rng.Float64() // ±2% of reference
		target := int64(float64(ref)*f + 0.5)
		_, err = c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction: chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: price.Format(target, sym.Decimals),
			Venue:       venue, Tier: tier,
		})
		if err != nil {
			return err
		}
		return nil
	}

	venues, tiers := cat.DimValues(catalog.DimVenue), cat.DimValues(catalog.DimTier)
	for i := 0; i < nAlerts; i++ {
		sym := syms[rng.IntN(len(syms))]
		if err := seedOne(sym, venues[rng.IntN(len(venues))], tiers[rng.IntN(len(tiers))]); err != nil {
			return err
		}
	}
	// Fan-out set: same target on BTCUSDT ask, one alert per venue — the
	// "any venue" pattern is the caller's fan-out, one alert per value.
	var btc catalog.Symbol
	for _, s := range syms {
		if s.Name == "BTCUSDT" {
			btc = s
		}
	}
	for _, v := range venues {
		if err := seedOne(btc, v, catalog.Tiers[0]); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "seeded %d alerts (incl. %d-venue BTCUSDT fan-out); watching\n", nAlerts+len(venues), len(venues))
	return runWatch(ctx, c, out)
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronoctl/ -race -count=1 -timeout 60s`
Expected: PASS, goleak clean. (`feed.SeedBytes` is the single derivation shared by feed and demo — no duplication.)

- [ ] **Step 5: Commit**

```bash
git add cmd/chronoctl/
git commit -m "feat(chronoctl): demo client with alert/watch/demo"
```

---

### Task 12: Stress, service-layer oracle, integration smoke, README, gates

**Files:**
- Create: `internal/service/stress_test.go`
- Create: `internal/service/oracle_test.go`
- Create: `tests/integration_test.go` (build tag `integration`)
- Modify: `README.md`

**Interfaces:**
- Consumes: everything.
- Produces: the acceptance evidence for the spec's gates.

- [ ] **Step 1: Write `internal/service/stress_test.go` (race-detector concurrency)**

```go
package service

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
)

// TestStressWatchersAndFeed: 4 watchers, parallel upserts and cancels, and
// a 5k/s tick stream for ~1.5s. Under -race this exercises the pump,
// broadcast eviction, and catalog mutation together. Assertions are the
// documented invariants: triggers fired > 0, deliveries observed, no
// watcher drops at this modest rate.
func TestStressWatchersAndFeed(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var delivered = make([]uint64, 4)
	for i := range delivered {
		stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
		if err != nil {
			t.Fatal(err)
		}
		go func(i int, s chronov1.AlertService_WatchTriggersClient) {
			for {
				if _, err := s.Recv(); err != nil {
					return
				}
				delivered[i]++
			}
		}(i, stream)
	}

	syms := catalog.Default().Symbols()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	// 500 alerts within ±1% of reference: many will fire under the stream.
	upserts := make(chan int, 500)
	for i := 0; i < 500; i++ {
		upserts <- i
	}
	close(upserts)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range upserts {
				sym := syms[rng.IntN(len(syms))]
				ref := parseRefForTest(t, sym)
				target := int64(float64(ref) * (0.99 + 0.02*rng.Float64()))
				_, err := e.alerts().UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
					Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
					Direction: chronov1.Direction_DIRECTION_ABOVE,
					TargetPrice: formatForTest(t, target, sym.Decimals),
					Venue:       catalog.Venues[rng.IntN(3)], Tier: catalog.Tiers[rng.IntN(2)],
				})
				if err != nil {
					return // ctx expired mid-seed
				}
			}
		}()
	}

	// Feed: random symbols, prices above reference — fires the ABOVE alerts.
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		stream, err := e.feed().StreamTicks(ctx)
		if err != nil {
			return
		}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				_, _ = stream.CloseAndRecv()
				return
			case <-tick.C:
				for i := 0; i < 50; i++ { // 5k/s
					sym := syms[rng.IntN(len(syms))]
					ref := parseRefForTest(t, sym)
					ask := ref * 105 / 100
					if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
						Symbol: sym.Name,
						Bid:    formatForTest(t, ref*104/100, sym.Decimals),
						Ask:    formatForTest(t, ask, sym.Decimals),
						Venue:  catalog.Venues[rng.IntN(3)], Tier: catalog.Tiers[rng.IntN(2)],
						TsUnixNanos: time.Now().UnixNano(),
					}}}); err != nil {
						return
					}
				}
			}
		}
	}()

	wg.Wait()
	<-feedDone
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.core.stats.TriggersFired.Load() > 0 && sum(delivered) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if e.core.stats.TriggersFired.Load() == 0 {
		t.Fatal("nothing fired under stress")
	}
	if sum(delivered) == 0 {
		t.Fatal("watchers observed no deliveries")
	}
	if got := e.core.stats.WatcherDrops.Load(); got != 0 {
		t.Fatalf("watcher drops at 5k/s: %d (watchers should keep up)", got)
	}
}

func sum(v []uint64) uint64 {
	var s uint64
	for _, x := range v {
		s += x
	}
	return s
}
```

Helpers (same file):

```go
func parseRefForTest(t *testing.T, s catalog.Symbol) int64 {
	t.Helper()
	v, err := price.Parse(s.Reference, s.Decimals)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func formatForTest(t *testing.T, base int64, dec uint8) string {
	t.Helper()
	return price.Format(base, dec)
}
```

(imports add `sync` and `"github.com/emir/chrono-tree/price"`. The `delivered[i]++` from one goroutine per index is race-free — each index has exactly one writer.)

- [ ] **Step 2: Write `internal/service/oracle_test.go` (service-layer brute-force oracle)**

```go
package service

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

func TestServiceOracle(t *testing.T) {
	e := newEnv(t)
	cat := catalog.Default()
	all := cat.Symbols()
	syms := all[:25]
	rng := rand.New(rand.NewChaCha8([32]byte{1, 2, 3}))
	venues, tiers := catalog.Venues, catalog.Tiers

	type oracleAlert struct {
		id     string
		symbol string
		venue  string
		tier   string
		dir    chronov1.Direction
		target int64 // base units
		fired  bool
	}
	alerts := make([]oracleAlert, 200)
	for i := range alerts {
		sym := syms[rng.IntN(len(syms))]
		venue := venues[rng.IntN(len(venues))]
		tier := tiers[rng.IntN(len(tiers))]
		dir := chronov1.Direction_DIRECTION_ABOVE
		if rng.IntN(2) == 0 {
			dir = chronov1.Direction_DIRECTION_BELOW
		}
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		target := int64(float64(ref)*(0.9+0.2*rng.Float64()) + 0.5)
		resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction: dir, TargetPrice: price.Format(target, sym.Decimals),
			Venue: venue, Tier: tier,
		})
		if err != nil {
			t.Fatalf("seed upsert %d: %v", i, err)
		}
		alerts[i] = oracleAlert{id: resp.GetAlertId(), symbol: sym.Name, venue: venue, tier: tier, dir: dir, target: target}
	}

	// Watch in the background, collecting trigger IDs.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	wstream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := make(chan string, 4096)
	go func() {
		for {
			tr, err := wstream.Recv()
			if err != nil {
				close(gotIDs)
				return
			}
			gotIDs <- tr.GetAlertId()
		}
	}()
	// Let the watcher register before ticking.
	deadline := time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// Random tick stream; the naive evaluator applies each tick in order.
	type wireTick struct {
		symbol string
		ask    int64
		venue  string
		tier   string
	}
	var stream chronov1.FeedService_StreamTicksClient
	stream, err = e.feed().StreamTicks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var expected []string
	for i := 0; i < 3000; i++ {
		sym := syms[rng.IntN(len(syms))]
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		ask := int64(float64(ref)*(0.85+0.3*rng.Float64()) + 0.5)
		wt := wireTick{symbol: sym.Name, ask: ask, venue: venues[rng.IntN(len(venues))], tier: tiers[rng.IntN(len(tiers))]}
		for j := range alerts {
			a := &alerts[j]
			if a.fired || a.symbol != wt.symbol || a.venue != wt.venue || a.tier != wt.tier {
				continue
			}
			above := a.dir == chronov1.Direction_DIRECTION_ABOVE
			if (above && ask >= a.target) || (!above && ask <= a.target) {
				a.fired = true
				expected = append(expected, a.id)
			}
		}
		if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
			Symbol: wt.symbol,
			Bid:    price.Format(ask*99/100, sym.Decimals),
			Ask:    price.Format(ask, sym.Decimals),
			Venue:  wt.venue, Tier: wt.tier, TsUnixNanos: time.Now().UnixNano(),
		}}}); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}

	// Collect until we have the expected count or time out.
	want := map[string]bool{}
	for _, id := range expected {
		want[id] = true
	}
	have := map[string]bool{}
	timeout := time.After(10 * time.Second)
	for uint64(len(have)) < uint64(len(want)) && len(want) > 0 {
		select {
		case id, ok := <-gotIDs:
			if !ok {
				t.Fatalf("watch ended early: have %d of %d", len(have), len(want))
			}
			if !want[id] {
				t.Fatalf("unexpected trigger %s (not in oracle's fired set)", id)
			}
			have[id] = true
		case <-timeout:
			t.Fatalf("timed out: have %d of %d expected triggers", len(have), len(want))
		}
	}
	// Drain briefly for strays — none may appear.
	strayDeadline := time.After(1500 * time.Millisecond)
	for {
		select {
		case id, ok := <-gotIDs:
			if !ok {
				return
			}
			if !want[id] {
				t.Fatalf("unexpected trigger %s after completion", id)
			}
		case <-strayDeadline:
			return
		}
	}
}
```

- [ ] **Step 3: Write `tests/integration_test.go` (build tag `integration`)**

```go
//go:build integration

// End-to-end smoke: real binaries, real sockets, real network hop.
// Run: go test -tags integration ./tests -run Integration -v -timeout 120s
package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestIntegrationEndToEnd(t *testing.T) {
	grpcPort, httpPort := freePort(t), freePort(t)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	daemon := exec.CommandContext(ctx, "go", "run", "./cmd/chronod",
		"-grpc-addr", grpcAddr, "-http-addr", httpAddr)
	feeder := exec.CommandContext(ctx, "go", "run", "./cmd/chronofeed",
		"-server", grpcAddr, "-rate", "20000", "-seed", "1")
	startAndWait := func(cmd *exec.Cmd) {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %v: %v", cmd.Args, err)
		}
	}
	startAndWait(daemon)
	t.Cleanup(func() { _ = daemon.Process.Kill() })
	startAndWait(feeder)
	t.Cleanup(func() { _ = feeder.Process.Kill() })

	// Wait for readiness.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + httpAddr + "/readyz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Wire one alert near BTC's anchor and watch for it to fire.
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	alerts := chronov1.NewAlertServiceClient(conn)
	wctx, wcancel := context.WithTimeout(ctx, 30*time.Second)
	defer wcancel()
	wstream, err := alerts.WatchTriggers(wctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_BELOW, // walk down through it
		TargetPrice: "66000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	triggered := make(chan struct{})
	go func() {
		for {
			if _, err := wstream.Recv(); err == nil {
				close(triggered)
				return
			}
		}
	}()
	select {
	case <-triggered:
	case <-time.After(25 * time.Second):
		t.Fatal("no trigger observed end-to-end")
	}

	// /stats must show the feed's rate.
	resp, err := http.Get("http://" + httpAddr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	rate, _ := body["ticks_per_sec"].(float64)
	if rate < 19500 {
		t.Fatalf("ticks_per_sec = %v, want >= 19500 (spec gate)", rate)
	}
}
```

Notes: `go run` paths are relative to the repo root (run tests from the root — `go test` does). The watcher goroutine's close-on-first-trigger error swallow is acceptable for a smoke test. The BELOW alert at 66000 — above the 65000 anchor — is crossed by the random walk within seconds, inside the 25s window.

- [ ] **Step 4: README update**

In `README.md`, replace the final `## Status` section with a `## Services` section (keep `docs/superpowers/specs/` pointer at its end):

```markdown
## Services

Three binaries wrap the engine (spec: `docs/superpowers/specs/2026-09-04-service-design.md`):

- `chronod` — hosts the engine: `chrono.v1` gRPC on `:9090` (alerts, trigger
  streaming, tick ingestion, catalog; gRPC health + reflection), HTTP status
  on `:8080` (`/healthz`, `/readyz`, `/stats` json/v2, `/metrics`
  Prometheus, `/debug/pprof/`).
- `chronofeed` — synthetic crypto market: ~500 pairs (realistic majors down
  to sub-cent memecoins), heat-weighted random walk, 3 fictional venues ×
  2 book tiers as the engine's two dims, one client-stream per venue,
  `-rate 20000` default.
- `chronoctl` — demo client: `alert` (register one), `watch` (print
  triggers), `demo` (seed 1000 dim-scoped alerts + a per-venue fan-out
  set, then print every trigger).

```sh
go run ./cmd/chronod &
go run ./cmd/chronofeed -rate 20000 &
go run ./cmd/chronoctl demo -n 1000
curl -s localhost:8080/stats | jq
```

Prices are decimal strings end to end (`"65000.12"`), converted exactly
through the `price` package at the gRPC boundary. Triggers stream to every
`WatchTriggers` client; a watcher that falls behind is disconnected with
`ResourceExhausted` (drop-and-log — the pump never blocks, mirroring the
engine's own ring). The engine itself is untouched: the service layer adds
catalogs, conversion, fan-out and observability around it.

Integration smoke (real binaries over real sockets):

```sh
go test -tags integration ./tests -run Integration -v -timeout 120s
```

Not yet built: persistence, TLS/auth, multi-node anything.
```

Also update the earlier `## Packages` section with one line each for `internal/catalog`, `internal/service`, `internal/server`, `internal/feed`, and the `cmd/` binaries (one sentence per package, matching the existing style), and note in the status line that engine + price + service layer are complete.

- [ ] **Step 5: Run everything (the gates)**

```bash
go build ./... && go vet ./...
go test ./... -race -count=1
go test ./internal/service -run 'Oracle|Stress' -race -count=1 -v
go test -tags integration ./tests -run Integration -v -timeout 120s
```

Expected: all green. The integration run doubles as the spec's feed-rate gate (`ticks_per_sec >= 19500` at `-rate 20000`, asserted inside the test). If the rate gate fails on a loaded machine, re-run once on idle; if it still fails, record the measured number in the task report — do not silently relax the assertion.

- [ ] **Step 6: Commit**

```bash
git add internal/service/ tests/ README.md
git commit -m "test(service): stress, oracle and integration gates; docs"
```

---

## Post-plan notes for the executor

- Generated code (`api/gen/`) is committed; only regenerate with buf when the proto changes (`export PATH="$HOME/go/bin:$PATH" && buf dep update && buf generate` from the repo root).
- The engine and price packages must show ZERO diffs at the end of every task: `git diff --stat main -- engine price` is empty.
- If `buf`/`protoc-gen-go` installs fail (no network), stop and report BLOCKED — do not hand-write generated stubs.
- Go version is 1.27.0 (`go.mod` says `go 1.27.0`); `uuid`, `encoding/json/v2`, `math/rand/v2` require nothing extra.

