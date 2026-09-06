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
	"sync"

	"github.com/emir/chrono-tree/internal/price"
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
	x := math.Exp(float64(u%10_000)/10_000*(math.Log(100)-math.Log(0.00001)) + math.Log(0.00001))
	dec := decimalsFor(x)
	base := int64(math.Round(x * math.Pow10(int(dec))))
	return price.Format(base, dec)
}

// Symbol looks up one pair.
func (c *Catalog) Symbol(name string) (Symbol, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.symbols[name]
	return s, ok
}

// Symbols returns all pairs in deterministic (construction) order.
func (c *Catalog) Symbols() []Symbol {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order
}

// Decimals returns the quote precision of a pair.
func (c *Catalog) Decimals(name string) (uint8, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.symbols[name]
	return s.Decimals, ok
}

// Dims returns the positional dim names, matching engine.Config.Dims.
func (c *Catalog) Dims() []string { return c.dims }

// Value maps a dim value name to its engine uint16.
func (c *Catalog) Value(dim, valueName string) (uint16, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.values[dim][valueName]
	return v, ok
}

// Name maps an engine uint16 back to its dim value name.
func (c *Catalog) Name(dim string, value uint16) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.names[dim][value]
	return n, ok
}

// DimValues returns the ordered value names of a dim.
func (c *Catalog) DimValues(dim string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.names[dim]))
	for i := 0; ; i++ {
		n, ok := c.names[dim][uint16(i)]
		if !ok {
			return out
		}
		out = append(out, n)
	}
}

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
// the next free slot on first sight. ok=false ⇔ dim is not pre-registered
// or exhausted (ids 0..0xFFFE, 65,535 values; DimSentinel 0xFFFF is
// reserved by the engine).
func (c *Catalog) EnsureValue(dim, valueName string) (uint16, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.names[dim]; !ok {
		return 0, false // dim not pre-registered: never invent a slot
	}
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
