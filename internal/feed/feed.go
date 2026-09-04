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
	TS          time.Time // zero = "caller stamps" (chronofeed stamps at send)
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
