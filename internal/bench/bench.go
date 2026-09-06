// Package bench generates deterministic performance-campaign workloads:
// symbol universes, alert layouts, and paced mean-reverting tick streams.
// Both drivers (scripts/benchengine, scripts/benchfeed) build on it so the
// engine and service layers measure identical scenarios.
package bench

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/alertstore"
)

// Symbol is one generated tradable pair. Ref is the walk anchor in base
// units at Decimals precision.
type Symbol struct {
	Name     string
	Decimals uint8
	Ref      int64
}

// Symbols builds the deterministic universe of n pairs. Magnitudes are
// log-uniform in [0.00001, 10000) hashed from the name — catalog
// synthAnchor's recipe — with the catalog's precision rule (>=1000 → 2,
// >=1 → 4, else 8), so all three classes appear at any n >= ~30.
func Symbols(n int) []Symbol {
	syms := make([]Symbol, n)
	lo, hi := math.Log(0.00001), math.Log(10000)
	for i := range syms {
		name := fmt.Sprintf("BSYM%04d", i)
		h := fnv.New64a()
		_, _ = h.Write([]byte(name))
		u := h.Sum64()
		x := math.Exp(float64(u%10_000)/10_000*(hi-lo) + lo)
		dec := uint8(8)
		switch {
		case x >= 1000:
			dec = 2
		case x >= 1:
			dec = 4
		}
		syms[i] = Symbol{Name: name, Decimals: dec, Ref: int64(math.Round(x * math.Pow10(int(dec))))}
	}
	return syms
}

// Dim vocabulary: venue value is always 0; tier alternates 0/1. These are
// the engine uint16 values the drivers put in Dims, and the names the
// feeder puts on the wire.
const (
	VenueName = "ATLAS"
	TierTop   = "TOP"
	TierMid   = "MID"
)

func Tier(i int) string {
	if i%2 == 0 {
		return TierTop
	}
	return TierMid
}

// reversion pulls each tick 0.2% of the way back toward the reference —
// chronofeed's walk constant, so the stationary band is ~16σ around Ref.
const reversion = 0.002

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

// Quote is one advanced symbol's prices, base units at its decimals.
type Quote struct {
	SymIdx  int
	TierIdx int
	Bid     int64
	Ask     int64
}

// Market is a per-run mean-reverting walk over the universe, round-robin
// like chronofeed so every symbol streams at an equal rate. holdFirst pins
// symbol 0 at its reference: the GapCluster soak symbol, whose cluster
// must not fire before the gap tick.
type Market struct {
	syms      []Symbol
	base, ref []int64
	sigma     []float64
	rng       *rand.Rand
	symI      int
	holdFirst bool
}

func NewMarket(syms []Symbol, seed uint64, holdFirst bool) *Market {
	m := &Market{
		syms: syms, base: make([]int64, len(syms)), ref: make([]int64, len(syms)),
		sigma: make([]float64, len(syms)), rng: rand.New(rand.NewPCG(seed, seed*2654435761+1)),
		holdFirst: holdFirst,
	}
	for i, s := range syms {
		m.base[i], m.ref[i], m.sigma[i] = s.Ref, s.Ref, sigmaFor(s.Decimals)
	}
	return m
}

// Step advances one symbol and returns its quote.
func (m *Market) Step() Quote {
	i := m.symI % len(m.syms)
	m.symI++
	if m.holdFirst && i == 0 {
		return Quote{SymIdx: 0, TierIdx: 0, Bid: m.ref[0], Ask: m.ref[0] + 1}
	}
	step := reversion*float64(m.ref[i]-m.base[i]) + m.sigma[i]*float64(m.ref[i])*m.rng.NormFloat64()
	b := m.base[i] + int64(step)
	if b < 1 {
		b = 1
	}
	m.base[i] = b
	half := int64(float64(b) * 0.0005) // ~5bp spread, chronofeed's TOP level
	if half < 1 {
		half = 1
	}
	return Quote{SymIdx: i, TierIdx: m.symI % 2, Bid: b - half, Ask: b + half}
}

// Emitter shapes tick emission toward a target rate: accumulate the
// fractional budget, hand out whole ticks per interval (chronofeed's).
type Emitter struct {
	Target, acc float64
}

func (e *Emitter) Take(interval time.Duration) int {
	e.acc += e.Target * interval.Seconds()
	n := int(e.acc)
	e.acc -= float64(n)
	return n
}

// ReadRSS returns VmRSS for pid, in bytes (0 if unreadable).
func ReadRSS(pid int) uint64 { return readStatusKB(pid, "VmRSS") * 1024 }

// ReadPeakRSS returns VmHWM (high-water mark) for pid, in bytes.
func ReadPeakRSS(pid int) uint64 { return readStatusKB(pid, "VmHWM") * 1024 }

func readStatusKB(pid int, key string) uint64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, key+":"); ok {
			kb, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), 10, 64)
			return kb
		}
	}
	return 0
}

// ReadCPUSeconds returns utime+stime for pid. Linux USER_HZ is 100 on
// every mainstream build; the campaigns compare runs on one host, so the
// constant is fine.
func ReadCPUSeconds(pid int) float64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	// Fields 14/15 (1-based) follow "comm)" — find the last ')'.
	s := string(b)
	if i := strings.LastIndexByte(s, ')'); i >= 0 {
		s = s[i+2:]
	}
	f := strings.Fields(s)
	if len(f) < 13 {
		return 0
	}
	utime, _ := strconv.ParseFloat(f[11], 64)
	stime, _ := strconv.ParseFloat(f[12], 64)
	return (utime + stime) / 100
}

// ---- Alert layouts ----

// Spec is a layout-generated alert, transport-agnostic: benchengine turns
// it into engine.AlertSpec, benchfeed into alertstore.Alert.
type Spec struct {
	ID        engine.AlertID
	SymIdx    int
	Symbol    string
	Decimals  uint8
	PriceType engine.PriceType
	Direction engine.Direction
	Target    int64 // base units at Decimals
	TierIdx   int   // 0/1; venue value is always 0
}

// MkID derives a deterministic, non-zero, unique alert id from a layout
// index (UUIDv7-shaped for readability; uniqueness is what matters).
// The version marker sits in byte 0, which the counter never touches —
// masking or ORing it into the counter bytes would sacrifice counter
// bits and collide ids (every 4096 for the brief's byte-6 mask).
func MkID(i uint64) engine.AlertID {
	var id engine.AlertID
	id[0] = 0x70
	binary.BigEndian.PutUint64(id[1:9], i+1)
	return id
}

// parked builds n zero-fire alerts: GTE rungs at 3× the walk anchor, LTE
// rungs at a quarter of it — the walk never leaves the ±few-% band around
// Ref, so nothing ever crosses. IDs start at offset (GapCluster reuses
// this for its parked tail).
func parked(offset, n int, syms []Symbol) []Spec {
	out := make([]Spec, 0, n)
	for i := 0; i < n; i++ {
		sym := syms[(i+offset)%len(syms)]
		s := Spec{
			ID: MkID(uint64(offset + i)), SymIdx: (i + offset) % len(syms),
			Symbol: sym.Name, Decimals: sym.Decimals,
			PriceType: engine.PriceType(i % 4), TierIdx: i % 2,
		}
		if i%2 == 0 {
			s.Direction, s.Target = engine.DirGTE, sym.Ref*3+int64(i)
		} else {
			s.Direction, s.Target = engine.DirLTE, max(sym.Ref/4, 1)
		}
		out = append(out, s)
	}
	return out
}

// Parked returns the zero-fire ingest-control layout over the whole
// universe.
func Parked(n int, syms []Symbol) []Spec { return parked(0, n, syms) }

// Trickle returns n rungs inside ±band of each symbol's anchor,
// alternating ABOVE/BELOW: the mean-reverting walk crosses them at a
// steady, sparse pace.
func Trickle(n int, syms []Symbol, band float64) []Spec {
	out := make([]Spec, 0, n)
	for i := 0; i < n; i++ {
		sym := syms[i%len(syms)]
		frac := (float64(i) + 0.5) / float64(n)
		s := Spec{
			ID: MkID(uint64(i)), SymIdx: i % len(syms),
			Symbol: sym.Name, Decimals: sym.Decimals,
			PriceType: engine.PriceType(i % 4), TierIdx: i % 2,
		}
		if i%2 == 0 {
			s.Direction = engine.DirGTE
			s.Target = int64(float64(sym.Ref) * (1 + band*frac))
		} else {
			s.Direction = engine.DirLTE
			s.Target = int64(float64(sym.Ref) * (1 - band*frac))
		}
		s.Target = max(s.Target, 1)
		out = append(out, s)
	}
	return out
}

// GapCluster returns total alerts — k GTE rungs stacked one base unit
// apart just above symbol 0's anchor (all tier 0, so one gap tick fires
// them all), the rest the parked tail on symbols 1.. — plus the gap
// quote: a price above every cluster target.
func GapCluster(total, k int, syms []Symbol) ([]Spec, Quote) {
	g0 := syms[0]
	cluster := make([]Spec, k)
	for i := 0; i < k; i++ {
		cluster[i] = Spec{
			ID: MkID(uint64(i)), SymIdx: 0, Symbol: g0.Name, Decimals: g0.Decimals,
			PriceType: engine.PriceType(i % 4), Direction: engine.DirGTE,
			Target: g0.Ref + 1 + int64(i), TierIdx: 0,
		}
	}
	specs := append(cluster, parked(k, total-k, syms[1:])...)
	gap := Quote{
		SymIdx: 0, TierIdx: 0,
		Bid: g0.Ref + int64(k) + 10_000,
		Ask: g0.Ref + int64(k) + 10_001,
	}
	return specs, gap
}

// EngineSpec converts a Spec for direct engine submission. ValidFrom 1:
// valid since the dawn of time; AutoDeactivate so fired entries retire.
func EngineSpec(s Spec) engine.AlertSpec {
	return engine.AlertSpec{
		ID: s.ID, Symbol: s.Symbol, PriceType: s.PriceType, Direction: s.Direction,
		TargetPrice: engine.Price(s.Target), ValidFrom: 1, AutoDeactivate: true,
		Dims: engine.Dims(0, uint16(s.TierIdx)),
	}
}

// EngineTick converts a Quote into an engine tick carrying all four
// price types and matching dims.
func EngineTick(syms []Symbol, q Quote, ts int64) engine.Tick {
	s := syms[q.SymIdx]
	return engine.Tick{
		Symbol: s.Name,
		Bid:    engine.Price(q.Bid), Ask: engine.Price(q.Ask),
		Mid:    engine.Price((q.Bid + q.Ask) / 2), Last: engine.Price(q.Ask),
		Present: engine.TickAllPresent(), TS: ts,
		Dims: engine.Dims(0, uint16(q.TierIdx)),
	}
}

// StoreAlert converts a Spec into a persisted record for seeding the
// service's bbolt store.
func StoreAlert(s Spec, now int64) alertstore.Alert {
	return alertstore.Alert{
		ID: s.ID, Symbol: s.Symbol, Decimals: s.Decimals,
		Venue: VenueName, Tier: Tier(s.TierIdx),
		PriceType: s.PriceType, Direction: s.Direction,
		TargetPrice: engine.Price(s.Target),
		ValidFrom: now, State: alertstore.StateActive, CreatedAt: now,
	}
}
