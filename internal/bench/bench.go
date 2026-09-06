// Package bench generates deterministic performance-campaign workloads:
// symbol universes, alert layouts, and paced mean-reverting tick streams.
// Both drivers (scripts/benchengine, scripts/benchfeed) build on it so the
// engine and service layers measure identical scenarios.
package bench

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
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
