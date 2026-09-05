package alertstore

import (
	"slices"
	"testing"

	"github.com/emir/chrono-tree/engine"
)

// seedQueryPop writes a deterministic population exercising every index:
// states, symbols, venues, tiers, directions interleave.
func seedQueryPop(t *testing.T, s *Store) []Alert {
	t.Helper()
	symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	venues := []string{"ATLAS", "NOVA"}
	tiers := []string{"TOP", "MID"}
	dirs := []engine.Direction{engine.DirGTE, engine.DirLTE}
	states := []State{StateActive, StateTriggered, StateCancelled}
	var pop []Alert
	i := 0
	for _, sym := range symbols {
		for _, venue := range venues {
			for _, tier := range tiers {
				for _, dir := range dirs {
					for _, st := range states {
						a := sampleAlert()
						copy(a.ID[:], []byte{byte(i >> 8), byte(i), 0x55})
						a.Symbol, a.Venue, a.Tier, a.Direction = sym, venue, tier, dir
						a.State = st
						a.CreatedAt = int64(1_000_000 + i) // unique, ordered by i
						if st == StateTriggered {
							a.FiredPrice, a.FiredAt = engine.Price(7), int64(i)
						}
						if err := s.Put(a); err != nil {
							t.Fatalf("Put %d: %v", i, err)
						}
						pop = append(pop, a)
						i++
					}
				}
			}
		}
	}
	return pop
}

// bruteForce is the oracle: filter + sort + paginate in memory.
func bruteForce(pop []Alert, f Filter) ([]Alert, int) {
	match := func(a Alert) bool {
		return (!f.HasState || a.State == f.State) &&
			(f.Symbol == "" || a.Symbol == f.Symbol) &&
			(f.Venue == "" || a.Venue == f.Venue) &&
			(f.Tier == "" || a.Tier == f.Tier) &&
			(!f.HasDirection || a.Direction == f.Direction)
	}
	var hits []Alert
	for _, a := range pop {
		if match(a) {
			hits = append(hits, a)
		}
	}
	slices.SortStableFunc(hits, func(x, y Alert) int {
		if x.CreatedAt != y.CreatedAt {
			return int(y.CreatedAt - x.CreatedAt) // newest first
		}
		return int(x.ID[1]) - int(y.ID[1]) // insertion order tiebreak
	})
	total := len(hits)
	start := min(max(f.Offset, 0), total)
	end := total
	if f.Limit > 0 {
		end = min(start+f.Limit, total)
	}
	return hits[start:end], total
}

func TestQueryOracle(t *testing.T) {
	s, _ := openStore(t)
	pop := seedQueryPop(t, s)
	filters := []Filter{
		{},
		{HasState: true, State: StateTriggered},
		{Symbol: "BTCUSDT"},
		{Venue: "NOVA"},
		{Tier: "MID"},
		{HasDirection: true, Direction: engine.DirLTE},
		{HasState: true, State: StateActive, Symbol: "ETHUSDT"},
		{HasState: true, State: StateTriggered, Venue: "ATLAS", Tier: "TOP"},
		{Symbol: "SOLUSDT", Venue: "NOVA", Tier: "MID", HasDirection: true, Direction: engine.DirGTE},
		{HasState: true, State: StateActive, Symbol: "SOLUSDT", Venue: "ATLAS", Tier: "MID", HasDirection: true, Direction: engine.DirLTE},
		{Symbol: "NOSUCH"}, // empty result
	}
	for _, f := range filters {
		for _, pg := range []Filter{
			f,
			{State: f.State, HasState: f.HasState, Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier, Direction: f.Direction, HasDirection: f.HasDirection, Limit: 3},
			{State: f.State, HasState: f.HasState, Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier, Direction: f.Direction, HasDirection: f.HasDirection, Limit: 3, Offset: 4},
		} {
			got, total, err := s.Query(pg)
			if err != nil {
				t.Fatalf("Query(%+v): %v", pg, err)
			}
			want, wantTotal := bruteForce(pop, pg)
			if total != wantTotal {
				t.Fatalf("Query(%+v) total = %d, want %d", pg, total, wantTotal)
			}
			if len(got) != len(want) {
				t.Fatalf("Query(%+v) page len = %d, want %d", pg, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("Query(%+v)[%d] = %v, want %v", pg, i, got[i], want[i])
				}
			}
		}
	}
}
