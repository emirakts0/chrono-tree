package alertstore

import (
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

// benchSeed bulk-loads n records in chunked write txs (the bench setup
// is not the system under test; chunking just keeps setup fast).
func benchSeed(b *testing.B, s *Store, n int) []Alert {
	b.Helper()
	alerts := make([]Alert, n)
	for i := range n {
		copy(alerts[i].ID[:], []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), 0x99})
		alerts[i] = Alert{
			Symbol: []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}[i%3], Decimals: 8,
			Venue: []string{"ATLAS", "NOVA", "ZENITH"}[i%3], Tier: []string{"TOP", "MID"}[i%2],
			PriceType: engine.PriceAsk, Direction: engine.Direction(i % 2),
			TargetPrice: engine.Price(i), CreatedAt: int64(1_700_000_000_000_000_000 + i),
			State:       StateActive,
		}
		if i%10 == 0 { // 10% of the population is triggered: the inquiry worst case
			alerts[i].State = StateTriggered
			alerts[i].FiredPrice, alerts[i].FiredAt = engine.Price(i), int64(i)
		}
	}
	const chunk = 10000
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		err := s.db.Batch(func(tx *bolt.Tx) error {
			for i := start; i < end; i++ {
				a := alerts[i]
				if err := putIndexed(tx, &a); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	return alerts
}

// BenchmarkQueryTriggeredPage1 is the spec's acceptance gate:
// state=triggered among 1M alerts, first page of 50, < 10 ms.
func BenchmarkQueryTriggeredPage1(b *testing.B) {
	s, _ := openStoreB(b)
	benchSeed(b, s, 1_000_000)
	b.ResetTimer()
	for b.Loop() {
		items, total, err := s.Query(Filter{HasState: true, State: StateTriggered, Limit: 50})
		if err != nil {
			b.Fatal(err)
		}
		if total != 100_000 || len(items) != 50 {
			b.Fatalf("total=%d len=%d", total, len(items))
		}
	}
}

func openStoreB(b *testing.B) (*Store, string) {
	b.Helper()
	path := b.TempDir() + "/bench.bbolt"
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s, path
}
