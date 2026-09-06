package alertstore

import (
	"bytes"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

// Filter selects alerts for the inquiry API. Empty strings and false
// Has* flags match anything. Limit/Offset apply after the intersection;
// Limit <= 0 means no cap. Results are newest-first.
type Filter struct {
	State         State
	HasState      bool
	Symbol        string
	Venue         string
	Tier          string
	Direction     engine.Direction
	HasDirection  bool
	Limit, Offset int
}

type idxPred struct {
	bkt    []byte
	prefix []byte
}

// Query intersects the set filters' index ranges and returns one
// materialized page plus the full match count. The lead range is walked
// newest-first (inverted-timestamp keys); each candidate is verified
// against the remaining indexes by exact key seek — bolt seeks are
// O(log n), so the query touches candidates plus a few tree descents,
// never a full scan.
func (s *Store) Query(f Filter) ([]Alert, int, error) {
	var preds []idxPred
	if f.HasState {
		preds = append(preds, idxPred{bktIdxState, statePrefix(f.State)})
	}
	if f.Symbol != "" {
		preds = append(preds, idxPred{bktIdxSymbol, []byte(f.Symbol)})
	}
	if f.Venue != "" {
		preds = append(preds, idxPred{bktIdxVenue, []byte(f.Venue)})
	}
	if f.Tier != "" {
		preds = append(preds, idxPred{bktIdxTier, []byte(f.Tier)})
	}
	if f.HasDirection {
		preds = append(preds, idxPred{bktIdxDirection, dirPrefix(f.Direction)})
	}
	if len(preds) == 0 {
		preds = append(preds, idxPred{bktIdxCreated, nil}) // no filter: all, newest-first
	}
	lead, rest := preds[0], preds[1:] // state first by construction: the UI's dominant filter

	var out []Alert
	total := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		var page []engine.AlertID
		c := tx.Bucket(lead.bkt).Cursor()
		for k, _ := c.Seek(lead.prefix); k != nil && bytes.HasPrefix(k, lead.prefix); k, _ = c.Next() {
			ts, id, ok := idxTail(k, len(lead.prefix))
			if !ok {
				continue
			}
			if !matchesRest(tx, rest, ts, id) {
				continue
			}
			total++
			start := max(f.Offset, 0)
			if f.Limit <= 0 || (total > start && total <= start+f.Limit) {
				page = append(page, id)
			}
		}
		if len(page) == 0 {
			return nil
		}
		// Materialize the page in the same tx: keys came out sorted
		// newest-first, so the page is already ordered.
		alerts := tx.Bucket(bktAlerts)
		out = make([]Alert, 0, len(page))
		for _, id := range page {
			raw := alerts.Get(id[:])
			if raw == nil {
				continue // index/record skew is impossible; tolerated anyway
			}
			a, err := decodeAlert(raw)
			if err != nil {
				return err
			}
			out = append(out, a)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// matchesRest verifies a candidate against the non-lead indexes by
// exact key existence — the index key embeds createdAt, so the seek is
// precise, not a prefix test.
func matchesRest(tx *bolt.Tx, preds []idxPred, ts int64, id engine.AlertID) bool {
	for _, p := range preds {
		if tx.Bucket(p.bkt).Get(idxKey(p.prefix, ts, id)) == nil {
			return false
		}
	}
	return true
}
