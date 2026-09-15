package engine

import (
	"time"

	"github.com/tidwall/btype"
)

// compareExp orders the reaper's expiry table by (expires, idx, gen). The
// gen tie-break makes keys unique per handout: btype Insert is a no-op on
// equal keys, and a collision between a recycled slot's new registration and
// its previous occupant's stale entry would silently drop the new one.
func compareExp(a, b expEntry) int {
	if a.expires < b.expires {
		return -1
	}
	if a.expires > b.expires {
		return 1
	}
	switch {
	case a.idx < b.idx:
		return -1
	case a.idx > b.idx:
		return 1
	}
	switch {
	case a.gen < b.gen:
		return -1
	case a.gen > b.gen:
		return 1
	}
	return 0
}

// runReaper is the sole owner of the expiry table: it drains expQ
// registrations, sweeps entries past their deadline, and recycles slots past
// their grace period.
func (e *Engine) runReaper() {
	defer e.reapWG.Done()
	e.expiry = *btype.NewTableOptions(btype.TableOptions[expEntry]{Compare: compareExp})
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case x := <-e.expQ:
			e.expiry.Insert(x)
			// Drain what's already queued in one go: each outer selectgo
			// re-reads the ticker channel, which costs a runtime timer lock.
		drain:
			for {
				select {
				case x := <-e.expQ:
					e.expiry.Insert(x)
				default:
					break drain
				}
			}
		case now := <-ticker.C:
			e.sweep(now.UnixNano())
			e.slots.recycle(time.Now().Add(-2 * e.cfg.ReaperInterval))
		case <-e.done:
			for {
				select {
				case x := <-e.expQ:
					e.expiry.Insert(x)
					continue
				default:
				}
				return
			}
		}
	}
}

// sweep expires entries whose deadline has passed. Slot CAS (gen-checked)
// guards against racing fires, cancels, and recycled slots; removal goes
// through the flusher exactly like a trigger removal.
func (e *Engine) sweep(now int64) int {
	const maxPerSweep = 10_000
	var due []expEntry
	for x := range e.expiry.All() { // table is expiry-ordered
		if x.expires > now || len(due) >= maxPerSweep {
			break
		}
		due = append(due, x)
	}
	for _, x := range due {
		e.expiry.Delete(x) // after iteration completes; safe
		// If the generation moved, the slot was retired, recycled, and reused
		// since registration — a stale entry must not kill the new occupant.
		if e.slots.casGenAny(x.idx, x.gen, StatusExpired, StatusActive, StatusPaused) {
			e.submit(mutation{op: mutRemove, sid: x.ref.sid, e: x.ref.e, gen: x.gen})
		}
	}
	return len(due)
}
