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
	case a.e.idx < b.e.idx:
		return -1
	case a.e.idx > b.e.idx:
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
// their grace period. Every IntegrityEvery-th tick it runs the integrity
// sweep, the backstop for removals lost to a full mutation queue.
func (e *Engine) runReaper() {
	defer e.reapWG.Done()
	e.expiry = *btype.NewTableOptions(btype.TableOptions[expEntry]{Compare: compareExp})
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()
	ticks := 0
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
			ticks++
			e.sweep(now.UnixNano())
			e.slots.recycle(time.Now().Add(-2 * e.cfg.ReaperInterval))
			if ticks%e.cfg.IntegrityEvery == 0 {
				e.integrity()
			}
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
		if e.slots.casGenAny(x.e.idx, x.gen, StatusExpired, StatusActive, StatusPaused) {
			e.submit(mutation{op: mutRemove, sid: x.sid, e: x.e, gen: x.gen})
		}
	}
	return len(due)
}

// integrity is the backstop for lost removals: fire's removal enqueue is
// best-effort, so a TRIGGERED entry whose enqueue failed would stay indexed
// forever (the expiry sweep only CASes Active/Paused). This pass re-submits
// the removal for any entry whose slot is still TRIGGERED at the registered
// generation; retireGen dedupes, so a double submission is harmless. Spent
// registrations are deleted so the table tracks live alerts, not history.
func (e *Engine) integrity() {
	var spent []expEntry
	for x := range e.expiry.All() {
		w := e.slots.get(x.e.idx).Load()
		if w>>slotGenShift != x.gen {
			spent = append(spent, x) // slot recycled: removal already landed
			continue
		}
		if w&slotRetiredBit != 0 {
			spent = append(spent, x) // removal already landed
			continue
		}
		if slotStatus(w) == StatusTriggered {
			e.submit(mutation{op: mutRemove, sid: x.sid, e: x.e, gen: x.gen})
		}
	}
	for _, x := range spent { // after iteration completes; safe
		e.expiry.Delete(x)
	}
}
