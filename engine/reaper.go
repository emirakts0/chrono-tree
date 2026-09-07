package engine

import (
	"time"

	"github.com/tidwall/btype"
)

// compareExp orders the reaper's expiry table by (expires, idx, gen). The
// gen tie-break makes keys unique per handout: a recycled slot's NEW
// registration must not collide with its previous occupant's stale entry
// (integrity cleanup lags the recycle grace by the cadence), and btype
// Insert is a no-op on equal keys — a collision would silently drop the new
// registration, losing the integrity backstop and, when the expires values
// also match, letting the alert fire past expiry (sweep deletes the stale
// entry, the gen-checked CAS fails on the mismatch, nothing re-registers).
// Expiry-ordered iteration is unaffected, and Delete becomes exact.
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

// runReaper is the sole owner of the expiry table. It drains expQ
// registrations, sweeps entries past their deadline, and recycles slots
// whose grace period (2× interval, long after any snapshot reader) elapsed.
// Every IntegrityEvery-th tick it also runs the integrity sweep, the
// backstop for removals lost to a full mutation queue.
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
			// re-checks ticker.C, and a timer channel in the case set makes
			// the runtime lock the timer and read the clock per iteration —
			// µs-scale and chip-serialized on HPET-clocked hosts.
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

// sweep expires entries whose deadline has passed. Slot CAS guards against
// racing fires and cancels; removal (and slot retirement) goes through the
// flusher exactly like a trigger removal.
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
		// Generation-checked CAS: expiry entries outlive the recycle grace,
		// so the slot may have been retired, recycled, and reused since this
		// entry was registered. If the generation moved, both attempts fail —
		// a stale expiry entry must not kill the slot's new occupant.
		if e.slots.casGenAny(x.e.idx, x.gen, StatusExpired, StatusActive, StatusPaused) {
			e.submit(mutation{op: mutRemove, sid: x.sid, e: x.e, gen: x.gen})
		}
	}
	return len(due)
}

// integrity is the backstop for lost removals. fire dispatches from the Match
// hot path, so its removal enqueue is trySubmit-best-effort: a TRIGGERED
// entry whose enqueue failed would stay indexed forever — the expiry sweep
// never touches it (it only CASes Active/Paused). Every alert is registered
// in the expiry table (sentinel deadline when it never expires), so this pass
// walks the registry and re-submits the removal for any entry whose slot is
// still TRIGGERED at the registered generation. retireGen dedupes against a
// removal that did (or later does) land, so a double submission is harmless.
// Spent registrations — slot retired, or recycled into a new generation —
// are collected and deleted so the table tracks live alerts, not history.
// Bounded staleness for a leaked entry: IntegrityEvery × ReaperInterval (this
// cadence) + flush lag.
func (e *Engine) integrity() {
	var spent []expEntry
	for x := range e.expiry.All() {
		w := e.slots.get(x.e.idx).Load()
		if w>>slotGenShift != x.gen {
			// Stale: the slot was recycled since registration, which means
			// this occupant's removal already landed.
			spent = append(spent, x)
			continue
		}
		if w&slotRetiredBit != 0 {
			// Removal already landed for this exact handout.
			spent = append(spent, x)
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
