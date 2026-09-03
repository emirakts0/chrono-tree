package engine

import (
	"time"

	"github.com/tidwall/btype"
)

// compareExp orders the reaper's expiry table by (expires, idx).
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
	return 0
}

// runReaper is the sole owner of the expiry table. It drains expQ
// registrations, sweeps entries past their deadline, and recycles slots
// whose grace period (2× interval, long after any snapshot reader) elapsed.
func (e *Engine) runReaper() {
	defer e.reapWG.Done()
	e.expiry = *btype.NewTableOptions(btype.TableOptions[expEntry]{Compare: compareExp})
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case x := <-e.expQ:
			e.expiry.Insert(x)
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
		s := e.slots.get(x.e.idx)
		if s.CompareAndSwap(uint32(StatusActive), uint32(StatusExpired)) ||
			s.CompareAndSwap(uint32(StatusPaused), uint32(StatusExpired)) {
			e.submit(mutation{op: mutRemove, sid: x.sid, e: x.e})
		}
	}
	return len(due)
}
