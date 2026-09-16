package engine

import (
	"time"
	"unsafe"

	"github.com/tidwall/btype"
)

// compareExp orders the reaper's expiry table by (expires, ref). The ref
// pointer tie-break keeps keys unique per handout: btype Insert is a no-op
// on equal keys, and one alertRef allocation per upsert means an in-flight
// stale entry (its exact-key dereg still queued behind eager
// deregistration) and a recycled slot's fresh occupant with the same
// deadline can never collide. Pointers are compared numerically; the
// pointee is never touched, so the comparator stays dereference-free.
func compareExp(a, b expEntry) int {
	if a.expires < b.expires {
		return -1
	}
	if a.expires > b.expires {
		return 1
	}
	pa, pb := uintptr(unsafe.Pointer(a.ref)), uintptr(unsafe.Pointer(b.ref))
	switch {
	case pa < pb:
		return -1
	case pa > pb:
		return 1
	}
	return 0
}

// runReaper is the sole owner of the expiry table: it drains expQ
// commands (registrations and exact-key deregs), sweeps entries past their
// deadline, and recycles slots past their grace period.
func (e *Engine) runReaper() {
	defer e.reapWG.Done()
	e.expiry = *btype.NewTableOptions(btype.TableOptions[expEntry]{Compare: compareExp})
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		e.drainExp()
		select {
		case <-e.expNotify:
		case now := <-ticker.C:
			e.sweep(now.UnixNano())
			e.slots.recycle(time.Now().Add(-2 * e.cfg.ReaperInterval))
		case <-e.done:
			e.drainExp()
			return
		}
	}
}

// drainExp applies every pending expiry command: registrations insert,
// deregistrations delete by exact key. Draining at the top of every reaper
// iteration replaces the incidental queue consumption the old channel
// select sometimes performed on the tick path.
func (e *Engine) drainExp() {
	buf := e.expBuf[:0]
	for {
		buf = e.expQ.drain(buf)
		if len(buf) == 0 {
			break
		}
		for _, c := range buf {
			if c.dereg {
				e.expiry.Delete(c.x)
			} else {
				e.expiry.Insert(c.x)
			}
		}
		if len(buf) < cap(buf) {
			break // queue observed empty under the lock
		}
		buf = buf[:0]
	}
	e.expBuf = buf
	e.expLen.Store(int64(e.expiry.Len()))
}

// sweep expires entries whose deadline has passed. Liveness is validated by
// alertRef pointer identity: e.refs must still map the entry's id to the
// exact ref the registration borrowed, which rules out fired, cancelled,
// and replaced handouts without carrying a generation in the entry. Slot
// reuse cannot interleave with this check because recycle runs in this same
// goroutine, after sweep returns — pinned ordering; moving recycle ahead of
// the sweep silently reintroduces the ABA the gen field used to guard.
// Slot CAS (status-only) still decides exactly-once against racing fires
// and cancels; removal flows to the flusher like a trigger removal, with
// the generation derived from the word this CAS wins.
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
		e.mu.Lock()
		ref, ok := e.refs[x.ref.e.id]
		e.mu.Unlock()
		if !ok || ref != x.ref {
			continue // stale: fired, cancelled, or replaced since registration
		}
		// Gen comes from the word the CAS wins — nothing can bump the
		// generation until this slot is retired, recycled, and re-handed,
		// and none of that can happen while this goroutine is in sweep.
		if gen, ok := e.slots.casStatusAny(x.ref.e.idx, StatusExpired, StatusActive, StatusPaused); ok {
			_ = e.submit(mutation{op: mutRemove, sid: x.ref.sid, e: x.ref.e, gen: gen})
		}
	}
	e.expLen.Store(int64(e.expiry.Len()))
	return len(due)
}
