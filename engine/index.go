package engine

// stateFor interns sym and guarantees an initial published snapshot.
func (e *Engine) stateFor(sym string) (SymbolID, error) {
	if sid, ok := e.syms.Get(sym); ok {
		if uint64(sid) < uint64(len(e.states)) {
			return sid, nil
		}
		return 0, ErrSymbolLimit
	}
	sid := e.syms.Intern(sym)
	if uint64(sid) >= uint64(len(e.states)) {
		return 0, ErrSymbolLimit
	}
	st := &e.states[sid]
	if st.snap.Load() == nil {
		st.snap.CompareAndSwap(nil, newSnapshot())
	}
	return sid, nil
}

// submit blocks until m is queued (control plane only; the hot path uses
// trySubmit). Returns ErrClosed when the engine is shutting down.
func (e *Engine) submit(m mutation) error {
	select {
	case <-e.done:
		return ErrClosed
	default:
	}
	select {
	case e.mutQ <- m:
		return nil
	case <-e.done:
		return ErrClosed
	}
}

// trySubmit is the hot path's best-effort, never-blocking enqueue. A miss
// only defers cleanup (stale entries are skipped via their status slot and
// swept later); it can never cause a missed or duplicate trigger.
func (e *Engine) trySubmit(m mutation) bool {
	select {
	case e.mutQ <- m:
		return true
	default:
		return false
	}
}

// Sync blocks until every mutation submitted before Sync has been applied.
// Returns immediately if the engine is closed (barrier cannot complete).
func (e *Engine) Sync() {
	done := make(chan struct{})
	if err := e.submit(mutation{op: mutSync, done: done}); err != nil {
		return
	}
	<-done
}

// runFlusher is the single writer: it drains the mutation queue in batches,
// applies each batch to COW copies, and republishes per-symbol snapshots.
func (e *Engine) runFlusher() {
	defer e.flushWG.Done()
	batch := make([]mutation, 0, e.cfg.FlushBatch)
	for {
		select {
		case m := <-e.mutQ:
			batch = append(batch, m)
		drain:
			for len(batch) < cap(batch) {
				select {
				case m := <-e.mutQ:
					batch = append(batch, m)
				default:
					break drain
				}
			}
			e.applyBatch(batch)
			batch = batch[:0]
		case <-e.done:
			for {
				select {
				case m := <-e.mutQ:
					batch = append(batch, m)
					if len(batch) == cap(batch) {
						e.applyBatch(batch)
						batch = batch[:0]
					}
				default:
					e.applyBatch(batch)
					return
				}
			}
		}
	}
}

// applyBatch applies ops grouped by symbol so each symbol is copied once,
// then publishes all snapshots atomically before releasing the old ones.
func (e *Engine) applyBatch(batch []mutation) {
	type pending struct {
		st   *symbolState
		old  *snapshot
		next *snapshot
	}
	bySym := make(map[SymbolID]*pending) // cold path; map is fine
	var syncs []chan struct{}
	for _, m := range batch {
		switch m.op {
		case mutSync:
			syncs = append(syncs, m.done)
			continue
		}
		p := bySym[m.sid]
		if p == nil {
			st := &e.states[m.sid]
			old := st.snap.Load()
			p = &pending{st: st, old: old, next: old.copy()}
			bySym[m.sid] = p
		}
		switch m.op {
		case mutInsert:
			p.next.trees[treeIndex(m.e.priceType(), m.e.direction())].Insert(m.e)
		case mutRemove:
			p.next.trees[treeIndex(m.e.priceType(), m.e.direction())].Delete(m.e)
			// Slot bookkeeping: retire the slot, and clean refs/meta/live only
			// if the live ref still matches this exact entry (a same-ID upsert
			// may have already replaced it).
			e.slots.retire(m.e.idx)
			e.mu.Lock()
			if r, ok := e.refs[m.e.id]; ok && r.e.idx == m.e.idx {
				delete(e.refs, m.e.id)
				delete(e.meta, m.e.id)
				e.live--
			}
			e.mu.Unlock()
		}
	}
	for _, p := range bySym {
		p.st.snap.Store(p.next)
		if !p.old.retireRelease() {
			e.parked = append(e.parked, p.old)
		}
	}
	// Sweep parked snapshots whose readers have drained. In-place filter:
	// the write index never passes the read index.
	alive := e.parked[:0]
	for _, s := range e.parked {
		if s.readers.Load() != 0 {
			alive = append(alive, s)
		} else {
			s.release()
		}
	}
	e.parked = alive
	for _, d := range syncs {
		close(d)
	}
}

// Upsert inserts a new alert or atomically replaces the one with the same ID.
// Blocking on the mutation queue; returns ErrClosed, ErrAlertLimit,
// ErrSymbolLimit, or a validation error.
func (e *Engine) Upsert(a AlertSpec) error {
	if err := a.validate(); err != nil {
		return err
	}
	if e.closed.Load() {
		return ErrClosed
	}
	sid, err := e.stateFor(a.Symbol)
	if err != nil {
		return err
	}
	var replace mutation
	hasReplace := false
	e.mu.Lock()
	if ref, ok := e.refs[a.ID]; ok {
		old := e.slots.get(ref.e.idx)
		old.CompareAndSwap(uint32(StatusActive), uint32(StatusCancelled))
		old.CompareAndSwap(uint32(StatusPaused), uint32(StatusCancelled))
		replace = mutation{op: mutRemove, sid: ref.sid, e: ref.e}
		hasReplace = true
		delete(e.refs, a.ID)
		delete(e.meta, a.ID)
		e.live--
	} else if e.live >= e.cfg.MaxAlerts {
		e.mu.Unlock()
		return ErrAlertLimit
	}
	e.mu.Unlock()

	idx := e.slots.alloc()
	e.slots.get(idx).Store(uint32(StatusActive))
	ent := entry{
		price:     a.TargetPrice,
		id:        a.ID,
		validFrom: a.ValidFrom,
		expires:   a.Expires,
		idx:       idx,
		flags:     makeFlags(a.PriceType, a.Direction, a.AutoDeactivate),
	}
	meta := a.Meta
	e.mu.Lock()
	e.refs[a.ID] = &alertRef{sid: sid, e: ent}
	e.meta[a.ID] = &meta
	e.live++
	e.mu.Unlock()

	// Submission order is queue order: removal of the old entry lands before
	// the new insert. The slot is Active before publication, but the entry is
	// unreachable to readers until the insert flushes.
	if hasReplace {
		if err := e.submit(replace); err != nil {
			return err
		}
	}
	if err := e.submit(mutation{op: mutInsert, sid: sid, e: ent}); err != nil {
		return err
	}
	if a.Expires != 0 {
		return e.submitExpiry(expEntry{expires: a.Expires, sid: sid, e: ent})
	}
	return nil
}

// submitExpiry registers an alert with the reaper's expiry table.
func (e *Engine) submitExpiry(x expEntry) error {
	select {
	case e.expQ <- x:
		return nil
	case <-e.done:
		return ErrClosed
	}
}

// removeIfLive CASes the alert's slot from ACTIVE/PAUSED to want and returns
// its removal mutation. refs/meta/live cleanup happens in applyBatch.
func (e *Engine) removeIfLive(id AlertID, want Status) (mutation, error) {
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return mutation{}, ErrNotFound
	}
	s := e.slots.get(ref.e.idx)
	for {
		cur := Status(s.Load())
		switch cur {
		case StatusActive, StatusPaused:
			if s.CompareAndSwap(uint32(cur), uint32(want)) {
				return mutation{op: mutRemove, sid: ref.sid, e: ref.e}, nil
			}
		default: // TRIGGERED (removal already queued), CANCELLED, EXPIRED
			return mutation{}, ErrInvalidTransition
		}
	}
}

// Cancel permanently retires an alert.
func (e *Engine) Cancel(id AlertID) error {
	m, err := e.removeIfLive(id, StatusCancelled)
	if err != nil {
		return err
	}
	return e.submit(m)
}

// SetStatus transitions an alert between ACTIVE and PAUSED, or cancels it.
// Same-state transitions are idempotent; terminal states reject everything.
func (e *Engine) SetStatus(id AlertID, target Status) error {
	switch target {
	case StatusActive, StatusPaused, StatusCancelled:
	default:
		return ErrInvalidStatus
	}
	if target == StatusCancelled {
		m, err := e.removeIfLive(id, StatusCancelled)
		if err != nil {
			return err
		}
		return e.submit(m)
	}
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	from, to := StatusActive, StatusPaused
	if target == StatusActive {
		from, to = StatusPaused, StatusActive
	}
	s := e.slots.get(ref.e.idx)
	for {
		cur := Status(s.Load())
		switch cur {
		case from:
			if s.CompareAndSwap(uint32(cur), uint32(to)) {
				return nil
			}
		case to:
			return nil // already there; idempotent
		default:
			return ErrInvalidTransition
		}
	}
}
