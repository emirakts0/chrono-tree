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
		st.snap.CompareAndSwap(nil, newSnapshot(e.dimWidth))
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
			// Slot bookkeeping: retire the slot (gen-gated, so a duplicate
			// removal parks it at most once), and clean refs/live only
			// if the live ref still matches this exact entry (a same-ID upsert
			// may have already replaced it).
			e.slots.retireGen(m.e.idx, m.gen)
			e.mu.Lock()
			if r, ok := e.refs[m.e.id]; ok && r.e.idx == m.e.idx {
				delete(e.refs, m.e.id)
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
	dims, ok := normalizeDims(a.Dims, e.dimWidth)
	if !ok {
		return ErrDims
	}
	var replace mutation
	hasReplace := false
	e.mu.Lock()
	if ref, ok := e.refs[a.ID]; ok {
		// Gated replace: queue the old entry's removal ONLY if we win the CAS
		// to CANCELLED. If the slot is already terminal (TRIGGERED/CANCELLED/
		// EXPIRED), fire/Cancel/the reaper already owns a removal for it —
		// queueing a second one would retire the slot twice and hand the SAME
		// index to two future alerts. The existing owner's removal plus the
		// ref-idx guard in applyBatch still clean the tree entry and the
		// refs/live bookkeeping below exactly once.
		s := e.slots.get(ref.e.idx)
		for {
			w := s.Load()
			if st := slotStatus(w); st != StatusActive && st != StatusPaused {
				break // terminal: another path owns the removal
			}
			if s.CompareAndSwap(w, w&^0xff|uint32(StatusCancelled)) {
				// The generation at CAS-win time is the old entry's handout
				// gen: while its ref exists the slot has not been recycled.
				replace = mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}
				hasReplace = true
				break
			}
			// Lost a concurrent transition (pause/resume); re-read and retry.
		}
		delete(e.refs, a.ID)
		e.live--
	} else if e.live >= e.cfg.MaxAlerts {
		e.mu.Unlock()
		return ErrAlertLimit
	}
	e.mu.Unlock()

	idx := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	// Capture the handout generation now, before the refs are published and
	// before any blocking submit below: a full mutQ can stall the submits for
	// longer than the recycle grace, and once refs are visible another
	// goroutine can cancel this alert — retire, recycle, reuse — so a gen
	// read at expEntry-construction time could be the NEW occupant's.
	// Nothing can retire an unpublished idx, so this point is airtight.
	gen := e.slots.gen(idx)
	ent := entry{
		price:     a.TargetPrice,
		id:        a.ID,
		dims:      dims,
		validFrom: a.ValidFrom,
		expires:   a.Expires,
		idx:       idx,
		flags:     makeFlags(a.PriceType, a.Direction, a.AutoDeactivate),
	}
	e.mu.Lock()
	e.refs[a.ID] = &alertRef{sid: sid, e: ent}
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
	// Every alert is registered with the reaper: never-expiring ones carry
	// the expiryNever sentinel so the integrity sweep can find them (the
	// expiry sweep itself never acts on sentinel entries).
	exp := a.Expires
	if exp == 0 {
		exp = expiryNever
	}
	return e.submitExpiry(expEntry{expires: exp, sid: sid, e: ent, gen: gen})
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
// its removal mutation. refs/live cleanup happens in applyBatch.
func (e *Engine) removeIfLive(id AlertID, want Status) (mutation, error) {
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return mutation{}, ErrNotFound
	}
	s := e.slots.get(ref.e.idx)
	for {
		w := s.Load()
		if st := slotStatus(w); st != StatusActive && st != StatusPaused {
			// TRIGGERED (removal already queued), CANCELLED, EXPIRED
			return mutation{}, ErrInvalidTransition
		}
		if s.CompareAndSwap(w, w&^0xff|uint32(want)) {
			// Generation at CAS-win time is the entry's handout gen: the
			// slot cannot be recycled while its ref is still live.
			return mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}, nil
		}
		// Lost the race (e.g. a concurrent pause/resume): retry while the
		// alert is still live.
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
		w := s.Load()
		switch slotStatus(w) {
		case from:
			if s.CompareAndSwap(w, w&^0xff|uint32(to)) {
				return nil
			}
		case to:
			return nil // already there; idempotent
		default:
			return ErrInvalidTransition
		}
	}
}
