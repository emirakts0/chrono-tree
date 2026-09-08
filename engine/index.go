package engine

import "time"

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
// only defers cleanup (the integrity sweep re-submits later); it can never
// cause a missed or duplicate trigger. Drops are counted in e.mutDrops so
// shedding is observable via Stats — the atomic add runs only on the miss
// path; the success path stays one channel send, zero allocs, never blocks.
func (e *Engine) trySubmit(m mutation) bool {
	select {
	case e.mutQ <- m:
		return true
	default:
		e.mutDrops.Add(1)
		return false
	}
}

// Sync blocks until every mutation submitted before Sync has been applied.
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

// refClean is one pending refs-map cleanup item: delete e.refs[id] only if
// the ref still points at the removed entry's slot.
type refClean struct {
	id  AlertID
	idx uint32
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
	now := time.Now()                    // one clock read per batch
	removals := e.refBuf[:0]             // flusher-owned scratch; no per-batch alloc
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
			// Retire the slot (gen-gated, duplicate removals park at most
			// once); refs/live cleanup is deferred to one locked pass below.
			e.slots.retireGen(m.e.idx, m.gen, now)
			removals = append(removals, refClean{id: m.e.id, idx: m.e.idx})
		}
	}
	// One refs pass per batch, not per removal. Same semantics as the
	// per-removal lock it replaces: the match-this-exact-entry check still
	// runs under the lock — a same-ID upsert may have replaced the entry,
	// and only a ref whose idx still equals the removed entry's is deleted.
	e.mu.Lock()
	for _, r := range removals {
		if ref, ok := e.refs[r.id]; ok && ref.e.idx == r.idx {
			delete(e.refs, r.id)
			e.live--
		}
	}
	e.mu.Unlock()
	e.refBuf = removals // keep capacity for the next batch
	for _, p := range bySym {
		p.st.snap.Store(p.next)
		if !p.old.retireRelease() {
			e.parked = append(e.parked, p.old)
		}
	}
	// Release parked snapshots whose readers have drained.
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
// Returns ErrClosed, ErrAlertLimit, ErrSymbolLimit, or a validation error.
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
		// Gated replace: queue the old entry's removal only if we win the
		// CAS to CANCELLED. If the slot is already terminal, fire/Cancel/
		// the reaper already owns a removal — a second one would retire the
		// slot twice and hand the same index to two future alerts.
		s := e.slots.get(ref.e.idx)
		for {
			w := s.Load()
			if st := slotStatus(w); st != StatusActive && st != StatusPaused {
				break // terminal: another path owns the removal
			}
			if s.CompareAndSwap(w, w&^0xff|uint32(StatusCancelled)) {
				replace = mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}
				hasReplace = true
				break
			}
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
	// Capture the handout generation before refs are published and before any
	// blocking submit: once refs are visible, another goroutine can cancel
	// this alert — retire, recycle, reuse — so a later gen read could be the
	// new occupant's. Nothing can retire an unpublished idx, so this is safe.
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

	// Queue order: removal of the old entry lands before the new insert.
	if hasReplace {
		if err := e.submit(replace); err != nil {
			return err
		}
	}
	if err := e.submit(mutation{op: mutInsert, sid: sid, e: ent}); err != nil {
		return err
	}
	// Never-expiring alerts carry the expiryNever sentinel so the integrity
	// sweep can find them.
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
// its removal mutation.
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
			return mutation{}, ErrInvalidTransition
		}
		if s.CompareAndSwap(w, w&^0xff|uint32(want)) {
			return mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}, nil
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
