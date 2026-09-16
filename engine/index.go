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

// submit queues m for the flusher. It never blocks and never drops; the
// only error is a best-effort ErrClosed.
func (e *Engine) submit(m mutation) error {
	select {
	case <-e.done:
		return ErrClosed
	default:
	}
	e.mutQ.enqueue(m)
	return nil
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
// mutClose is the shutdown sentinel — everything enqueued before it is
// applied, then the loop exits. The sentinel can land mid-batch via drain,
// so applyBatch detects it too, not just the dequeue-side check.
func (e *Engine) runFlusher() {
	defer e.flushWG.Done()
	batch := make([]mutation, 0, e.cfg.FlushBatch)
	for {
		m := e.mutQ.dequeue() // parks while idle
		if m.op == mutClose {
			e.applyBatch(batch)
			return
		}
		batch = append(batch, m)
		batch = e.mutQ.drain(batch)
		if e.applyBatch(batch) {
			return
		}
		batch = batch[:0]
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
// It reports whether the mutClose sentinel was in the batch: everything
// before it has been applied, and the flusher must exit.
func (e *Engine) applyBatch(batch []mutation) (stop bool) {
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
		case mutClose:
			// Shutdown sentinel; everything FIFO-before it has been applied.
			// Detected here as well because drain can pull it mid-batch,
			// where the dequeue-side check never looks.
			stop = true
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
	// One refs pass per batch, not per removal: only a ref whose idx still
	// equals the removed entry's is deleted — a same-ID upsert may have
	// replaced it.
	e.mu.Lock()
	for _, r := range removals {
		if ref, ok := e.refs[r.id]; ok && ref.e.idx == r.idx {
			delete(e.refs, r.id)
			e.live--
			// The removed handout is terminal; drop its expiry
			// registration. deregExpiry takes only the expQ mutex, so it
			// is safe under e.mu.
			e.deregExpiry(ref)
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
	return stop
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
	// Gate, intern, and publish under one critical section so that:
	//   - a rejected upsert leaves no intern trace (the Interner never
	//     evicts): the limit is checked before interning, and no concurrent
	//     upsert can fill the limit in between;
	//   - two concurrent upserts of one new ID cannot both take the
	//     new-alert path — the second sees the first's published ref, so an
	//     ID never has two live, independently firable handouts;
	//   - the new ref is published only after its insert (and, on replace,
	//     the old entry's removal) are queued, so every remover observing
	//     the ref enqueues its removal after the insert: FIFO order keeps
	//     remove-after-insert, and a tree entry can never outlive its slot
	//     and alias a recycled occupant.
	// Replaces skip the limit (they never grow live). The lock order below
	// is e.mu → slots/expQ/mutQ; nothing takes e.mu while holding any of
	// those (the flusher releases mutQ before applyBatch, and the reaper
	// releases e.mu before submit).
	dims, ok := normalizeDims(a.Dims, e.dimWidth)
	if !ok {
		return ErrDims
	}
	e.mu.Lock()
	if _, replacing := e.refs[a.ID]; !replacing && e.live >= e.cfg.MaxAlerts {
		e.mu.Unlock()
		return ErrAlertLimit
	}
	sid, err := e.stateFor(a.Symbol)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	var replace mutation
	hasReplace := false
	if ref, ok := e.refs[a.ID]; ok {
		// Gated replace: queue the old entry's removal only if we win the
		// CAS to CANCELLED. A terminal slot means another path already owns
		// the removal — a second one would retire the slot twice.
		s := e.slots.get(ref.e.idx)
		for {
			w := s.Load()
			if st := slotStatus(w); st != StatusActive && st != StatusPaused {
				// Terminal: another path owns the removal, but this
				// goroutine holds the ref, so it owns the expiry dereg —
				// the flusher's pass will miss the replaced ref.
				e.deregExpiry(ref)
				break
			}
			if s.CompareAndSwap(w, w&^0xff|uint32(StatusCancelled)) {
				replace = mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}
				e.deregExpiry(ref)
				hasReplace = true
				break
			}
		}
		delete(e.refs, a.ID)
		e.live--
	}
	idx := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	ent := entry{
		price:     a.TargetPrice,
		id:        a.ID,
		dims:      dims,
		validFrom: a.ValidFrom,
		expires:   a.Expires,
		idx:       idx,
		flags:     makeFlags(a.PriceType, a.Direction),
	}
	ref := &alertRef{sid: sid, e: ent}
	// Queue order: removal of the old entry lands before the new insert, and
	// both land before the ref is published. On a best-effort ErrClosed the
	// old entry is already terminal and its removal is lost — the Close race
	// accepted by design (see Close).
	if hasReplace {
		if err := e.submit(replace); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	if err := e.submit(mutation{op: mutInsert, sid: sid, e: ent}); err != nil {
		e.mu.Unlock()
		return err
	}
	// Only real deadlines are registered; never-expiring alerts have no
	// expiry-table presence at all.
	if a.Expires != 0 {
		e.submitExpiry(expCmd{x: expEntry{expires: a.Expires, ref: ref}})
	}
	e.refs[a.ID] = ref
	e.live++
	e.mu.Unlock()
	return nil
}

// submitExpiry registers an alert with the reaper's expiry table. Lossless
// like submit: never blocks, never drops; the closed check is best-effort.
func (e *Engine) submitExpiry(c expCmd) {
	select {
	case <-e.done:
		return
	default:
	}
	e.pushExp(c)
}

// deregExpiry queues an exact-key expiry-table delete for a ref being
// dropped. No-op for never-expiring alerts, which are never registered.
func (e *Engine) deregExpiry(ref *alertRef) {
	if ref.e.expires == 0 {
		return
	}
	e.pushExp(expCmd{dereg: true, x: expEntry{expires: ref.e.expires, ref: ref}})
}

// pushExp enqueues an expiry command and wakes the reaper with a
// non-blocking token; no wakeup can be lost.
func (e *Engine) pushExp(c expCmd) {
	e.expQ.enqueue(c)
	select {
	case e.expNotify <- struct{}{}:
	default:
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
			e.deregExpiry(ref)
			return mutation{op: mutRemove, sid: ref.sid, e: ref.e, gen: w >> slotGenShift}, nil
		}
	}
}

// Cancel permanently retires an alert.
func (e *Engine) Cancel(id AlertID) error {
	return e.SetStatus(id, StatusCancelled)
}

// Status reports the alert's current lifecycle state. Ledger semantics: an
// Upsert is visible immediately (the ledger is updated synchronously), a
// terminal transition is observable right away via the slot, and the entry
// reports false once the flusher has applied its removal.
func (e *Engine) Status(id AlertID) (Status, bool) {
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return StatusZero, false
	}
	return e.slots.status(ref.e.idx), true
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
