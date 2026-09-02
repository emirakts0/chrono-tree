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
		p.old.release()
	}
	for _, d := range syncs {
		close(d)
	}
}
