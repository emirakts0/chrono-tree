package engine

// Tick is a market tick. Present is a bitmask of PriceType bits for the
// quote fields the feed carries; absent fields are not evaluated.
type Tick struct {
	Symbol  string
	Bid     Price
	Ask     Price
	Mid     Price
	Last    Price
	Present uint8
	TS      int64          // unix nanos
	Dims    [dimMax]uint16 // width real values; trailing slots normalized by Match
}

// TickAllPresent returns a Present mask covering all four price types.
func TickAllPresent() uint8 { return 1<<priceTypeCount - 1 }

func priceOf(t *Tick, pt PriceType) Price {
	switch pt {
	case PriceBid:
		return t.Bid
	case PriceAsk:
		return t.Ask
	case PriceMid:
		return t.Mid
	default:
		return t.Last
	}
}

// Match evaluates a tick against every indexed alert for its symbol.
// Allocation-free and non-blocking; firing is CAS-gated per alert, tree
// removal is deferred to the flusher.
func (e *Engine) Match(t *Tick) {
	if e.closed.Load() {
		return // engine shut down; trees may be released
	}
	if t.Present == 0 {
		return
	}
	// Trailing dim slots are normalized so zero-value ticks work at any
	// width; a sentinel inside width is a malformed tick — dropped (Match
	// cannot return errors).
	dims, ok := normalizeDims(t.Dims, e.dimWidth)
	if !ok {
		return
	}
	sid, ok := e.syms.Get(t.Symbol)
	if !ok {
		return
	}
	// Same guard stateFor has: a symbol interned past MaxSymbols has no
	// snapshot, and indexing states with it would panic.
	if uint64(sid) >= uint64(len(e.states)) {
		return
	}
	st := &e.states[sid]
	// Pin the snapshot against release; a pin that loses the race with
	// retirement retries on the fresh snapshot.
	var snap *snapshot
	for {
		snap = st.snap.Load()
		if snap == nil {
			return
		}
		if snap.pin() {
			break
		}
	}
	defer snap.unpin()
	for pt := PriceType(0); pt < priceTypeCount; pt++ {
		if t.Present&(1<<uint(pt)) == 0 {
			continue
		}
		price := priceOf(t, pt)
		// GTE: descend from the tick price downward — every target <= price
		// qualifies. Scan stops the moment the dim block ends, since entries
		// arrive in descending (dims, price, id) order.
		for en := range snap.trees[treeIndex(pt, DirGTE)].Descend(entryKeyMax(dims, price)) {
			if e.dimWidth > 0 && en.dims != dims {
				break
			}
			e.fire(sid, &en, price, t.TS)
		}
		// LTE: ascend from the tick price upward — every target >= price
		// qualifies. Same dim-block stop, mirrored.
		for en := range snap.trees[treeIndex(pt, DirLTE)].Ascend(entryKey(dims, price)) {
			if e.dimWidth > 0 && en.dims != dims {
				break
			}
			e.fire(sid, &en, price, t.TS)
		}
	}
}

// fire performs the exactly-once transition for one candidate entry: lazy
// validity window, CAS ACTIVE→TRIGGERED, dispatch, deferred removal.
func (e *Engine) fire(sid SymbolID, en *entry, price Price, ts int64) {
	if ts < en.validFrom {
		return
	}
	if en.expires != 0 && ts >= en.expires {
		return
	}
	s := e.slots.get(en.idx)
	var gen uint32
	for {
		cur := s.Load()
		if slotStatus(cur) != StatusActive {
			return // paused, or already fired/retired by another path
		}
		// Full-word CAS preserves the generation: a stale entry from an older
		// generation can never win.
		if s.CompareAndSwap(cur, cur&^0xff|uint32(StatusTriggered)) {
			gen = cur >> slotGenShift
			break
		}
	}
	e.triggers.TryPush(Trigger{ID: en.id, Price: price, TS: ts})
	e.trySubmit(mutation{op: mutRemove, sid: sid, e: *en, gen: gen})
}
