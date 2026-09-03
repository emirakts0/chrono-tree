package engine

// Tick is a market tick. Present is a bitmask of PriceType bits for the
// quote fields the feed actually carries; absent fields are not evaluated.
type Tick struct {
	Symbol  string
	Bid     float64
	Ask     float64
	Mid     float64
	Last    float64
	Present uint8
	TS      int64 // unix nanos
}

// TickAllPresent returns a Present mask covering all four price types.
func TickAllPresent() uint8 { return 1<<priceTypeCount - 1 }

func priceOf(t *Tick, pt PriceType) float64 {
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
// Lock-free and allocation-free; it never blocks and never mutates shared
// trees — firing is CAS-gated per alert, tree removal is deferred.
func (e *Engine) Match(t *Tick) {
	if t.Present == 0 {
		return
	}
	sid, ok := e.syms.Get(t.Symbol)
	if !ok {
		return
	}
	st := &e.states[sid]
	// Pin the snapshot against concurrent release: the flusher may retire
	// it the moment a newer snapshot is published. A pin that loses the
	// race with retirement retries on the fresh snapshot.
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
		// GTE fires when market >= target ⇔ every target <= price:
		// Descend from the tick price downward — all entries qualify. The
		// probe carries the max id so entries exactly at the tick price are
		// not skipped by the id tie-break.
		for en := range snap.trees[treeIndex(pt, DirGTE)].Descend(entryKeyMax(price)) {
			e.fire(sid, &en, price, t.TS)
		}
		// LTE fires when market <= target ⇔ every target >= price:
		// Ascend from the tick price upward — all entries qualify.
		for en := range snap.trees[treeIndex(pt, DirLTE)].Ascend(entryKey(price)) {
			e.fire(sid, &en, price, t.TS)
		}
	}
}

// fire performs the exactly-once transition for one candidate entry: lazy
// validity window, CAS ACTIVE→TRIGGERED, dispatch, deferred removal.
func (e *Engine) fire(sid SymbolID, en *entry, price float64, ts int64) {
	if ts < en.validFrom {
		return
	}
	if en.expires != 0 && ts >= en.expires {
		return
	}
	s := e.slots.get(en.idx)
	for {
		cur := s.Load()
		if Status(cur) != StatusActive {
			return // paused, or already fired/retired by another path
		}
		if s.CompareAndSwap(cur, uint32(StatusTriggered)) {
			break
		}
	}
	e.triggers.TryPush(Trigger{ID: en.id, Price: price, TS: ts})
	e.trySubmit(mutation{op: mutRemove, sid: sid, e: *en})
}
