package engine

import (
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

// SymbolID is a dense identifier indexing the Engine's symbolState array.
type SymbolID uint32

// Interner maps symbol strings to dense SymbolIDs. Interning is rare; Get is
// the hot path and lock-free.
type Interner struct {
	ids   *xsync.Map[string, SymbolID]
	names *xsync.Map[SymbolID, string]
	next  atomic.Uint32
}

func NewInterner() *Interner {
	return &Interner{
		ids:   xsync.NewMap[string, SymbolID](),
		names: xsync.NewMap[SymbolID, string](),
	}
}

// Get returns the id of an already-interned symbol.
func (in *Interner) Get(s string) (SymbolID, bool) {
	return in.ids.Load(s)
}

// Intern returns the id of s, assigning a new one on first sight. The
// LoadOrCompute constructor runs once per absent key, so each id is unique.
func (in *Interner) Intern(s string) SymbolID {
	id, _ := in.ids.LoadOrCompute(s, func() (SymbolID, bool) {
		n := SymbolID(in.next.Add(1) - 1)
		in.names.Store(n, s)
		return n, false // false: do not cancel the insertion
	})
	return id
}

// Name returns the symbol string for id, or "" if id was never assigned.
func (in *Interner) Name(id SymbolID) string {
	n, _ := in.names.Load(id)
	return n
}
