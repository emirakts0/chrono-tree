package engine

import "sync"

// SymbolID is a dense identifier indexing the Engine's symbolState array.
type SymbolID uint32

// Interner maps symbol strings to dense SymbolIDs. Interning is rare
// (new symbols only); Get is hot-path.
type Interner struct {
	mu    sync.RWMutex
	ids   map[string]SymbolID
	names []string
}

func NewInterner() *Interner {
	return &Interner{ids: make(map[string]SymbolID)}
}

// Get returns the id of an already-interned symbol.
func (in *Interner) Get(s string) (SymbolID, bool) {
	in.mu.RLock()
	defer in.mu.RUnlock()
	id, ok := in.ids[s]
	return id, ok
}

// Intern returns the id of s, assigning a new one on first sight.
func (in *Interner) Intern(s string) SymbolID {
	in.mu.RLock()
	id, ok := in.ids[s]
	in.mu.RUnlock()
	if ok {
		return id
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if id, ok := in.ids[s]; ok {
		return id
	}
	id = SymbolID(len(in.names))
	in.ids[s] = id
	in.names = append(in.names, s)
	return id
}

// Name returns the symbol string for id.
func (in *Interner) Name(id SymbolID) string {
	in.mu.RLock()
	defer in.mu.RUnlock()
	return in.names[id]
}
