package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btype"
)

// Sentinel errors returned by the control plane.
var (
	ErrClosed            = errors.New("chrono-tree: engine closed")
	ErrNotFound          = errors.New("chrono-tree: alert not found")
	ErrInvalidStatus     = errors.New("chrono-tree: invalid status value")
	ErrInvalidTransition = errors.New("chrono-tree: invalid status transition")
	ErrSymbolLimit       = errors.New("chrono-tree: symbol limit exceeded")
	ErrAlertLimit        = errors.New("chrono-tree: max alerts exceeded")
)

// Config bounds all preallocated structures. See DefaultConfig.
type Config struct {
	MaxSymbols         uint32        // fixed symbolState array size
	MaxAlerts          uint64        // live alert cap
	MutationQueueDepth int           // bounded mutation queue
	FlushBatch         int           // max ops applied per flush cycle
	RingSize           int           // trigger ring capacity (rounded to pow2)
	ReaperInterval     time.Duration // expiry sweep + slot recycle period
}

func DefaultConfig() Config {
	return Config{
		MaxSymbols:         1 << 16,
		MaxAlerts:          10_000_000,
		MutationQueueDepth: 4096,
		FlushBatch:         256,
		RingSize:           1 << 16,
		ReaperInterval:     time.Second,
	}
}

// AlertSpec is the validated control-plane input for Upsert.
type AlertSpec struct {
	ID             AlertID
	Symbol         string
	PriceType      PriceType
	Direction      Direction
	TargetPrice    float64
	ValidFrom      int64 // unix nanos
	Expires        int64 // unix nanos; 0 = never
	AutoDeactivate bool
	Meta           AlertMeta // cold data, stored verbatim
}

func (a *AlertSpec) validate() error {
	if a.ID == (AlertID{}) {
		return errors.New("chrono-tree: alert id is zero")
	}
	if a.Symbol == "" {
		return errors.New("chrono-tree: symbol is empty")
	}
	if a.PriceType >= priceTypeCount {
		return errors.New("chrono-tree: invalid price type")
	}
	if a.Direction > DirLTE {
		return errors.New("chrono-tree: invalid direction")
	}
	if a.Expires != 0 && a.Expires <= a.ValidFrom {
		return errors.New("chrono-tree: expires at or before valid-from")
	}
	return nil
}

// AlertMeta is the cold record: everything the notification pipeline needs
// after a trigger fires. Never touched by Match.
type AlertMeta struct {
	ID          AlertID
	Symbol      string
	UserID      string
	Segment     string
	Channels    []string
	Notes       string
	CreatedAt   int64 // unix nanos
	PriceType   PriceType
	Direction   Direction
	TargetPrice float64
}

// Stats is a point-in-time engine snapshot for observability.
type Stats struct {
	Live            uint64
	DroppedTriggers uint64
}

// alertRef locates an alert's index structures for control-plane ops.
type alertRef struct {
	sid SymbolID
	e   entry
}

// mutation is a queued index change, applied by the flusher (Task 8).
type mutation struct {
	op   mutOp
	sid  SymbolID
	e    entry
	done chan struct{} // op == mutSync: closed once applied
}

type mutOp uint8

const (
	mutInsert mutOp = iota
	mutRemove
	mutSync
)

// expEntry registers an alert with the reaper's expiry table (Task 11).
type expEntry struct {
	expires int64
	sid     SymbolID
	e       entry
}

// Engine is the alert evaluation engine. Zero network, zero I/O.
type Engine struct {
	cfg    Config
	syms   *Interner
	states []symbolState // fixed len MaxSymbols, indexed by SymbolID
	slots  *slotArena

	mutQ     chan mutation
	expQ     chan expEntry
	triggers *TriggerQueue
	expiry   btype.Table[expEntry] // owned by the reaper only

	mu   sync.Mutex // guards refs, meta, live
	refs map[AlertID]*alertRef
	meta map[AlertID]*AlertMeta
	live uint64

	done    chan struct{}
	flushWG sync.WaitGroup
	reapWG  sync.WaitGroup
	closed  atomic.Bool
}

func New(cfg Config) *Engine {
	e := &Engine{
		cfg:      cfg,
		syms:     NewInterner(),
		states:   make([]symbolState, cfg.MaxSymbols),
		slots:    newSlotArena(cfg.MaxAlerts),
		mutQ:     make(chan mutation, cfg.MutationQueueDepth),
		expQ:     make(chan expEntry, cfg.MutationQueueDepth),
		triggers: NewTriggerQueue(cfg.RingSize),
		refs:     make(map[AlertID]*alertRef),
		meta:     make(map[AlertID]*AlertMeta),
		done:     make(chan struct{}),
	}
	e.flushWG.Add(1)
	go e.runFlusher()
	return e
}

// Close stops the flusher and reaper and releases all snapshots. Idempotent.
func (e *Engine) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	close(e.done)
	e.flushWG.Wait()
	e.reapWG.Wait()
	for i := range e.states {
		if s := e.states[i].snap.Load(); s != nil {
			s.release()
			e.states[i].snap.Store(nil)
		}
	}
}

// Triggers exposes the trigger ring for downstream consumption.
func (e *Engine) Triggers() *TriggerQueue { return e.triggers }

// Stats reports live alerts and dropped triggers.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	live := e.live
	e.mu.Unlock()
	return Stats{Live: live, DroppedTriggers: e.triggers.Dropped()}
}
