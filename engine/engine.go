package engine

import (
	"errors"
	"fmt"
	"math"
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
	ErrDims              = errors.New("chrono-tree: sentinel dim value inside configured width")
)

// Config bounds all preallocated structures.
type Config struct {
	MaxSymbols         uint32        // fixed symbolState array size
	MaxAlerts          uint64        // live alert cap
	MutationQueueDepth int           // bounded mutation queue
	FlushBatch         int           // max ops applied per flush cycle
	TriggerQueueSize   int           // trigger queue capacity
	ReaperInterval     time.Duration // expiry sweep + slot recycle period
	IntegrityEvery     int           // integrity sweep cadence, in reaper ticks (<=0 → default)
	Dims               []string      // positional dimension names; slot i = Dims[i]; max 8
}

// defaultIntegrityEvery is the integrity sweep cadence in reaper ticks; a
// leaked entry is cleaned within this × ReaperInterval + flush lag.
const defaultIntegrityEvery = 30

func DefaultConfig() Config {
	return Config{
		MaxSymbols:         1 << 16,
		MaxAlerts:          10_000_000,
		MutationQueueDepth: 4096,
		FlushBatch:         256,
		TriggerQueueSize:   1 << 16,
		ReaperInterval:     time.Second,
		IntegrityEvery:     defaultIntegrityEvery,
	}
}

// AlertSpec is the validated control-plane input for Upsert.
type AlertSpec struct {
	ID             AlertID
	Symbol         string
	PriceType      PriceType
	Direction      Direction
	TargetPrice    Price // base units
	ValidFrom      int64 // unix nanos
	Expires        int64 // unix nanos; 0 = never
	AutoDeactivate bool
	Dims           [dimMax]uint16 // width real values; trailing slots normalized
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

// mutation is a queued index change, applied by the flusher.
type mutation struct {
	op   mutOp
	sid  SymbolID
	e    entry
	gen  uint32        // for mutRemove: handout generation of e.idx
	done chan struct{} // for mutSync: closed once applied
}

type mutOp uint8

const (
	mutInsert mutOp = iota
	mutRemove
	mutSync
)

// expEntry registers an alert with the reaper's expiry table. gen is the slot
// generation at handout: expiry entries outlive the recycle grace, so sweeps
// must reject entries whose slot has been recycled (gen moved). Every alert
// is registered, never-expiring ones with the expiryNever sentinel: the table
// doubles as the integrity sweep's registry of live alerts.
type expEntry struct {
	expires int64
	sid     SymbolID
	e       entry
	gen     uint32
}

// expiryNever is the never-due deadline sentinel: the expiry sweep skips it;
// only the integrity sweep ever acts on sentinel entries.
const expiryNever = math.MaxInt64

// Engine is the alert evaluation engine. Zero network, zero I/O.
type Engine struct {
	cfg      Config
	dimWidth uint8 // len(cfg.Dims); fixed for the engine's lifetime
	syms     *Interner
	states   []symbolState // fixed len MaxSymbols, indexed by SymbolID
	slots    *slotArena

	mutQ     chan mutation
	expQ     chan expEntry
	triggers *TriggerQueue
	expiry   btype.Table[expEntry] // owned by the reaper only
	parked   []*snapshot           // flusher-owned: retired snapshots awaiting reader drain

	mu   sync.Mutex // guards refs, live
	refs map[AlertID]*alertRef
	live uint64

	done    chan struct{}
	flushWG sync.WaitGroup
	reapWG  sync.WaitGroup
	closed  atomic.Bool
}

func New(cfg Config) *Engine {
	if cfg.IntegrityEvery <= 0 {
		cfg.IntegrityEvery = defaultIntegrityEvery
	}
	if len(cfg.Dims) > dimMax {
		panic(fmt.Sprintf("chrono-tree: at most %d dims, got %d", dimMax, len(cfg.Dims)))
	}
	for _, n := range cfg.Dims {
		if n == "" {
			panic("chrono-tree: dim names must be non-empty")
		}
	}
	e := &Engine{
		cfg:      cfg,
		dimWidth: uint8(len(cfg.Dims)),
		syms:     NewInterner(),
		states:   make([]symbolState, cfg.MaxSymbols),
		slots:    newSlotArena(cfg.MaxAlerts),
		mutQ:     make(chan mutation, cfg.MutationQueueDepth),
		expQ:     make(chan expEntry, cfg.MutationQueueDepth),
		triggers: NewTriggerQueue(cfg.TriggerQueueSize),
		refs:     make(map[AlertID]*alertRef),
		done:     make(chan struct{}),
	}
	e.flushWG.Add(1)
	go e.runFlusher()
	e.reapWG.Add(1)
	go e.runReaper()
	return e
}

// Close stops the flusher and reaper and releases all snapshots. Idempotent.
// It synchronously drains readers of each snapshot (current and parked)
// before freeing its trees, so it can block as long as a Match scan holds a
// pin — never call it from a latency-sensitive path. Callers must stop
// submitting before Close; a residual race window is accepted by design.
func (e *Engine) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	close(e.done)
	e.flushWG.Wait()
	e.reapWG.Wait()
	for i := range e.states {
		if s := e.states[i].snap.Load(); s != nil {
			s.shutdownRelease()
			e.states[i].snap.Store(nil)
		}
	}
	// Parked snapshots need the same reader drain before their trees are freed.
	for _, s := range e.parked {
		s.shutdownRelease()
	}
	e.parked = nil
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
