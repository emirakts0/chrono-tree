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

// Config bounds all preallocated structures. See DefaultConfig.
type Config struct {
	MaxSymbols         uint32        // fixed symbolState array size
	MaxAlerts          uint64        // live alert cap
	MutationQueueDepth int           // bounded mutation queue
	FlushBatch         int           // max ops applied per flush cycle
	RingSize           int           // trigger ring capacity (rounded to pow2)
	ReaperInterval     time.Duration // expiry sweep + slot recycle period
	IntegrityEvery     int           // integrity sweep cadence, in reaper ticks (<=0 → default)
	Dims               []string      // positional dimension names; slot i = Dims[i]; max 8. Width is len(Dims).
}

// defaultIntegrityEvery is the integrity sweep cadence in reaper ticks. The
// sweep re-submits removals for TRIGGERED entries whose original enqueue was
// lost (fire's trySubmit is best-effort), so a leaked entry is cleaned within
// IntegrityEvery × ReaperInterval + flush lag.
const defaultIntegrityEvery = 30

func DefaultConfig() Config {
	return Config{
		MaxSymbols:         1 << 16,
		MaxAlerts:          10_000_000,
		MutationQueueDepth: 4096,
		FlushBatch:         256,
		RingSize:           1 << 16,
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
	TargetPrice    Price // base units; scale is the caller's contract
	ValidFrom      int64 // unix nanos
	Expires        int64 // unix nanos; 0 = never
	AutoDeactivate bool
	Dims           [dimMax]uint16 // width real values; trailing slots normalized by the engine
	Meta           AlertMeta      // cold data, stored verbatim
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
	TargetPrice Price
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
	gen  uint32        // op == mutRemove: handout generation of e.idx, gates retireGen
	done chan struct{} // op == mutSync: closed once applied
}

type mutOp uint8

const (
	mutInsert mutOp = iota
	mutRemove
	mutSync
)

// expEntry registers an alert with the reaper's expiry table (Task 11).
// gen is the slot generation at handout: expiry entries outlive the recycle
// grace, so sweep must reject entries whose slot has since been recycled and
// reused (its generation moved) — a fresh word alone can't tell stale from
// current, the generation captured here can.
//
// EVERY alert is registered, never-expiring ones with the expiryNever
// sentinel: the table doubles as the integrity sweep's registry of live
// alerts (see reaper.integrity).
type expEntry struct {
	expires int64
	sid     SymbolID
	e       entry
	gen     uint32
}

// expiryNever is the never-due deadline sentinel for alerts without Expires:
// the expiry sweep skips it (expires > now for any real now); only the
// integrity sweep ever acts on sentinel entries.
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
	parked   []*snapshot           // flusher-owned: retired snapshots pinned by readers, released once drained

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
		triggers: NewTriggerQueue(cfg.RingSize),
		refs:     make(map[AlertID]*alertRef),
		meta:     make(map[AlertID]*AlertMeta),
		done:     make(chan struct{}),
	}
	e.flushWG.Add(1)
	go e.runFlusher()
	e.reapWG.Add(1)
	go e.runReaper()
	return e
}

// Close stops the flusher and reaper and releases all snapshots. Idempotent.
// It waits for in-flight Match scans to finish before freeing any tree:
// each snapshot (current and parked) is shut down via a synchronous
// mark-retired + spin-until-readers-drain, so a concurrent Match either
// completes on a valid snapshot or observes closed/nil and returns. That
// wait spins until the longest in-flight scan drains, so Close can block
// for as long as a reader holds a pin — never call it from a
// latency-sensitive path.
// Lifecycle contract: callers must stop submitting before calling Close; a
// residual race window between a final submit and Close is accepted by design.
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
	// Parked snapshots need the same reader drain: their trees must not be
	// freed under a scan that pinned them before retirement either.
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
