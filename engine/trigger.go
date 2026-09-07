package engine

import "sync/atomic"

// Trigger is the dispatch payload emitted when an alert fires: 32 bytes.
type Trigger struct {
	ID    AlertID
	Price Price // fired price in base units
	TS    int64
}

// TriggerQueue is the engine's outbound delivery queue: a buffered channel
// with drop-and-count overflow semantics.
//
// Producers are the Match hot path, so TryPush never blocks and never
// allocates: it is a select-default send that counts a drop when the buffer
// is full. A slow consumer degrades to counted drops, never to
// backpressure into matching.
//
// Consumers may poll with Pop/PopBatch or receive from C(), which supports
// blocking receives and select against other event sources. The channel is
// never closed by the engine — a racing producer past Engine.Close would
// panic on a closed channel — so consumers should pair C() with their own
// shutdown signaling.
//
// Synchronization is the channel's runtime-managed lock. It replaces an
// earlier hand-rolled Vyukov MPMC ring whose contended-CAS/retry enqueue
// degraded under many producers (negative scaling past 4 threads on
// 12-thread hardware); the channel's short critical section measured flat
// ~47 ns/push at 12 threads, ~5x the ring's aggregate throughput.
// Exactly-once firing is unaffected: it is guaranteed by the per-alert slot
// CAS that gates every TryPush, not by the queue, and a channel's
// send/receive pairing provides the happens-before edge that publishes each
// Trigger payload.
type TriggerQueue struct {
	ch      chan Trigger
	dropped atomic.Uint64
}

// NewTriggerQueue builds a queue holding up to capacity undelivered
// triggers.
func NewTriggerQueue(capacity int) *TriggerQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &TriggerQueue{ch: make(chan Trigger, capacity)}
}

// C exposes the underlying channel for blocking receives and select. It is
// never closed.
func (q *TriggerQueue) C() <-chan Trigger { return q.ch }

// TryPush offers t without blocking. It returns false — and counts a drop —
// when the queue is full. Safe for any number of concurrent producers.
func (q *TriggerQueue) TryPush(t Trigger) bool {
	select {
	case q.ch <- t:
		return true
	default:
		q.dropped.Add(1)
		return false
	}
}

// Pop removes one trigger without blocking; ok is false when the queue is
// empty. Safe for any number of concurrent consumers.
func (q *TriggerQueue) Pop() (Trigger, bool) {
	select {
	case t := <-q.ch:
		return t, true
	default:
		return Trigger{}, false
	}
}

// PopBatch drains up to len(dst) triggers into dst, returning the count.
func (q *TriggerQueue) PopBatch(dst []Trigger) int {
	n := 0
	for n < len(dst) {
		t, ok := q.Pop()
		if !ok {
			break
		}
		dst[n] = t
		n++
	}
	return n
}

// Dropped reports triggers rejected because the queue was full.
func (q *TriggerQueue) Dropped() uint64 { return q.dropped.Load() }

// Len reports the number of queued, undelivered triggers (advisory).
func (q *TriggerQueue) Len() int { return len(q.ch) }
