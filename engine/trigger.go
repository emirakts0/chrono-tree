package engine

import "sync/atomic"

// Trigger is the dispatch payload emitted when an alert fires: 32 bytes.
type Trigger struct {
	ID    AlertID
	Price Price // fired price in base units
	TS    int64
}

// TriggerQueue is the outbound delivery queue: a buffered channel with
// drop-and-count overflow. Producers are the Match hot path, so TryPush never
// blocks or allocates; a slow consumer degrades to counted drops, never to
// backpressure into matching. Consumers poll via Pop/PopBatch or receive
// from C(). The channel is never closed by the engine (a racing producer
// past Engine.Close would panic) — consumers pair C() with their own
// shutdown signaling. Exactly-once firing is guaranteed by the per-alert
// slot CAS that gates every TryPush, not by the queue.
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

// C exposes the underlying channel for blocking receives and select.
func (q *TriggerQueue) C() <-chan Trigger { return q.ch }

// TryPush offers t without blocking; false (and a counted drop) when full.
func (q *TriggerQueue) TryPush(t Trigger) bool {
	select {
	case q.ch <- t:
		return true
	default:
		q.dropped.Add(1)
		return false
	}
}

// Pop removes one trigger without blocking; ok is false when empty.
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

// Len reports the number of queued triggers (advisory).
func (q *TriggerQueue) Len() int { return len(q.ch) }
