package engine

import "sync/atomic"

// Trigger is the dispatch payload emitted when an alert fires: 32 bytes.
type Trigger struct {
	ID    AlertID
	Price float64
	TS    int64
}

type triggerCell struct {
	seq atomic.Uint64
	val Trigger
}

// TriggerQueue is a bounded lock-free MPMC ring buffer (Vyukov design).
// TryPush never blocks: on a full queue it returns false and bumps Dropped.
type TriggerQueue struct {
	buf        []triggerCell
	mask       uint64
	enqueuePos atomic.Uint64
	dequeuePos atomic.Uint64
	dropped    atomic.Uint64
}

// NewTriggerQueue builds a queue; capacity is rounded up to a power of two.
func NewTriggerQueue(capacity int) *TriggerQueue {
	if capacity < 2 {
		capacity = 2
	}
	p := 1
	for p < capacity {
		p <<= 1
	}
	q := &TriggerQueue{buf: make([]triggerCell, p), mask: uint64(p - 1)}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

func (q *TriggerQueue) TryPush(t Trigger) bool {
	for {
		pos := q.enqueuePos.Load()
		c := &q.buf[pos&q.mask]
		seq := c.seq.Load()
		switch dif := int64(seq) - int64(pos); {
		case dif == 0:
			if q.enqueuePos.CompareAndSwap(pos, pos+1) {
				c.val = t
				c.seq.Store(pos + 1)
				return true
			}
		case dif < 0: // full
			q.dropped.Add(1)
			return false
		default: // another producer claimed the slot; retry
		}
	}
}

func (q *TriggerQueue) Pop() (Trigger, bool) {
	for {
		pos := q.dequeuePos.Load()
		c := &q.buf[pos&q.mask]
		seq := c.seq.Load()
		switch dif := int64(seq) - int64(pos+1); {
		case dif == 0:
			if q.dequeuePos.CompareAndSwap(pos, pos+1) {
				v := c.val
				c.seq.Store(pos + q.mask + 1)
				return v, true
			}
		case dif < 0: // empty
			return Trigger{}, false
		default: // another consumer claimed the slot; retry
		}
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
