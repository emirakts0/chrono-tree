package engine

import "sync"

// mutQueue is the engine's mutation queue: many producers (Match goroutines
// and the control plane), one consumer (the flusher). A mutex-guarded,
// chunked ring of fixed-size buffers with condvar wakeup and whole-batch
// draining.
//
// The storage is a chain of fixed-size chunks recycled through a free list
// instead of a single growable slice: under sustained fire load the growable
// buffer's append reallocation recopied the whole queue (measured 285MB /
// 10.7% of allocations in Sweep1MDims/g12, with B/op up to 4x baseline at
// g12 parallelism). Chunks are allocated only when the queue first reaches a
// new depth; emptied chunks are recycled, never recopied, so steady-state
// enqueue allocates nothing.
//
// Memory is bounded by the high-water chunk count, which is bounded in turn
// by outstanding items (live alerts plus in-flight upserts, ≤ MaxAlerts).
// Enqueue never blocks and never fails; bursts are absorbed by chunk growth.
// Chosen over xsync.UMPSCQueue on measurement (Gate 1, see the measurements
// doc): draining a whole batch under a single lock acquisition beat both the
// channel baseline and UMPSC by 2-3x.
type mutQueue struct {
	mu      sync.Mutex
	cond    sync.Cond // wakes a dequeue parked on empty
	head    *mutChunk // consumer position
	headIdx int       // next unread slot in head
	tail    *mutChunk // producer append position (may == head)
	tailLen int       // populated slots in tail
	free    *mutChunk // recycled emptied chunks (singly linked via next)
	count   int       // enqueued-but-undrained items
}

// mutChunk is one fixed-size link of the queue. When populated, all chunks
// except the tail are full; only the tail may be partially filled.
type mutChunk struct {
	items []mutation // len == mutQueueChunk
	next  *mutChunk
}

// mutQueueChunk is the number of entries per chunk, sized at DefaultConfig's
// MutationQueueDepth scale; it is not cfg-driven (expQ owns that field).
const mutQueueChunk = 4096

func newMutQueue() *mutQueue {
	q := &mutQueue{}
	q.cond.L = &q.mu
	return q
}

// getChunk returns a chunk from the free list or allocates one. Lock held.
func (m *mutQueue) getChunk() *mutChunk {
	if c := m.free; c != nil {
		m.free = c.next
		c.next = nil
		return c
	}
	return &mutChunk{items: make([]mutation, mutQueueChunk)}
}

// advanceLocked is called when the head chunk is fully consumed. If it is
// also the tail (queue now empty), its positions reset in place for reuse;
// otherwise it is recycled to the free list and head moves to the next chunk,
// which roll-time linking guarantees is already there. Lock held.
func (m *mutQueue) advanceLocked() {
	if m.head == m.tail {
		m.headIdx = 0
		m.tailLen = 0
		return
	}
	c := m.head
	m.head = c.next
	m.headIdx = 0
	c.next = m.free
	m.free = c
}

// enqueue appends and wakes a parked consumer. Signal runs outside the
// lock: it does not require L held, and the mutex is the batching
// bottleneck.
func (m *mutQueue) enqueue(v mutation) {
	m.mu.Lock()
	if m.tail == nil { // fresh queue: first chunk on demand
		m.tail = m.getChunk()
		m.head = m.tail
	}
	if m.tailLen == mutQueueChunk {
		next := m.getChunk()
		m.tail.next = next // link eagerly: dequeue never sees a torn chain
		m.tail = next
		m.tailLen = 0
	}
	m.tail.items[m.tailLen] = v
	m.tailLen++
	m.count++
	m.mu.Unlock()
	m.cond.Signal()
}

// dequeue receives one item, blocking while empty (single consumer only).
func (m *mutQueue) dequeue() mutation {
	m.mu.Lock()
	for m.count == 0 {
		m.cond.Wait()
	}
	v := m.head.items[m.headIdx]
	m.headIdx++
	m.count--
	pop := mutQueueChunk // head != tail implies a full chunk
	if m.head == m.tail {
		pop = m.tailLen
	}
	if m.headIdx == pop {
		m.advanceLocked()
	}
	m.mu.Unlock()
	return v
}

// drain takes up to cap(dst) items under one lock and never parks: it
// observes emptiness exactly because enqueue and drain share the mutex.
// Copying into dst is the flusher's pre-existing batch copy, not chunk
// recopying — live entries never move between chunks.
func (m *mutQueue) drain(dst []mutation) []mutation {
	m.mu.Lock()
	n := m.count
	if room := cap(dst) - len(dst); n > room {
		n = room
	}
	for n > 0 {
		pop := mutQueueChunk // head != tail implies a full chunk
		if m.head == m.tail {
			pop = m.tailLen
		}
		c := pop - m.headIdx
		if c > n {
			c = n
		}
		dst = append(dst, m.head.items[m.headIdx:m.headIdx+c]...)
		m.headIdx += c
		m.count -= c
		n -= c
		if m.headIdx == pop {
			m.advanceLocked()
		}
	}
	m.mu.Unlock()
	return dst
}

// pending reports the number of enqueued-but-undrained items (diagnostics
// and tests).
func (m *mutQueue) pending() int {
	m.mu.Lock()
	n := m.count
	m.mu.Unlock()
	return n
}
