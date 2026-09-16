package engine

import "sync"

// chunkQueue is a lossless MPSC queue: many producers, one consumer — the
// mutation queue's producers are Match goroutines and the control plane;
// the expiry-command queue's are Upsert, the flusher, and the control
// plane. A mutex-guarded, chunked ring of fixed-size buffers with condvar
// wakeup and whole-batch draining.
//
// Storage is a chain of fixed-size chunks recycled through a free list
// rather than one growable slice, so a burst never recopies live entries:
// chunks are allocated only when the queue first reaches a new depth, and
// emptied chunks are recycled. Memory is bounded by outstanding items
// (live alerts plus in-flight upserts, ≤ MaxAlerts). Enqueue never blocks
// and never fails.
type chunkQueue[T any] struct {
	mu      sync.Mutex
	cond    sync.Cond  // wakes a dequeue parked on empty
	head    *qChunk[T] // consumer position
	headIdx int        // next unread slot in head
	tail    *qChunk[T] // producer append position (may == head)
	tailLen int        // populated slots in tail
	free    *qChunk[T] // recycled emptied chunks (singly linked via next)
	count   int        // enqueued-but-undrained items
	chunk   int        // items per chunk; fixed at construction
}

// qChunk is one fixed-size link of the queue. When populated, all chunks
// except the tail are full; only the tail may be partially filled.
type qChunk[T any] struct {
	items []T
	next  *qChunk[T]
}

// mutQueueChunk is the mutation queue's items-per-chunk; not cfg-driven.
const mutQueueChunk = 4096

// expChunk is the expiry-command queue's items-per-chunk.
const expChunk = 4096

func newChunkQueue[T any](chunk int) *chunkQueue[T] {
	q := &chunkQueue[T]{chunk: chunk}
	q.cond.L = &q.mu
	return q
}

// getChunk returns a chunk from the free list or allocates one. Lock held.
func (m *chunkQueue[T]) getChunk() *qChunk[T] {
	if c := m.free; c != nil {
		m.free = c.next
		c.next = nil
		return c
	}
	return &qChunk[T]{items: make([]T, m.chunk)}
}

// popLen reports how many populated slots the head chunk holds. Lock held;
// head != tail implies a full chunk ahead.
func (m *chunkQueue[T]) popLen() int {
	if m.head == m.tail {
		return m.tailLen
	}
	return m.chunk
}

// advanceLocked recycles a fully consumed head chunk. If it is also the
// tail (queue now empty), its positions reset in place; otherwise head
// moves to the next chunk. Lock held.
func (m *chunkQueue[T]) advanceLocked() {
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

// enqueue appends and wakes a parked consumer; the signal runs outside the
// lock.
func (m *chunkQueue[T]) enqueue(v T) {
	m.mu.Lock()
	if m.tail == nil { // fresh queue: first chunk on demand
		m.tail = m.getChunk()
		m.head = m.tail
	}
	if m.tailLen == m.chunk {
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
func (m *chunkQueue[T]) dequeue() T {
	m.mu.Lock()
	for m.count == 0 {
		m.cond.Wait()
	}
	v := m.head.items[m.headIdx]
	m.headIdx++
	m.count--
	if m.headIdx == m.popLen() {
		m.advanceLocked()
	}
	m.mu.Unlock()
	return v
}

// drain takes up to cap(dst) items under one lock and never parks; it
// observes emptiness exactly because enqueue and drain share the mutex.
func (m *chunkQueue[T]) drain(dst []T) []T {
	m.mu.Lock()
	n := m.count
	if room := cap(dst) - len(dst); n > room {
		n = room
	}
	for n > 0 {
		pop := m.popLen()
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
func (m *chunkQueue[T]) pending() int {
	m.mu.Lock()
	n := m.count
	m.mu.Unlock()
	return n
}
