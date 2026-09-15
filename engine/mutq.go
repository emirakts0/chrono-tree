package engine

import "sync"

// mutQueue is the engine's mutation queue: many producers (Match goroutines
// and the control plane), one consumer (the flusher). A mutex-guarded buffer
// with condvar wakeup and whole-batch draining.
//
// Enqueue never blocks and never fails: bursts are absorbed by growth,
// bounded by live alerts plus in-flight upserts (≤ MaxAlerts); the buffer
// is recycled once fully consumed, so steady-state enqueue allocates
// nothing. Chosen over xsync.UMPSCQueue on measurement (Gate 1, see the
// measurements doc): draining a whole batch under a single lock acquisition
// beat both the channel baseline and UMPSC by 2-3x.
type mutQueue struct {
	mu   sync.Mutex
	cond sync.Cond // wakes a dequeue parked on empty
	buf  []mutation
	head int
}

func newMutQueue() *mutQueue {
	q := &mutQueue{buf: make([]mutation, 0, 4096)}
	q.cond.L = &q.mu
	return q
}

// enqueue appends and wakes a parked consumer. Signal runs outside the
// lock: it does not require L held, and the mutex is the batching
// bottleneck.
func (m *mutQueue) enqueue(v mutation) {
	m.mu.Lock()
	m.buf = append(m.buf, v)
	m.mu.Unlock()
	m.cond.Signal()
}

// dequeue receives one item, blocking while empty (single consumer only).
func (m *mutQueue) dequeue() mutation {
	m.mu.Lock()
	for len(m.buf)-m.head == 0 {
		m.cond.Wait()
	}
	v := m.buf[m.head]
	m.head++
	if m.head == len(m.buf) {
		m.buf = m.buf[:0] // recycle: capacity retained, steady state alloc-free
		m.head = 0
	}
	m.mu.Unlock()
	return v
}

// drain takes up to cap(dst) items under one lock and never parks: it
// observes emptiness exactly because enqueue and drain share the mutex.
func (m *mutQueue) drain(dst []mutation) []mutation {
	m.mu.Lock()
	n := len(m.buf) - m.head
	if n > cap(dst)-len(dst) {
		n = cap(dst) - len(dst)
	}
	dst = append(dst, m.buf[m.head:m.head+n]...)
	m.head += n
	if m.head == len(m.buf) {
		m.buf = m.buf[:0]
		m.head = 0
	}
	m.mu.Unlock()
	return dst
}

// pending reports the number of enqueued-but-undrained items (diagnostics
// and tests).
func (m *mutQueue) pending() int {
	m.mu.Lock()
	n := len(m.buf) - m.head
	m.mu.Unlock()
	return n
}
