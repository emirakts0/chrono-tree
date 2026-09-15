package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
)

// benchMutQChan measures the current channel-backed mutation queue in the
// fire-removal shape: producers send, one consumer batches like runFlusher
// (first blocking recv, then non-blocking drain to the batch cap). Sends
// block when full — the channel's only non-drop option; the hot path today
// would drop instead, so this is the channel's best case for comparison.
func benchMutQChan(b *testing.B, producers int) {
	const depth = 4096 // DefaultConfig().MutationQueueDepth
	q := make(chan mutation, depth)
	batch := make([]mutation, 0, 256) // DefaultConfig().FlushBatch
	go func() {                       // consumer: abandoned at bench end, like the engine's flusher
		for {
			batch = append(batch[:0], <-q)
		drain:
			for len(batch) < cap(batch) {
				select {
				case m := <-q:
					batch = append(batch, m)
				default:
					break drain
				}
			}
		}
	}()
	per := b.N/producers + 1 // total sends == b.N, so ns/op is per item
	var wgp sync.WaitGroup
	for p := 0; p < producers; p++ {
		wgp.Add(1)
		go func() {
			defer wgp.Done()
			m := mutation{op: mutRemove, sid: 1, gen: 1}
			for i := 0; i < per; i++ {
				q <- m
			}
		}()
	}
	wgp.Wait()
	// Let the consumer finish the tail so sends aren't credited unfairly.
	for len(q) > 0 {
		runtime.Gosched()
	}
}

func BenchmarkMutQChan(b *testing.B)          { benchMutQChan(b, 1) }
func BenchmarkMutQChanContended(b *testing.B) { benchMutQChan(b, 4) }

// benchUMPSCQueue wraps xsync.UMPSCQueue with a bench-local pending counter
// so the rejected Gate-1 mutQueue design stays reproducible: no production
// type is involved, the counter is bookkeeping the underlying queue lacks
// (it offers no TryDequeue, so drain needs a claim count to stop at empty).
type benchUMPSCQueue struct {
	q       *xsync.UMPSCQueue[mutation]
	pending atomic.Int64
}

func (b *benchUMPSCQueue) enqueue(v mutation) {
	b.pending.Add(1)
	b.q.Enqueue(v)
}

func (b *benchUMPSCQueue) dequeue() mutation {
	v := b.q.Dequeue()
	b.pending.Add(-1)
	return v
}

func (b *benchUMPSCQueue) drain(dst []mutation) []mutation {
	for len(dst) < cap(dst) && b.pending.Load() > 0 {
		dst = append(dst, b.dequeue())
	}
	return dst
}

// benchMutQUMPSC mirrors benchMutQChan on the rejected UMPSC design: same
// producers, same batching shape (first blocking dequeue, then
// pending-bounded drain).
func benchMutQUMPSC(b *testing.B, producers int) {
	q := &benchUMPSCQueue{q: xsync.NewUMPSCQueue[mutation]()}
	batch := make([]mutation, 0, 256)
	go func() { // consumer: abandoned at bench end, like the engine's flusher
		for {
			batch = append(batch[:0], q.dequeue())
			batch = q.drain(batch)
		}
	}()
	per := b.N/producers + 1 // total sends == b.N, so ns/op is per item
	var wgp sync.WaitGroup
	for p := 0; p < producers; p++ {
		wgp.Add(1)
		go func() {
			defer wgp.Done()
			m := mutation{op: mutRemove, sid: 1, gen: 1}
			for i := 0; i < per; i++ {
				q.enqueue(m)
			}
		}()
	}
	wgp.Wait()
	for q.pending.Load() > 0 {
		runtime.Gosched()
	}
}

func BenchmarkMutQUMPSC(b *testing.B)          { benchMutQUMPSC(b, 1) }
func BenchmarkMutQUMPSCContended(b *testing.B) { benchMutQUMPSC(b, 4) }

// benchMutQMutex mirrors benchMutQUMPSC on the adopted mutQueue: same
// producers, same batching shape. Tail-drain spin uses the locked pending
// read (exact under the same mutex).
func benchMutQMutex(b *testing.B, producers int) {
	q := newMutQueue()
	batch := make([]mutation, 0, 256)
	go func() { // consumer: abandoned at bench end, like the engine's flusher
		for {
			batch = append(batch[:0], q.dequeue())
			batch = q.drain(batch)
		}
	}()
	per := b.N/producers + 1 // total sends == b.N, so ns/op is per item
	var wgp sync.WaitGroup
	for p := 0; p < producers; p++ {
		wgp.Add(1)
		go func() {
			defer wgp.Done()
			m := mutation{op: mutRemove, sid: 1, gen: 1}
			for i := 0; i < per; i++ {
				q.enqueue(m)
			}
		}()
	}
	wgp.Wait()
	for q.pending() > 0 {
		runtime.Gosched()
	}
}

func BenchmarkMutQMutex(b *testing.B)          { benchMutQMutex(b, 1) }
func BenchmarkMutQMutexContended(b *testing.B) { benchMutQMutex(b, 4) }
