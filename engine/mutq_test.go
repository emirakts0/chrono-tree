package engine

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMutQueueDeliversAllUnderBurst is the lossless contract: every enqueue
// from every producer must come out exactly once, and pending must return
// to zero once drained — no drop path exists.
func TestMutQueueDeliversAllUnderBurst(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	const producers, each = 8, 10_000
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				q.enqueue(mutation{op: mutRemove, sid: SymbolID(p)})
			}
		}(p)
	}
	got := make(map[SymbolID]int)
	batch := make([]mutation, 0, 256)
	total := 0
	for total < producers*each {
		batch = q.drain(batch)
		if len(batch) == 0 {
			runtime.Gosched()
			continue
		}
		for _, m := range batch {
			got[m.sid]++
			total++
		}
		batch = batch[:0]
	}
	wg.Wait()
	for p := 0; p < producers; p++ {
		if got[SymbolID(p)] != each {
			t.Fatalf("producer %d: delivered %d, want %d", p, got[SymbolID(p)], each)
		}
	}
	if n := q.pending(); n != 0 {
		t.Fatalf("pending = %d after full drain, want 0", n)
	}
}

// TestMutQueueRollsChunksAndRecycles pins the chunked-buffer path: bursts
// larger than one chunk must roll across chunk boundaries losslessly, and
// after a full drain the recycled free list must serve a second burst
// (steady-state alloc-free) without corruption or loss.
func TestMutQueueRollsChunksAndRecycles(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	const producers = 4
	each := 3*mutQueueChunk + 17 // crosses several chunk boundaries

	runBurst := func(tag string) {
		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				for i := 0; i < each; i++ {
					q.enqueue(mutation{op: mutRemove, sid: SymbolID(p)})
				}
			}(p)
		}
		got := make(map[SymbolID]int)
		batch := make([]mutation, 0, 128) // small batch: forces many drains across chunks
		total := 0
		for total < producers*each {
			batch = q.drain(batch)
			if len(batch) == 0 {
				runtime.Gosched() // producers still in flight; yield and retry
				continue
			}
			for _, m := range batch {
				got[m.sid]++
				total++
			}
			batch = batch[:0]
		}
		wg.Wait()
		for p := 0; p < producers; p++ {
			if got[SymbolID(p)] != each {
				t.Fatalf("%s: producer %d delivered %d, want %d", tag, p, got[SymbolID(p)], each)
			}
		}
		if n := q.pending(); n != 0 {
			t.Fatalf("%s: pending = %d after full drain, want 0", tag, n)
		}
	}

	runBurst("first burst")
	runBurst("second burst") // exercises chunk recycling from the free list
}

// TestChunkQueueFreeListCapped pins the retention bound: drained chunks
// beyond the free-list cap are released (GC-reclaimable) instead of parked
// forever, while bursts within the cap keep their chunks parked for
// alloc-free reuse. Uses 4-item chunks so a few hundred items exercise many
// chunks; deterministic, single producer, single consumer.
func TestChunkQueueFreeListCapped(t *testing.T) {
	q := newChunkQueue[mutation](4)

	countFree := func() int {
		n := 0
		for c := q.free; c != nil; c = c.next {
			n++
		}
		return n
	}
	drainAll := func() {
		batch := make([]mutation, 0, 64)
		for q.pending() > 0 {
			batch = q.drain(batch)
			batch = batch[:0]
		}
	}

	// Burst within the cap: all drained chunks stay parked for reuse.
	const small = 5 * 4 // 5 chunks
	for i := 0; i < small; i++ {
		q.enqueue(mutation{op: mutInsert})
	}
	drainAll()
	// a full drain resets the single head/tail chunk in place; the other 4 recycle
	if n := countFree(); n != 4 {
		t.Fatalf("small burst: free list = %d chunks, want 4", n)
	}

	// Burst far beyond the cap: the free list must settle at the cap.
	const big = (2*chunkFreeCap + 3) * 4 // 131 chunks
	for i := 0; i < big; i++ {
		q.enqueue(mutation{op: mutInsert})
	}
	drainAll()
	if n := countFree(); n > chunkFreeCap {
		t.Fatalf("big burst: free list = %d chunks, want <= %d", n, chunkFreeCap)
	}
	if p := q.pending(); p != 0 {
		t.Fatalf("pending = %d after drain, want 0", p)
	}
}

// TestMutQueueDrainStopsAtEmpty pins the batch-closing contract the flusher
// relies on: drain returns without parking when nothing is claimed.
func TestMutQueueDrainStopsAtEmpty(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	q.enqueue(mutation{op: mutInsert})
	batch := q.drain(make([]mutation, 0, 256))
	if len(batch) != 1 {
		t.Fatalf("drain got %d items, want 1", len(batch))
	}
	done := make(chan struct{})
	go func() {
		q.drain(make([]mutation, 0, 256))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain parked on an empty queue")
	}
}

// TestMutQueueDequeueBlocks pins the handoff contract: dequeue parks on
// empty and is woken by the very next enqueue.
func TestMutQueueDequeueBlocks(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	got := make(chan mutation, 1)
	go func() { got <- q.dequeue() }()
	q.enqueue(mutation{op: mutInsert, sid: 7})
	select {
	case m := <-got:
		if m.sid != 7 {
			t.Fatalf("dequeued sid %d, want 7", m.sid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dequeue not woken by enqueue")
	}
	if n := q.pending(); n != 0 {
		t.Fatalf("pending = %d after dequeue, want 0", n)
	}
}

// TestMutQueueFIFOOrderAcrossChunks pins the global FIFO contract the
// Upsert remove-after-insert anti-aliasing argument and the mutClose sentinel
// semantics rest on: a single producer's sequence must emerge in exact order
// across chunk boundaries, with no reordering, duplication, or gap. Consumed
// with the exact runFlusher pattern (one dequeue plus a 256-cap drain per
// round); existing tests use concurrent producers and assert per-producer
// counts only.
func TestMutQueueFIFOOrderAcrossChunks(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	const n = 2*mutQueueChunk + 9 // crosses several chunk boundaries
	for i := 0; i < n; i++ {
		q.enqueue(mutation{op: mutInsert, sid: SymbolID(i)})
	}
	batch := make([]mutation, 0, 256)
	got := make([]SymbolID, 0, n)
	for q.pending() > 0 {
		m := q.dequeue()
		batch = q.drain(batch)
		got = append(got, m.sid)
		for _, x := range batch {
			got = append(got, x.sid)
		}
		batch = batch[:0]
	}
	if len(got) != n {
		t.Fatalf("delivered %d items, want %d", len(got), n)
	}
	for i, sid := range got {
		if sid != SymbolID(i) {
			t.Fatalf("item %d = %d — reordering, duplication, or gap across chunks", i, sid)
		}
	}
	if p := q.pending(); p != 0 {
		t.Fatalf("pending = %d after full consumption, want 0", p)
	}
}

// TestMutQueueDequeueAdvancesAcrossChunkBoundary pins deterministic
// chunk-boundary crossing through the dequeue path (previously exercised only
// via drain): the chunk-th dequeue pops the last slot of a full head chunk
// and drives advanceLocked's head != tail branch, and a second burst served
// from the recycled head chunk must never yield a stale item.
func TestMutQueueDequeueAdvancesAcrossChunkBoundary(t *testing.T) {
	q := newChunkQueue[mutation](mutQueueChunk)
	chunk := mutQueueChunk
	for i := 0; i < chunk; i++ {
		q.enqueue(mutation{op: mutInsert, sid: SymbolID(i)})
	}
	for i := 0; i < 5; i++ { // tail rolls to a second chunk; head full and non-tail
		q.enqueue(mutation{op: mutInsert, sid: SymbolID(chunk + i)})
	}
	for i := 0; i < chunk+1; i++ {
		if m := q.dequeue(); m.sid != SymbolID(i) {
			t.Fatalf("dequeue %d returned tag %d", i, m.sid)
		}
	}
	// chunk+5 enqueued minus chunk+1 dequeued leaves 4 in the second chunk.
	if p := q.pending(); p != 4 {
		t.Fatalf("pending = %d after chunk+1 dequeues, want 4", p)
	}

	// Expected consumption order: the 4 leftover tags still queued, then the
	// burst's fresh tags — any stale item from the recycled chunk's previous
	// occupancy (a tag ≤ chunk) breaks the sequence.
	const burst = mutQueueChunk + 3 // served partly from the recycled head chunk
	wantSeq := make([]SymbolID, 0, 4+burst)
	for i := chunk + 1; i <= chunk+4; i++ {
		wantSeq = append(wantSeq, SymbolID(i))
	}
	for i := 0; i < burst; i++ {
		wantSeq = append(wantSeq, SymbolID(chunk+5+i))
	}
	for i := 0; i < burst; i++ {
		q.enqueue(mutation{op: mutInsert, sid: SymbolID(chunk + 5 + i)})
	}
	batch := make([]mutation, 0, 256)
	k := 0
	for q.pending() > 0 {
		m := q.dequeue()
		if m.sid != wantSeq[k] {
			t.Fatalf("dequeue returned %d, want %d (stale item from recycled chunk?)", m.sid, wantSeq[k])
		}
		k++
		batch = q.drain(batch)
		for _, x := range batch {
			if x.sid != wantSeq[k] {
				t.Fatalf("drain returned %d, want %d (stale item from recycled chunk?)", x.sid, wantSeq[k])
			}
			k++
		}
		batch = batch[:0]
	}
	if k != len(wantSeq) {
		t.Fatalf("delivered %d items, want %d", k, len(wantSeq))
	}
	if p := q.pending(); p != 0 {
		t.Fatalf("pending = %d after full drain, want 0", p)
	}
}
