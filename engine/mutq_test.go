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
	q := newMutQueue()
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

// TestMutQueueDrainStopsAtEmpty pins the batch-closing contract the flusher
// relies on: drain returns without parking when nothing is claimed.
func TestMutQueueDrainStopsAtEmpty(t *testing.T) {
	q := newMutQueue()
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
	q := newMutQueue()
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
