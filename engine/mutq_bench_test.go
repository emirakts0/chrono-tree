package engine

import (
	"runtime"
	"sync"
	"testing"
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
