package server

import (
	"sync"
)

// sseClientQ bounds the frames buffered per browser. A client that falls
// behind is closed (its browser reconnects) — senders never block.
const sseClientQ = 64

// sseHub fans trigger frames out to connected browsers.
type sseHub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{subs: make(map[chan []byte]struct{})}
}

// add registers a client and returns its frame channel plus remove. The
// channel is closed exactly once — by remove on disconnect, or by
// broadcast on overflow, whichever comes first (both hold mu and only
// close members still in the map).
func (h *sseHub) add() (<-chan []byte, func()) {
	ch := make(chan []byte, sseClientQ)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	remove := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
	return ch, remove
}

// broadcast fans one pre-marshaled frame to every client. Runs on
// the NATS reader goroutine — must never block.
func (h *sseHub) broadcast(frame []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- frame:
		default: // slow client: drop it, it will reconnect
			delete(h.subs, ch)
			// A closed channel still delivers its buffered frames, so
			// drop them first: the client must observe the close (and
			// must not be handed data after being dropped).
		drain:
			for {
				select {
				case <-ch:
				default:
					break drain
				}
			}
			close(ch)
		}
	}
}
