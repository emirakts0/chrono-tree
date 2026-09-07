//go:build ablation

package engine

import "sync/atomic"

// mutDrops counts trySubmit misses (full mutQ). Diagnostic only: the default
// build's measured path is unchanged; this exists to test whether the
// benchmark's fast 1-core mode is a drop-degenerate state.
var mutDrops atomic.Int64

// MutationDrops reports trySubmit drops since process start.
func MutationDrops() int64 { return mutDrops.Load() }

// trySubmit is the ablatable twin of submit_default.go: identical semantics,
// plus a drop counter.
func (e *Engine) trySubmit(m mutation) bool {
	select {
	case e.mutQ <- m:
		return true
	default:
		mutDrops.Add(1)
		return false
	}
}
