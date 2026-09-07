//go:build !ablation

package engine

// trySubmit is the hot path's best-effort, never-blocking enqueue. A miss
// only defers cleanup; it can never cause a missed or duplicate trigger.
func (e *Engine) trySubmit(m mutation) bool {
	select {
	case e.mutQ <- m:
		return true
	default:
		return false
	}
}
