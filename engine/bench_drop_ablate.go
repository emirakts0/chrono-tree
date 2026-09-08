//go:build ablation

package engine

import "testing"

// reportMutationDrops logs the engine's trySubmit drop count so each
// benchmark row's drop behavior is visible in the campaign output. Called
// before and after the timed run; the row's own drops = end − start.
func reportMutationDrops(b *testing.B, e *Engine) {
	b.Logf("mutQ drops so far: %d", e.Stats().MutationDrops)
}
