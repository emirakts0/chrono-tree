//go:build ablation

package engine

import "testing"

// reportMutationDrops logs the process-wide trySubmit drop count so each
// benchmark row's drop behavior is visible in the campaign output.
func reportMutationDrops(b *testing.B) {
	b.Logf("mutQ drops so far: %d", MutationDrops())
}
