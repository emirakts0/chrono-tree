//go:build !ablation

package engine

import "testing"

// reportMutationDrops is a no-op in the default build: the counter exists
// (Stats().MutationDrops) but campaign rows stay unpolluted; shedding is read
// from the ablation build's logs, whose engine code is identical.
func reportMutationDrops(b *testing.B, e *Engine) {}
