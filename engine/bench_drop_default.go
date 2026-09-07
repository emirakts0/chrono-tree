//go:build !ablation

package engine

import "testing"

// reportMutationDrops is a no-op in the default build (no counter exists).
func reportMutationDrops(b *testing.B) {}
