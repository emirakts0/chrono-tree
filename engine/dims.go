package engine

import "fmt"

// DimSentinel marks an unused dimension slot; the value vocabulary is
// caller-owned.
const DimSentinel uint16 = 0xFFFF

// dimMax caps configured dimensions: the hot entry carries one
// [dimMax]uint16 inline array, keeping the record at one cache line.
const dimMax = 8

// Dims builds a sentinel-padded dim array from up to dimMax real values.
// More than dimMax values panics: the count is bounded by Config.Dims, so
// this is a programmer error and must fail loudly.
func Dims(values ...uint16) [dimMax]uint16 {
	if len(values) > dimMax {
		panic(fmt.Sprintf("chrono-tree: at most %d dims, got %d", dimMax, len(values)))
	}
	var d [dimMax]uint16
	for i := range d {
		d[i] = DimSentinel
	}
	for i, v := range values {
		d[i] = v
	}
	return d
}

// normalizeDims validates d against the engine's width: every slot below
// width must carry a real value; slots at or past width are overwritten with
// the sentinel, so a zero-value array is valid at any width.
func normalizeDims(d [dimMax]uint16, width uint8) ([dimMax]uint16, bool) {
	for i := 0; i < int(width); i++ {
		if d[i] == DimSentinel {
			return d, false
		}
	}
	for i := int(width); i < dimMax; i++ {
		d[i] = DimSentinel
	}
	return d, true
}
