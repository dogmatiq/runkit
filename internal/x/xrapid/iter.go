package xrapid

import (
	"iter"
	"slices"

	"pgregory.net/rapid"
)

// SampledFromSeq returns a generator that produces random elements from the
// given sequence.
func SampledFromSeq[T any](seq iter.Seq[T]) *rapid.Generator[T] {
	return rapid.Custom(
		func(t *rapid.T) T {
			slice := slices.Collect(seq)

			if len(slice) == 0 {
				t.Skip("sequence is empty")
			}

			index := rapid.IntRange(0, len(slice)-1).Draw(t, "index")
			return slice[index]
		},
	)
}
