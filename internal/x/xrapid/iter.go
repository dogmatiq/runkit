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

			return rapid.SampledFrom(slice).Draw(t, "element")
		},
	)
}

// Pair is a key/value pair.
type Pair[K, V any] struct {
	Key   K
	Value V
}

// SampledFromSeq2 returns a generator that produces random elements from the
// given sequence.
func SampledFromSeq2[K, V any](seq iter.Seq2[K, V]) *rapid.Generator[Pair[K, V]] {
	return rapid.Custom(
		func(t *rapid.T) Pair[K, V] {
			var pairs []Pair[K, V]
			for k, v := range seq {
				pairs = append(pairs, Pair[K, V]{Key: k, Value: v})
			}

			if len(pairs) == 0 {
				t.Skip("sequence is empty")
			}

			return rapid.SampledFrom(pairs).Draw(t, "pair")
		},
	)
}
