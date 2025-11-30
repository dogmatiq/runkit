package xrapid

import (
	"iter"

	"pgregory.net/rapid"
)

// SampledFromKeysOf returns a generator that produces random keys from the
// given map.
func SampledFromKeysOf[K any](
	m interface {
		Len() int
		Keys() iter.Seq[K]
	},
) *rapid.Generator[K] {
	return rapid.Custom(
		func(t *rapid.T) K {
			if m.Len() == 0 {
				t.Skip("map is empty")
			}

			iter := 0
			stop := rapid.IntRange(0, m.Len()-1).Draw(t, "iterations")

			for key := range m.Keys() {
				if iter == stop {
					return key
				}
				iter++
			}

			panic("unreachable")
		},
	)
}

// SampledFromValuesOf returns a generator that produces a random value from the
// given map.
func SampledFromValuesOf[V any](
	m interface {
		Len() int
		Values() iter.Seq[V]
	},
) *rapid.Generator[V] {
	return rapid.Custom(
		func(t *rapid.T) V {
			if m.Len() == 0 {
				t.Skip("map is empty")
			}

			iter := 0
			stop := rapid.IntRange(0, m.Len()-1).Draw(t, "iterations")

			for value := range m.Values() {
				if iter == stop {
					return value
				}
				iter++
			}

			panic("unreachable")
		},
	)
}
