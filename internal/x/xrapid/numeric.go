package xrapid

import "pgregory.net/rapid"

// Uint64Range is a generic version of [rapid.Uint64Range] that produces values
// of type T.
func Uint64Range[T ~uint64](min T, max T) *rapid.Generator[T] {
	return rapid.Custom(
		func(t *rapid.T) T {
			return T(
				rapid.
					Uint64Range(
						uint64(min),
						uint64(max),
					).
					Draw(t, "value"),
			)
		},
	)
}
