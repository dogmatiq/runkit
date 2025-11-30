package xrapid

import "pgregory.net/rapid"

// Nillable creates a generator that produces either nil or a value from the
// provided generator.
func Nillable[T any](gen *rapid.Generator[*T]) *rapid.Generator[*T] {
	return rapid.Custom(
		func(t *rapid.T) *T {
			if rapid.Bool().Draw(t, "nil value") {
				return nil
			}

			return gen.Draw(t, "non-nil value")
		},
	)
}
