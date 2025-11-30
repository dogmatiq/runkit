package xrapid

import (
	"math"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"pgregory.net/rapid"
)

// UUID returns a generator of random [*uuidpb.UUID] values.
func UUID() *rapid.Generator[*uuidpb.UUID] {
	return rapid.Custom(
		func(t *rapid.T) *uuidpb.UUID {
			return &uuidpb.UUID{
				Upper: rapid.Uint64Range(1, math.MaxUint64).Draw(t, "upper"),
				Lower: rapid.Uint64Range(1, math.MaxUint64).Draw(t, "lower"),
			}
		},
	)
}
