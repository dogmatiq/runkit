package xrapid

import (
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"pgregory.net/rapid"
)

// Identity returns a generator of random [*identitypb.Identity] values.
func Identity() *rapid.Generator[*identitypb.Identity] {
	return rapid.Custom(
		func(t *rapid.T) *identitypb.Identity {
			return &identitypb.Identity{
				Name: rapid.String().Draw(t, "identity name"),
				Key:  uuidpb.Generate(),
			}
		},
	)
}
