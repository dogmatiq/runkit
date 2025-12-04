package eventstreamregistry

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/persistencekit/set"
)

// RangeFunc is used to range over all event stream IDs in the registry.
type RangeFunc = set.RangeFunc[*uuidpb.UUID]

// Registry is an alias for the type of set that stores event stream IDs.
type Registry interface {
	Add(context.Context, *uuidpb.UUID) error
	Range(context.Context, RangeFunc) error
	Close() error
}

// Open returns the set for storing the known event streams.
func Open(
	ctx context.Context,
	store set.BinaryStore,
) (Registry, error) {
	return set.
		NewMarshalingStore(store, uuidMarshaler).
		Open(ctx, "runkit.eventstream.set.v1")
}

var uuidMarshaler = marshaler.New(
	func(uuid *uuidpb.UUID) ([]byte, error) {
		return uuid.AsBytes(), nil
	},
	func(data []byte) (*uuidpb.UUID, error) {
		return uuidpb.FromBytes(data)
	},
)
