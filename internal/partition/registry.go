package partition

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/persistence/uuidpersistence"
)

// RangeFunc is used to range over all partitions in a [Registry].
type RangeFunc = set.RangeFunc[*uuidpb.UUID]

// Registry is a persisted set of all known partitions.
type Registry interface {
	Add(context.Context, *uuidpb.UUID) error
	Range(context.Context, RangeFunc) error
	Close() error
}

// OpenRegistry returns the [Registry] used to track known partitions.
func OpenRegistry(
	ctx context.Context,
	store set.BinaryStore,
) (Registry, error) {
	return set.
		NewMarshalingStore(store, uuidpersistence.Marshaler).
		Open(ctx, "runkit.partition.registry.v1")
}
