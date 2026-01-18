package persistence

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/x/xpersistence"
)

// OpenMessageKeyspace returns the keyspace that stores the poison queue's
// messages.
func OpenMessageKeyspace(
	ctx context.Context,
	store kv.BinaryStore,
) (kv.Keyspace[*uuidpb.UUID, *QueueMessage], error) {
	return kv.
		NewMarshalingStore(store, xpersistence.UUIDMarshaler, messageMarshaler).
		Open(ctx, "runkit.poisonqueue.v1")
}

// messageMarshaler is a [marshaler.Marshaler] for [QueueMessage] values.
var messageMarshaler = marshaler.NewProto[*QueueMessage]()
