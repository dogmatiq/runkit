package persistence

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
)

// namespace is the a prefix used for naming persistence primitives such as
// journals.
const namespace = "runkit.eventstream.v1"

// OpenTransactionJournal returns the journal for the specified stream partition.
func OpenTransactionJournal(
	ctx context.Context,
	store journal.BinaryStore,
	partitionID *uuidpb.UUID,
) (journal.Journal[*Transaction], error) {
	name := namespace + "/" + partitionID.AsString()

	return journal.
		NewMarshalingStore(store, transactionMarshaler).
		Open(ctx, name)
}
