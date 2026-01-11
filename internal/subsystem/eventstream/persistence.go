package eventstream

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
)

// openJournal returns the journal for the specified stream partition.
func openJournal(
	ctx context.Context,
	store journal.BinaryStore,
	partitionID *uuidpb.UUID,
) (journal.Journal[*persistence.Transaction], error) {
	name := fmt.Sprintf(
		"runkit.eventstream.v1/%s",
		partitionID,
	)

	return journal.
		NewMarshalingStore(store, transactionMarshaler).
		Open(ctx, name)
}

var transactionMarshaler = marshaler.NewProto[*persistence.Transaction]()

// searchForOffset returns a comparison function that searches for the
// [transaction.Transaction] that contains the [transaction.AppendOperation]
// that appends the event at the given offset.
func searchForOffset(offset uint64) journal.CompareFunc[*persistence.Transaction] {
	return func(
		_ context.Context,
		_ journal.Position,
		txn *persistence.Transaction,
	) (int, error) {
		if txn.MetaData.OffsetBefore > offset {
			return +1, nil
		} else if txn.MetaData.OffsetAfter <= offset {
			return -1, nil
		}
		return 0, nil
	}
}
