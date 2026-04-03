package persistence

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/marshaler"
)

// OpenTransactionJournal returns the journal that store's the transactions for
// the specified stream partition.
func OpenTransactionJournal(
	ctx context.Context,
	store journal.BinaryStore,
	partitionID *uuidpb.UUID,
) (journal.Journal[*Transaction], error) {
	name := fmt.Sprintf(
		"runkit.eventstream.v1/%s/transactions",
		partitionID,
	)

	return journal.
		NewMarshalingStore(store, transactionMarshaler).
		Open(ctx, name)
}

// transactionMarshaler is a [marshaler.Marshaler] for [Transaction] values.
var transactionMarshaler = marshaler.NewProto[*Transaction]()
