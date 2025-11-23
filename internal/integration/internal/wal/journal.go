package wal

import (
	"context"

	uuidpb "github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/marshaler"
)

// OpenJournal returns a [journal.Journal] that contains the WAL for the handler
// with the given key.
func OpenJournal(
	ctx context.Context,
	s journal.BinaryStore,
	key *uuidpb.UUID,
) (journal.Journal[*Transaction], error) {
	store := journal.NewMarshalingStore(s, recordMarshaler)
	name := JournalName(key)
	return store.Open(ctx, name)
}

var recordMarshaler = marshaler.NewProto[*Transaction]()

// JournalName returns the name of the journal that contains the WAL for the
// handler with the given key.
func JournalName(key *uuidpb.UUID) string {
	return "integration:" + key.AsString()
}
