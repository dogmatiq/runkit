package eventstreamjournal

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/marshaler"
)

// Journal is an alias for the type of journal that stores [Record] entries.
type Journal = journal.Journal[*Record]

// Open returns the journal for the specified stream partition.
func Open(
	ctx context.Context,
	store journal.BinaryStore,
	partitionID *uuidpb.UUID,
) (Journal, error) {
	return journal.
		NewMarshalingStore(store, recordMarshaler).
		Open(ctx, "runkit.eventstream.journal.v1/"+partitionID.AsString())
}

var recordMarshaler = marshaler.NewProto[*Record]()

// SearchByOffset returns a comparison function that searches for the [Record]
// that records the [AppendEvents] operation containing the event at the given
// offset.
func SearchByOffset(off uint64) journal.CompareFunc[*Record] {
	return func(
		_ context.Context,
		_ journal.Position,
		rec *Record,
	) (int, error) {
		if rec.MetaData.OffsetBefore > off {
			return +1, nil
		} else if rec.MetaData.OffsetAfter <= off {
			return -1, nil
		}
		return 0, nil
	}
}
