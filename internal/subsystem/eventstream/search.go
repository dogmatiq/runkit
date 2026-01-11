package eventstream

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
)

// searchForOffset returns a comparison function that searches for the
// [persistence.Transaction] that contains the [persistence.AppendOperation]
// that appended the event at the given offset.
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
