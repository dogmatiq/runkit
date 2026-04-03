package eventstream

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
)

// searchForOffset returns a comparison function that searches for the
// [persistence.Transaction] that contains the [persistence.AppendOperation]
// that appended the event at the given offset.
func searchForOffset(offset Offset) journal.CompareFunc[*persistence.Transaction] {
	return func(
		_ context.Context,
		_ journal.Position,
		txn *persistence.Transaction,
	) (int, error) {
		if Offset(txn.MetaData.OffsetBefore) > offset {
			return +1, nil
		} else if Offset(txn.MetaData.OffsetAfter) <= offset {
			return -1, nil
		}
		return 0, nil
	}
}
