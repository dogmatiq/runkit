package runkit

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// PersistenceProvider provides the persistence stores used by the engine.
type PersistenceProvider interface {
	NewKVStore(ctx context.Context) (kv.BinaryStore, error)
	NewJournalStore(ctx context.Context) (journal.BinaryStore, error)
	NewSetStore(ctx context.Context) (set.BinaryStore, error)
}
