package persistence

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Provider provides the persistence stores used by the engine.
type Provider interface {
	KVStore(ctx context.Context) (kv.BinaryStore, error)
	JournalStore(ctx context.Context) (journal.BinaryStore, error)
	SetStore(ctx context.Context) (set.BinaryStore, error)
}
