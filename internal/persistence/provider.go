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
	Close() error
}

// NopCloser is a [Provider] adaptor that ignores calls to [Provider.Close].
type NopCloser struct{ Provider }

// Close is a no-op.
func (NopCloser) Close() error {
	return nil
}
