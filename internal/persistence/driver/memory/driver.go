package memory

import (
	"context"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is an in-memory persistence driver.
var Driver driver

// driver is a stateless in-memory implementation of runkit.PersistenceProvider.
// Each call to a store method returns a fresh, independent store.
type driver struct{}

// NewKVStore returns a new in-memory key-value store.
func (driver) NewKVStore(context.Context) (kv.BinaryStore, error) {
	return &memorykv.BinaryStore{}, nil
}

// NewJournalStore returns a new in-memory journal store.
func (driver) NewJournalStore(context.Context) (journal.BinaryStore, error) {
	return &memoryjournal.BinaryStore{}, nil
}

// NewSetStore returns a new in-memory set store.
func (driver) NewSetStore(context.Context) (set.BinaryStore, error) {
	return &memoryset.BinaryStore{}, nil
}
