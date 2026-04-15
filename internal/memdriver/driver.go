package memdriver

import (
	"context"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Driver is an in-memory implementation of the runkit.PersistenceProvider
// interface. Its zero value is ready to use.
type Driver struct {
	kv  memorykv.BinaryStore
	j   memoryjournal.BinaryStore
	set memoryset.BinaryStore
}

// KVStore implements runkit.PersistenceProvider.
func (d *Driver) KVStore(_ context.Context) (kv.BinaryStore, error) {
	return &d.kv, nil
}

// JournalStore implements runkit.PersistenceProvider.
func (d *Driver) JournalStore(_ context.Context) (journal.BinaryStore, error) {
	return &d.j, nil
}

// SetStore implements runkit.PersistenceProvider.
func (d *Driver) SetStore(_ context.Context) (set.BinaryStore, error) {
	return &d.set, nil
}
