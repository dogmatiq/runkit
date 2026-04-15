package runkit

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// PersistenceProvider provides the persistence stores used by the engine.
//
// The interface is satisfied implicitly — persistence driver packages implement
// these methods on their own types without any dependency on runkit.
type PersistenceProvider interface {
	KVStore(ctx context.Context) (kv.BinaryStore, error)
	JournalStore(ctx context.Context) (journal.BinaryStore, error)
	SetStore(ctx context.Context) (set.BinaryStore, error)
}

// PersistenceStores is a convenience type that satisfies [PersistenceProvider]
// by returning pre-constructed stores. It is useful when assembling stores from
// different drivers.
type PersistenceStores struct {
	KV      kv.BinaryStore
	Journal journal.BinaryStore
	Set     set.BinaryStore
}

// KVStore implements [PersistenceProvider].
func (s PersistenceStores) KVStore(_ context.Context) (kv.BinaryStore, error) {
	return s.KV, nil
}

// JournalStore implements [PersistenceProvider].
func (s PersistenceStores) JournalStore(_ context.Context) (journal.BinaryStore, error) {
	return s.Journal, nil
}

// SetStore implements [PersistenceProvider].
func (s PersistenceStores) SetStore(_ context.Context) (set.BinaryStore, error) {
	return s.Set, nil
}
