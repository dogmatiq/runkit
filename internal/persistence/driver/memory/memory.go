package memory

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

var registry sync.Map

// silo holds the actual in-memory stores shared by all [Provider] instances
// with the same name.
type silo struct {
	kv      memorykv.BinaryStore
	journal memoryjournal.BinaryStore
	set     memoryset.BinaryStore
}

// Provider is a persistence.Provider backed by a named silo of in-memory
// stores. Providers with the same silo name share state.
type Provider struct {
	silo string
}

// NewProvider returns a [Provider] configured from a memory:// URL. It returns
// an error if u.Scheme is not "memory".
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "memory" {
		return nil, fmt.Errorf("invalid memory URL: unexpected URL scheme %q", u.Scheme)
	}

	if u.Host != "" {
		return nil, fmt.Errorf("invalid memory URL: host component must be empty, use memory:///<silo> for a named silo")
	}

	if u.RawQuery != "" {
		return nil, fmt.Errorf("invalid memory URL: query parameters are not supported")
	}

	silo := strings.TrimPrefix(u.Path, "/")
	if silo == "" {
		return nil, fmt.Errorf("invalid memory URL: silo name is required in the URL path: memory:///<silo>")
	}

	return &Provider{silo: silo}, nil
}

// KVStore returns the silo's in-memory key-value store.
func (p *Provider) KVStore(context.Context) (kv.BinaryStore, error) {
	return &p.load().kv, nil
}

// JournalStore returns the silo's in-memory journal store.
func (p *Provider) JournalStore(context.Context) (journal.BinaryStore, error) {
	return &p.load().journal, nil
}

// SetStore returns the silo's in-memory set store.
func (p *Provider) SetStore(context.Context) (set.BinaryStore, error) {
	return &p.load().set, nil
}

// Close is a no-op.
func (p *Provider) Close() error {
	return nil
}

// load returns the silo for this provider, creating it if necessary.
func (p *Provider) load() *silo {
	v, _ := registry.LoadOrStore(p.silo, &silo{})
	return v.(*silo)
}
