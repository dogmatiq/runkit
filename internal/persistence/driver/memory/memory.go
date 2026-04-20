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

// Provider is a persistence.Provider backed by a silo of in-memory stores.
// Providers with the same silo name share state.
type Provider struct {
	kv      memorykv.BinaryStore
	journal memoryjournal.BinaryStore
	set     memoryset.BinaryStore
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

	p, ok := registry.Load(silo)
	if !ok {
		p, _ = registry.LoadOrStore(silo, &Provider{})
	}

	return p.(*Provider), nil
}

// KVStore implements persistence.Provider.
func (p *Provider) KVStore(context.Context) (kv.BinaryStore, error) {
	return &p.kv, nil
}

// JournalStore implements persistence.Provider.
func (p *Provider) JournalStore(context.Context) (journal.BinaryStore, error) {
	return &p.journal, nil
}

// SetStore implements persistence.Provider.
func (p *Provider) SetStore(context.Context) (set.BinaryStore, error) {
	return &p.set, nil
}
