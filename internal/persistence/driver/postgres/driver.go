package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// NewProvider returns a [Provider] configured from a postgres:// or
// postgresql:// URL. It returns an error if u.Scheme is not "postgres" or
// "postgresql".
//
// The URL is passed through verbatim to pgx. This driver is not yet
// implemented.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("invalid postgres URL: unexpected URL scheme %q", u.Scheme)
	}

	return &Provider{url: u.String()}, nil
}

// Provider is a persistence.Provider backed by PostgreSQL.
type Provider struct{ url string }

// KVStore implements persistence.Provider.
func (p *Provider) KVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}

// JournalStore implements persistence.Provider.
func (p *Provider) JournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}

// SetStore implements persistence.Provider.
func (p *Provider) SetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}
