package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

var validParams = map[string]struct{}{
	"region":   {},
	"role_arn": {},
	"insecure": {},
}

// NewProvider returns a [Provider] configured from a dynamodb:// URL. It
// returns an error if u.Scheme is not "dynamodb".
//
// This driver is not yet implemented.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "dynamodb" {
		return nil, fmt.Errorf("invalid dynamodb URL: unexpected URL scheme %q", u.Scheme)
	}

	base := strings.TrimPrefix(u.Path, "/")
	if base == "" {
		return nil, errors.New("invalid dynamodb URL: base name is required in the path (e.g. dynamodb:///<base>)")
	}

	for k := range u.Query() {
		if _, ok := validParams[k]; !ok {
			return nil, fmt.Errorf("invalid dynamodb URL: unknown parameter %q", k)
		}
	}

	return &Provider{}, nil
}

// Provider is a persistence.Provider backed by Amazon DynamoDB.
type Provider struct{}

// KVStore returns a key-value store backed by DynamoDB.
func (p *Provider) KVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}

// JournalStore returns a journal store backed by DynamoDB.
func (p *Provider) JournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}

// SetStore returns a set store backed by DynamoDB.
func (p *Provider) SetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}

// Close is a no-op.
func (*Provider) Close() error {
	return nil
}
