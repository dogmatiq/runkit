package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/persistence/driver/internal/xaws"
)

// NewProvider returns a [Provider] configured from an s3:// URL. It returns
// an error if u.Scheme is not "s3".
//
// This driver is not yet implemented.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid s3 URL: unexpected URL scheme %q", u.Scheme)
	}

	bucket := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return nil, errors.New("invalid s3 URL: bucket name is required in the path (e.g. s3:///<bucket>)")
	}

	if _, err := xaws.ParseParams("s3", u); err != nil {
		return nil, err
	}

	return &Provider{}, nil
}

// Provider is a persistence.Provider backed by Amazon S3.
type Provider struct{}

// KVStore returns a key-value store backed by Amazon S3.
func (p *Provider) KVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("s3 kv store is not yet implemented in persistencekit")
}

// JournalStore returns a journal store backed by Amazon S3.
func (p *Provider) JournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("s3 persistence driver is not yet implemented")
}

// SetStore returns a set store backed by Amazon S3.
func (p *Provider) SetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("s3 set store is not yet implemented in persistencekit")
}

// Close is a no-op.
func (*Provider) Close() error {
	return nil
}
