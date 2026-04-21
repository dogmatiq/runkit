package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgjournal"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgkv"
	"github.com/dogmatiq/persistencekit/driver/sql/postgres/pgset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// NewProvider returns a [Provider] configured from a postgres:// or
// postgresql:// URL. It returns an error if u.Scheme is not "postgres" or
// "postgresql".
//
// Pool settings can be configured via URL parameters, for example:
// pool_max_conns, pool_min_conns, pool_max_conn_lifetime, etc.
//
// See [pgxpool.ParseConfig] for the full list of supported parameters.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("invalid postgres URL: unexpected URL scheme %q", u.Scheme)
	}

	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("invalid postgres URL: %w", err)
	}

	return &Provider{config: cfg}, nil
}

// Provider is a persistence.Provider backed by PostgreSQL.
type Provider struct {
	config *pgxpool.Config

	m    sync.Mutex
	pool *pgxpool.Pool
	db   *sql.DB
}

// KVStore returns a key-value store backed by PostgreSQL.
func (p *Provider) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	db, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgkv.BinaryStore{DB: db}, nil
}

// JournalStore returns a journal store backed by PostgreSQL.
func (p *Provider) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	db, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgjournal.BinaryStore{DB: db}, nil
}

// SetStore returns a set store backed by PostgreSQL.
func (p *Provider) SetStore(ctx context.Context) (set.BinaryStore, error) {
	db, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return &pgset.BinaryStore{DB: db}, nil
}

// Close closes the underlying connection pool.
func (p *Provider) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	p.config = nil

	if p.db == nil {
		return nil
	}

	err := p.db.Close()
	p.db = nil

	p.pool.Close()
	p.pool = nil

	return err
}

// open returns the shared *sql.DB, creating the connection pool on first call.
func (p *Provider) open(ctx context.Context) (*sql.DB, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.db != nil {
		return p.db, nil
	}

	if p.config == nil {
		panic("provider is closed")
	}

	pool, err := pgxpool.NewWithConfig(ctx, p.config)
	if err != nil {
		return nil, err
	}

	p.pool = pool
	p.db = stdlib.OpenDBFromPool(pool)

	return p.db, nil
}
