# Persistence DSN — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement a DSN-based system for configuring persistence backends from
environment variables. The DSN is a URL whose scheme identifies the driver; all
built-in drivers are covered by the format. Only the memory driver is fully
implemented in this plan; the other drivers parse correctly but return a "not
yet implemented" error.

**Architecture:** A new `internal/persistence` package defines the `Provider`
interface (type-aliased as `runkit.PersistenceProvider`) and the
`ProviderFromDSN` function. Each driver lives in its own sub-package under
`internal/persistence/driver/`. The memory driver is backed by a
package-level registry of named in-memory store instances.

---

## DSN format reference

All DSNs are URLs — the `//` authority component is always required. Anything
without `//` is rejected with a clear error message.

| Driver     | Canonical form                                    | Notes                                                |
| ---------- | ------------------------------------------------- | ---------------------------------------------------- |
| Memory     | `memory://` or `memory:///name`                   | Named stores share state; unnamed = default instance |
| PostgreSQL | `postgres://user:pass@host:5432/dbname`           | Pass-through to pgx; `postgresql://` also accepted   |
| DynamoDB   | `dynamodb:///base` or `dynamodb://host:port/base` | `region`, `role_arn`, `insecure` params; strict      |
| S3         | `s3:///bucket` or `s3://endpoint.host/bucket`     | `region`, `role_arn`, `insecure` params; strict      |

**DynamoDB table naming:** `<base>-journal`, `<base>-kv`, `<base>-set`.

**S3:** Journal-only driver. `NewKVStore` and `NewSetStore` return errors until
persistencekit implements those drivers.

**Strict vs lenient validation:** Runkit-owned schemes (`memory`, `dynamodb`,
`s3`) reject unknown query parameters. Pass-through schemes (`postgres`,
`postgresql`) are opaque beyond the scheme — pgx validates the rest.

**Memory named stores:** `memory://` resolves to a default unnamed store.
`memory:///name` resolves to a named store. Multiple calls with the same name
return providers backed by the same underlying store objects. A non-empty host
component is an error.

**AWS params:** `region` overrides `AWS_REGION`. `role_arn` assumes a role on
top of the resolved base credentials. `insecure=true` uses HTTP instead of
HTTPS (for LocalStack, MinIO, etc.). These params apply to both `dynamodb` and
`s3` schemes.

---

## File layout

**New files:**

- `internal/persistence/provider.go` — `Provider` interface definition
- `internal/persistence/dsn.go` — `ProviderFromDSN` and URL dispatch logic
- `internal/persistence/driver/memory/registry.go` — named store registry
- `internal/persistence/driver/postgres/driver.go` — stub, returns unimplemented error
- `internal/persistence/driver/dynamodb/driver.go` — stub, returns unimplemented error
- `internal/persistence/driver/s3/driver.go` — stub, returns unimplemented error

**Modified files:**

- `persistence.go` — replace interface body with type alias to `persistence.Provider`
- `internal/persistence/driver/memory/driver.go` — update to use registry

---

## Task 1: Define `persistence.Provider` and alias it

**Files:**

- Create: `internal/persistence/provider.go`
- Modify: `persistence.go`

- [ ] **Step 1: Create `internal/persistence/provider.go`**

```go
package persistence

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// Provider provides the persistence stores used by the engine.
//
// The interface is satisfied implicitly — persistence driver packages implement
// these methods on their own types without importing this package.
type Provider interface {
	NewKVStore(ctx context.Context) (kv.BinaryStore, error)
	NewJournalStore(ctx context.Context) (journal.BinaryStore, error)
	NewSetStore(ctx context.Context) (set.BinaryStore, error)
}
```

- [ ] **Step 2: Replace `runkit.PersistenceProvider` with a type alias**

Replace the contents of `persistence.go` with:

```go
package runkit

import "github.com/dogmatiq/runkit/internal/persistence"

// PersistenceProvider provides the persistence stores used by the engine.
type PersistenceProvider = persistence.Provider
```

- [ ] **Step 3: Verify compilation**

```
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```
git add internal/persistence/provider.go persistence.go
git commit -m "Define persistence.Provider interface, alias as runkit.PersistenceProvider"
```

---

## Task 2: Implement named memory store registry

**Files:**

- Create: `internal/persistence/driver/memory/registry.go`
- Modify: `internal/persistence/driver/memory/driver.go`

The existing memory driver (`driver` struct) always returns fresh independent
stores. Replace it with a mechanism where `ProviderFromDSN` looks up a named
entry in a package-level registry.

- [ ] **Step 1: Create `internal/persistence/driver/memory/registry.go`**

```go
package memory

import (
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryset"
)

var (
	registryMu sync.Mutex
	registry   = map[string]*stores{}
)

// stores holds the three in-memory store instances for a named registry entry.
type stores struct {
	kv      memorykv.BinaryStore
	journal memoryjournal.BinaryStore
	set     memoryset.BinaryStore
}

// lookup returns the stores for the given name, creating them if they do not
// exist. An empty name refers to the default unnamed instance.
func lookup(name string) *stores {
	registryMu.Lock()
	defer registryMu.Unlock()

	s, ok := registry[name]
	if !ok {
		s = &stores{}
		registry[name] = s
	}
	return s
}
```

- [ ] **Step 2: Update `internal/persistence/driver/memory/driver.go`**

Replace the existing `driver` struct and `Driver` var with a named-store-aware
implementation:

```go
package memory

import (
	"context"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// provider is a persistence.Provider backed by a named in-memory store.
type provider struct {
	s *stores
}

// NewProvider returns a Provider backed by the named in-memory store. An empty
// name refers to the default unnamed instance. Providers with the same name
// share state.
func NewProvider(name string) *provider {
	return &provider{s: lookup(name)}
}

// NewKVStore implements persistence.Provider.
func (p *provider) NewKVStore(context.Context) (kv.BinaryStore, error) {
	return &p.s.kv, nil
}

// NewJournalStore implements persistence.Provider.
func (p *provider) NewJournalStore(context.Context) (journal.BinaryStore, error) {
	return &p.s.journal, nil
}

// NewSetStore implements persistence.Provider.
func (p *provider) NewSetStore(context.Context) (set.BinaryStore, error) {
	return &p.s.set, nil
}
```

- [ ] **Step 3: Update call sites that used `memory.Driver`**

Search for references to `memory.Driver` and replace with `memory.NewProvider("")`.

```
grep -rn "memory\.Driver" .
```

- [ ] **Step 4: Verify compilation and tests**

```
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/persistence/driver/memory/
git commit -m "Add named memory store registry to memory driver"
```

---

## Task 3: Implement `ProviderFromDSN`

**Files:**

- Create: `internal/persistence/dsn.go`

- [ ] **Step 1: Create `internal/persistence/dsn.go`**

```go
package persistence

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/dogmatiq/runkit/internal/persistence/driver/dynamodb"
	"github.com/dogmatiq/runkit/internal/persistence/driver/memory"
	"github.com/dogmatiq/runkit/internal/persistence/driver/postgres"
	"github.com/dogmatiq/runkit/internal/persistence/driver/s3"
)

// ProviderFromDSN returns a Provider configured from the given DSN string.
//
// The DSN must be a URL. The scheme identifies the driver:
//
//   - memory://  or  memory:///name
//   - postgres://user:pass@host:5432/dbname  (or postgresql://)
//   - dynamodb:///base  or  dynamodb://host:port/base
//   - s3:///bucket  or  s3://endpoint.host/bucket
func ProviderFromDSN(dsn string) (Provider, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid persistence DSN: %w", err)
	}

	if !strings.Contains(dsn, "//") {
		return nil, fmt.Errorf("invalid persistence DSN: missing // authority component")
	}

	switch u.Scheme {
	case "memory":
		return parseMemoryDSN(u)
	case "postgres", "postgresql":
		return postgres.ProviderFromDSN(dsn)
	case "dynamodb":
		return dynamodb.ProviderFromDSN(u)
	case "s3":
		return s3.ProviderFromDSN(u)
	default:
		return nil, fmt.Errorf("unknown persistence DSN scheme: %q", u.Scheme)
	}
}

func parseMemoryDSN(u *url.URL) (Provider, error) {
	if u.Host != "" {
		return nil, fmt.Errorf("invalid memory DSN: host component must be empty (use memory:///name for named stores)")
	}
	if u.RawQuery != "" {
		return nil, fmt.Errorf("invalid memory DSN: query parameters are not supported")
	}
	name := strings.TrimPrefix(u.Path, "/")
	return memory.NewProvider(name), nil
}
```

- [ ] **Step 2: Verify compilation**

```
go build ./internal/persistence/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```
git add internal/persistence/dsn.go
git commit -m "Implement ProviderFromDSN with memory driver support"
```

---

## Task 4: Add stubs for postgres, dynamodb, and s3 drivers

**Files:**

- Create: `internal/persistence/driver/postgres/driver.go`
- Create: `internal/persistence/driver/dynamodb/driver.go`
- Create: `internal/persistence/driver/s3/driver.go`

Each stub parses its DSN for structural validity and returns an unimplemented
error from each store method.

- [ ] **Step 1: Create `internal/persistence/driver/postgres/driver.go`**

```go
package postgres

import "errors"

// ProviderFromDSN returns a Provider configured from a postgres:// or
// postgresql:// DSN. The DSN is passed through verbatim to pgx.
//
// This driver is not yet implemented.
func ProviderFromDSN(dsn string) (*Provider, error) {
	return &Provider{dsn: dsn}, nil
}

// Provider is a persistence.Provider backed by PostgreSQL.
type Provider struct {
	dsn string
}

func (p *Provider) NewKVStore(_ interface{ Done() <-chan struct{} }) (interface{}, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}
```

Wait — the store method signatures must match `persistence.Provider` exactly.
Since `Provider` structs are in separate packages that cannot import
`internal/persistence`, they satisfy the interface implicitly. Use the correct
signatures from persistencekit directly.

Replace the stub above with:

```go
package postgres

import (
	"context"
	"errors"

	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
)

// ProviderFromDSN returns a Provider configured from a postgres:// or
// postgresql:// DSN. The full DSN string is passed through verbatim to pgx
// when the driver is fully implemented.
//
// This driver is not yet implemented.
func ProviderFromDSN(dsn string) (*Provider, error) {
	return &Provider{dsn: dsn}, nil
}

// Provider is a persistence.Provider backed by PostgreSQL.
type Provider struct{ dsn string }

// NewKVStore implements persistence.Provider.
func (p *Provider) NewKVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}

// NewJournalStore implements persistence.Provider.
func (p *Provider) NewJournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}

// NewSetStore implements persistence.Provider.
func (p *Provider) NewSetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("postgres persistence driver is not yet implemented")
}
```

- [ ] **Step 2: Create `internal/persistence/driver/dynamodb/driver.go`**

Parse the DSN strictly: reject unknown query parameters, require a non-empty
path (base name), and accept `region`, `role_arn`, `insecure` as the only
valid params.

```go
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

// ProviderFromDSN returns a Provider configured from a dynamodb:// DSN.
//
// This driver is not yet implemented.
func ProviderFromDSN(u *url.URL) (*Provider, error) {
	base := strings.TrimPrefix(u.Path, "/")
	if base == "" {
		return nil, errors.New("invalid dynamodb DSN: base name is required in the URL path (e.g. dynamodb:///myapp)")
	}

	for k := range u.Query() {
		if _, ok := validParams[k]; !ok {
			return nil, fmt.Errorf("invalid dynamodb DSN: unknown parameter %q", k)
		}
	}

	return &Provider{}, nil
}

// Provider is a persistence.Provider backed by Amazon DynamoDB.
type Provider struct{}

// NewKVStore implements persistence.Provider.
func (p *Provider) NewKVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}

// NewJournalStore implements persistence.Provider.
func (p *Provider) NewJournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}

// NewSetStore implements persistence.Provider.
func (p *Provider) NewSetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("dynamodb persistence driver is not yet implemented")
}
```

- [ ] **Step 3: Create `internal/persistence/driver/s3/driver.go`**

Parse strictly. S3 requires a non-empty path (bucket). The host is an optional
custom endpoint; empty host means AWS default endpoint.

```go
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
)

var validParams = map[string]struct{}{
	"region":   {},
	"role_arn": {},
	"insecure": {},
}

// ProviderFromDSN returns a Provider configured from an s3:// DSN.
//
// This driver is not yet implemented for kv and set stores.
func ProviderFromDSN(u *url.URL) (*Provider, error) {
	bucket := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return nil, errors.New("invalid s3 DSN: bucket name is required in the URL path (e.g. s3:///my-bucket)")
	}

	for k := range u.Query() {
		if _, ok := validParams[k]; !ok {
			return nil, fmt.Errorf("invalid s3 DSN: unknown parameter %q", k)
		}
	}

	return &Provider{}, nil
}

// Provider is a persistence.Provider backed by Amazon S3.
type Provider struct{}

// NewKVStore implements persistence.Provider.
func (p *Provider) NewKVStore(context.Context) (kv.BinaryStore, error) {
	return nil, errors.New("s3 kv store is not yet implemented in persistencekit")
}

// NewJournalStore implements persistence.Provider.
func (p *Provider) NewJournalStore(context.Context) (journal.BinaryStore, error) {
	return nil, errors.New("s3 persistence driver is not yet implemented")
}

// NewSetStore implements persistence.Provider.
func (p *Provider) NewSetStore(context.Context) (set.BinaryStore, error) {
	return nil, errors.New("s3 set store is not yet implemented in persistencekit")
}
```

- [ ] **Step 4: Verify compilation**

```
go build ./internal/persistence/...
```

Expected: no errors.

- [ ] **Step 5: Commit**

```
git add internal/persistence/driver/postgres/ \
        internal/persistence/driver/dynamodb/ \
        internal/persistence/driver/s3/
git commit -m "Add stub drivers for postgres, dynamodb, and s3"
```

---

## Task 5: Tests for `ProviderFromDSN`

**Files:**

- Create: `internal/persistence/dsn_test.go`

- [ ] **Step 1: Create `internal/persistence/dsn_test.go`**

Cover the following cases:

- `memory://` returns a valid provider
- `memory:///myname` returns a valid provider
- `memory://myname` (non-empty host) returns an error
- `memory://?foo=bar` (query params) returns an error
- Two calls with `memory:///shared` return providers that share KV state
- Two calls with `memory://` return providers that share the default KV state
- `postgres://user:pass@host/db` returns a valid provider
- `postgresql://user:pass@host/db` returns a valid provider
- `dynamodb:///myapp` returns a valid provider
- `dynamodb:///myapp?region=us-east-1` returns a valid provider
- `dynamodb:///myapp?unknown=x` returns an error
- `dynamodb://` (missing base name) returns an error
- `s3:///my-bucket` returns a valid provider
- `s3://endpoint.host/my-bucket` returns a valid provider
- `s3:///my-bucket?insecure=true` returns a valid provider
- `s3:///my-bucket?unknown=x` returns an error
- `s3://` (missing bucket) returns an error
- `unknown://foo` returns an error
- `nodoubelslash` (missing `//`) returns an error

- [ ] **Step 2: Run the tests**

```
go test ./internal/persistence/...
```

Expected: PASS.

- [ ] **Step 3: Commit**

```
git add internal/persistence/dsn_test.go
git commit -m "Add ProviderFromDSN tests"
```
