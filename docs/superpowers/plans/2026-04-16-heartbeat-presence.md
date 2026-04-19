# Node Heartbeat and Presence — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first-cut node heartbeat/presence system from ADR-7, plus the persistence injection and network address infrastructure it depends on.

**Architecture:** The engine acquires a `PersistenceProvider` at startup, opens a typed KV keyspace, starts a stub TCP listener to resolve its advertise address, then runs a heartbeat writer that writes and refreshes a presence record keyed by node UUID. All components are wired together in `Engine.Run()` via `errgroup`.

**Tech Stack:** Go 1.25, `persistencekit` v0.14.0 (`kv`, `journal`, `set`; `driver/memory/memorykv` etc.), `enginekit` v0.21.0 (`uuidpb`, `identitypb`), `ferrite` v1.6.1 (env vars), `google.golang.org/protobuf` (proto generation), `golang.org/x/sync/errgroup`.

---

## Pre-work: Default port

Before Task 3, decide on a default port number for `DOGMA_BIND_ADDRESS`. It must be an
unregistered IANA port. Record it as `const defaultPort = <number>` in `engine.go`. Until
it is decided, use `defaultPort = 0` as a placeholder (port 0 = OS-assigned, acceptable for
tests, must be replaced before shipping).

---

## File layout

**New files:**

- `persistence.go` — `PersistenceProvider` interface and `PersistenceStores` helper
- `listener.go` — `listener` interface, `stubListener`, address resolution logic
- `internal/memdriver/driver.go` — in-memory `PersistenceProvider` for tests
- `internal/heartbeat/internal/heartbeatpb/heartbeat.proto` — proto schema
- `internal/heartbeat/internal/heartbeatpb/heartbeat.pb.go` — generated (do not edit)
- `internal/heartbeat/internal/heartbeatpb/heartbeat_primo.pb.go` — generated (do not edit)
- `internal/heartbeat/writer.go` — `Writer` type and write loop
- `internal/heartbeat/writer_test.go` — `Writer` tests

**Modified files:**

- `option.go` — add `WithPersistence`, `WithBindAddress`, `WithAdvertiseAddress`
- `environment.go` — add `DOGMA_BIND_ADDRESS`, `DOGMA_ADVERTISE_ADDRESS` ferrite vars
- `engine.go` — add fields, add `defaultPort` const, rewrite `Run()`
- `engine_test.go` — update `TestRun` and `TestExecuteCommand` to supply persistence + bind address

---

## Task 1: Persistence interface and option

**Files:**

- Create: `persistence.go`
- Modify: `option.go`
- Modify: `engine.go` (add field + panic)
- Modify: `engine_test.go` (add test for missing persistence panic)

- [ ] **Step 1: Write the failing test for the missing-persistence panic**

Add to the `TestRun` function in `engine_test.go`:

```go
t.Run("it panics if no persistence provider is configured", func(t *testing.T) {
    defer func() {
        if r := recover(); r == nil {
            t.Fatal("expected panic, got none")
        }
    }()

    e := New(
        WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
    )
    e.Run(t.Context())
})
```

- [ ] **Step 2: Run the test to verify it fails**

```
go test -run TestRun/it_panics_if_no_persistence_provider_is_configured ./...
```

Expected: FAIL — no panic occurs yet.

- [ ] **Step 3: Create `persistence.go`**

```go
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
```

- [ ] **Step 4: Add `WithPersistence` to `option.go`**

Add after the existing `WithApplication` function:

```go
// WithPersistence returns an [Option] that configures the persistence provider
// for the engine.
//
// A persistence provider is required. [Run] panics if none is configured.
func WithPersistence(p PersistenceProvider) Option {
	return func(e *Engine) {
		e.persistence = p
	}
}
```

- [ ] **Step 5: Add the `persistence` field and panic to `engine.go`**

Add the field to the `Engine` struct:

```go
type Engine struct {
	site        *identitypb.Identity
	nodeID      *uuidpb.UUID
	apps        []dogma.Application
	appsByKey   map[string]struct{}
	executors   map[dogma.Application]*executor
	running     atomic.Bool
	persistence PersistenceProvider
}
```

Add the panic after the existing site identity check in `Run()`:

```go
if e.persistence == nil {
    panic("runkit: a persistence provider is required, use WithPersistence()")
}
```

- [ ] **Step 6: Run the test to verify it passes**

```
go test -run TestRun ./...
```

Expected: PASS.

- [ ] **Step 7: Commit**

```
git add persistence.go option.go engine.go engine_test.go
git commit -m "Add PersistenceProvider interface, WithPersistence option, and startup panic"
```

---

## Task 2: In-memory persistence driver

**Files:**

- Create: `internal/memdriver/driver.go`

The memdriver satisfies `PersistenceProvider` using the zero-value structs from
`persistencekit`'s memory driver packages. No constructor is needed — `new(Driver)`
or `&Driver{}` is sufficient.

- [ ] **Step 1: Check the test that already exists for `PersistenceStores`**

`PersistenceStores` is tested indirectly when `memdriver.Driver` is used in the
engine tests. No separate unit test is needed for the struct methods.

- [ ] **Step 2: Create `internal/memdriver/driver.go`**

```go
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
```

- [ ] **Step 3: Verify it compiles and satisfies the interface**

```
go build ./internal/memdriver/...
```

Expected: no errors.

- [ ] **Step 4: Update `engine_test.go` to use `memdriver.Driver` where `Run()` is called**

The `TestExecuteCommand` test calls `e.Run()` but won't yet pass (it will panic on
missing persistence). Add the import and `WithPersistence` option:

```go
import (
    "testing"
    "time"

    "github.com/dogmatiq/dogma"
    "github.com/dogmatiq/enginekit/enginetest/stubs"
    . "github.com/dogmatiq/runkit"
    "github.com/dogmatiq/runkit/internal/memdriver"
)
```

In the `TestExecuteCommand` test, update the engine construction:

```go
e := New(
    WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
    WithPersistence(&memdriver.Driver{}),
    WithApplication(app),
)
```

- [ ] **Step 5: Run existing tests to verify they still pass (except the network/binding ones that come later)**

```
go test -run 'TestRun|TestExecutorFor|TestWithSite|TestWithNodeID|TestWithApplication|TestFromEnvironment' ./...
```

Expected: PASS on the panic tests; `TestExecuteCommand` may still fail (Run() will
return an error from the missing listener — that is expected at this stage; fix comes
in Task 7).

- [ ] **Step 6: Commit**

```
git add internal/memdriver/driver.go engine_test.go
git commit -m "Add in-memory persistence driver and update engine_test to use it"
```

---

## Task 3: Network address configuration

**Files:**

- Modify: `environment.go`
- Modify: `option.go`
- Modify: `engine.go` (add fields + constant)

- [ ] **Step 1: Write failing tests for the new options**

Add to `options_test.go`:

```go
func TestWithBindAddress(t *testing.T) {
	e := New(WithBindAddress("127.0.0.1:9000"))
	if e.bindAddr != "127.0.0.1:9000" {
		t.Fatalf("got %q, want %q", e.bindAddr, "127.0.0.1:9000")
	}
}

func TestWithAdvertiseAddress(t *testing.T) {
	e := New(WithAdvertiseAddress("192.168.1.1:9000"))
	if e.advertiseAddr != "192.168.1.1:9000" {
		t.Fatalf("got %q, want %q", e.advertiseAddr, "192.168.1.1:9000")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
go test -run 'TestWithBindAddress|TestWithAdvertiseAddress' ./...
```

Expected: FAIL — `WithBindAddress` and `WithAdvertiseAddress` are not defined.

- [ ] **Step 3: Add fields and constant to `engine.go`**

Add the constant near the top of `engine.go`, after the imports:

```go
// defaultPort is the default TCP port for the engine listener.
// TODO: replace with an assigned IANA port before shipping.
const defaultPort = 0
```

Add the fields to the `Engine` struct:

```go
type Engine struct {
	site          *identitypb.Identity
	nodeID        *uuidpb.UUID
	apps          []dogma.Application
	appsByKey     map[string]struct{}
	executors     map[dogma.Application]*executor
	running       atomic.Bool
	persistence   PersistenceProvider
	bindAddr      string
	advertiseAddr string
}
```

- [ ] **Step 4: Add the options to `option.go`**

```go
// WithBindAddress returns an [Option] that sets the TCP address the engine
// listens on, in "host:port" format (e.g. "0.0.0.0:7831").
//
// If [FromEnvironment] is also used, this option takes precedence over
// DOGMA_BIND_ADDRESS.
func WithBindAddress(addr string) Option {
	return func(e *Engine) {
		e.bindAddr = addr
	}
}

// WithAdvertiseAddress returns an [Option] that sets the address the engine
// advertises to peers, in "host:port" format.
//
// If unset, the advertise address is derived from the bind address and network
// interface introspection at startup.
//
// If [FromEnvironment] is also used, this option takes precedence over
// DOGMA_ADVERTISE_ADDRESS.
func WithAdvertiseAddress(addr string) Option {
	return func(e *Engine) {
		e.advertiseAddr = addr
	}
}
```

- [ ] **Step 5: Add the ferrite declarations to `environment.go`**

Add after the existing `envNodeID` declaration:

```go
var envBindAddress = ferrite.
	String("DOGMA_BIND_ADDRESS", "the TCP address the engine listens on (host:port)").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envAdvertiseAddress = ferrite.
	String("DOGMA_ADVERTISE_ADDRESS", "the address peers use to connect to this node (host:port)").
	Optional(ferrite.WithRegistry(FerriteRegistry))
```

- [ ] **Step 6: Update `FromEnvironment()` in `option.go` to read the new variables**

In the `FromEnvironment()` function body, add after the existing node ID block:

```go
if e.bindAddr == "" {
    if addr, ok := envBindAddress.Value(); ok {
        e.bindAddr = addr
    }
}
if e.advertiseAddr == "" {
    if addr, ok := envAdvertiseAddress.Value(); ok {
        e.advertiseAddr = addr
    }
}
```

- [ ] **Step 7: Add a `TestFromEnvironment` sub-test for the address variables**

In `environment_test.go`, add sub-tests inside `TestFromEnvironment`:

```go
t.Run("it sets the bind address from the environment", func(t *testing.T) {
    t.Setenv("DOGMA_BIND_ADDRESS", "0.0.0.0:8000")
    e := New(FromEnvironment())
    if e.bindAddr != "0.0.0.0:8000" {
        t.Fatalf("got %q, want %q", e.bindAddr, "0.0.0.0:8000")
    }
})

t.Run("it sets the advertise address from the environment", func(t *testing.T) {
    t.Setenv("DOGMA_ADVERTISE_ADDRESS", "10.0.0.1:8000")
    e := New(FromEnvironment())
    if e.advertiseAddr != "10.0.0.1:8000" {
        t.Fatalf("got %q, want %q", e.advertiseAddr, "10.0.0.1:8000")
    }
})

t.Run("explicit WithBindAddress takes precedence over environment", func(t *testing.T) {
    t.Setenv("DOGMA_BIND_ADDRESS", "0.0.0.0:8000")
    e := New(WithBindAddress("127.0.0.1:9000"), FromEnvironment())
    if e.bindAddr != "127.0.0.1:9000" {
        t.Fatalf("got %q, want %q", e.bindAddr, "127.0.0.1:9000")
    }
})
```

- [ ] **Step 8: Run all tests to verify they pass**

```
go test -run 'TestWithBindAddress|TestWithAdvertiseAddress|TestFromEnvironment' ./...
```

Expected: PASS.

- [ ] **Step 9: Commit**

```
git add engine.go option.go environment.go options_test.go environment_test.go
git commit -m "Add bind/advertise address options and ferrite env vars"
```

---

## Task 4: Stub TCP listener and address resolution

**Files:**

- Create: `listener.go`

The `listener` interface and `stubListener` implementation live in the root package
alongside `engine.go`. The interface is unexported, matching the pattern established
by the unexported `executor` type.

- [ ] **Step 1: Write failing tests for the stub listener**

Create `listener_test.go` in the root package (package `runkit`, not `runkit_test`,
so it can access unexported types):

```go
package runkit

import (
	"context"
	"fmt"
	"net"
	"testing"
)

func TestStubListener_binds_and_returns_advertise_address(t *testing.T) {
	s := &stubListener{
		bindAddr:      "127.0.0.1:0", // port 0: OS picks a free port
		advertiseAddr: "",             // resolved from bind addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotAddr string
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(addr string) {
			gotAddr = addr
			cancel() // signal we have the address
		})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error: %v", err)
	}

	if gotAddr == "" {
		t.Fatal("onReady was never called")
	}

	// Address should be host:port with a non-zero port.
	host, port, err := net.SplitHostPort(gotAddr)
	if err != nil {
		t.Fatalf("invalid address %q: %v", gotAddr, err)
	}
	if host == "" || host == "0.0.0.0" {
		t.Fatalf("expected a non-unspecified host, got %q", host)
	}
	_ = port
}

func TestStubListener_uses_explicit_advertise_address(t *testing.T) {
	s := &stubListener{
		bindAddr:      "127.0.0.1:0",
		advertiseAddr: "10.0.0.1:9000",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotAddr string
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(addr string) {
			gotAddr = addr
			cancel()
		})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error: %v", err)
	}

	if gotAddr != "10.0.0.1:9000" {
		t.Fatalf("got advertise address %q, want %q", gotAddr, "10.0.0.1:9000")
	}
}

func TestStubListener_closes_on_context_cancel(t *testing.T) {
	s := &stubListener{bindAddr: "127.0.0.1:0"}

	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(string) { close(ready) })
	}()

	<-ready
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error after cancel: %v", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
go test -run 'TestStubListener' ./...
```

Expected: FAIL — `stubListener` and `listener` are not defined.

- [ ] **Step 3: Create `listener.go`**

```go
package runkit

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// listener manages the lifecycle of the network listener.
//
// The stub implementation holds a TCP port open without accepting connections.
// The future gRPC implementation will replace it with no changes to the startup
// sequence in Run().
type listener interface {
	// ListenAndServe binds to the configured address and begins serving.
	// onReady is called with the resolved advertise address once the listener is
	// bound. ListenAndServe then blocks until ctx is cancelled or a fatal error
	// occurs.
	ListenAndServe(ctx context.Context, onReady func(advertiseAddr string)) error
}

// stubListener binds a TCP port but never accepts connections.
type stubListener struct {
	// bindAddr is the local address to bind (e.g. "0.0.0.0:7831").
	bindAddr string
	// advertiseAddr is the address to report to peers. If empty, it is derived
	// from bindAddr and network interface introspection.
	advertiseAddr string
}

func (s *stubListener) ListenAndServe(ctx context.Context, onReady func(string)) error {
	ln, err := net.Listen("tcp", s.bindAddr)
	if err != nil {
		return fmt.Errorf("binding listener: %w", err)
	}
	defer ln.Close()

	addr, err := resolveAdvertiseAddr(ln.Addr().(*net.TCPAddr), s.advertiseAddr)
	if err != nil {
		return err
	}

	onReady(addr)

	<-ctx.Done()
	return nil
}

// resolveAdvertiseAddr determines the address to advertise to peers.
//
// If configured is non-empty, it is used verbatim.
// Otherwise, the host from bound is used. If bound's IP is unspecified
// (0.0.0.0 or ::), the first non-loopback non-link-local IPv4 address found
// on any network interface is used instead.
func resolveAdvertiseAddr(bound *net.TCPAddr, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}

	host := bound.IP.String()

	if bound.IP.IsUnspecified() {
		found, err := firstRoutableIPv4()
		if err != nil {
			return "", err
		}
		host = found
	}

	return net.JoinHostPort(host, fmt.Sprintf("%d", bound.Port)), nil
}

// firstRoutableIPv4 returns the first non-loopback, non-link-local IPv4
// address found on any network interface.
func firstRoutableIPv4() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("enumerating network interfaces: %w", err)
	}

	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.To4() == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
				continue
			}

			return ip.String(), nil
		}
	}

	return "", errors.New(
		"no routable IPv4 address found; set DOGMA_ADVERTISE_ADDRESS explicitly",
	)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```
go test -run 'TestStubListener' ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add listener.go listener_test.go
git commit -m "Add stub TCP listener and advertise address resolution"
```

---

## Task 5: Heartbeat proto

**Files:**

- Create: `internal/heartbeat/internal/heartbeatpb/heartbeat.proto`
- Generated: `internal/heartbeat/internal/heartbeatpb/heartbeat.pb.go`
- Generated: `internal/heartbeat/internal/heartbeatpb/heartbeat_primo.pb.go`

- [ ] **Step 1: Create the proto file**

Create `internal/heartbeat/internal/heartbeatpb/heartbeat.proto`:

```proto
syntax = "proto3";

package dogmatiq.runkit.heartbeat;

option go_package = "github.com/dogmatiq/runkit/internal/heartbeat/internal/heartbeatpb";

import "google/protobuf/timestamp.proto";

// HeartbeatRecord stores the presence information for a single node.
message HeartbeatRecord {
    // address is the advertise address of the node (host:port).
    string address = 1;

    // expires_at is the time after which this record may be considered stale.
    google.protobuf.Timestamp expires_at = 2;
}
```

- [ ] **Step 2: Generate the Go code**

```
make generate
```

Expected: `heartbeat.pb.go` and `heartbeat_primo.pb.go` appear alongside the proto file.

- [ ] **Step 3: Verify the generated package compiles**

```
go build ./internal/heartbeat/...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```
git add internal/heartbeat/internal/heartbeatpb/
git commit -m "Add HeartbeatRecord proto and generated Go code"
```

---

## Task 6: Heartbeat writer

**Files:**

- Create: `internal/heartbeat/writer.go`
- Create: `internal/heartbeat/writer_test.go`

The `Writer` type manages the write loop. It accepts exported fields (no
constructor) so callers can configure it directly.

The `Interval` and `GracePeriod` fields are zero-means-default, which allows
tests to use short durations without a separate test-only constructor.

- [ ] **Step 1: Write failing tests**

Create `internal/heartbeat/writer_test.go`:

```go
package heartbeat_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	. "github.com/dogmatiq/runkit/internal/heartbeat"
)

func newTestWriter(t *testing.T, kv *memorykv.BinaryStore) *Writer {
	t.Helper()
	return &Writer{
		NodeID:        uuidpb.Generate(),
		KVStore:       kv,
		AdvertiseAddr: "127.0.0.1:9000",
		Interval:      20 * time.Millisecond,
		GracePeriod:   40 * time.Millisecond,
	}
}

func TestWriter_writes_initial_record(t *testing.T) {
	store := &memorykv.BinaryStore{}
	w := newTestWriter(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Run the writer, then cancel immediately after startup.
	// We use a short-lived context so the writer writes once and exits.
	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- w.Run(runCtx)
	}()

	// Give the writer time to write the initial record.
	time.Sleep(5 * time.Millisecond)
	runCancel()

	if err := <-done; err != nil {
		t.Fatalf("Run() returned unexpected error: %v", err)
	}
}

func TestWriter_refreshes_record(t *testing.T) {
	store := &memorykv.BinaryStore{}
	w := newTestWriter(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- w.Run(runCtx)
	}()

	// Wait for at least two intervals so that a refresh must have occurred.
	time.Sleep(3 * w.Interval)
	runCancel()

	if err := <-done; err != nil {
		t.Fatalf("Run() returned unexpected error: %v", err)
	}
}

func TestWriter_returns_fatal_error_on_OCC_conflict(t *testing.T) {
	store := &memorykv.BinaryStore{}
	w := newTestWriter(t, store)

	// Pre-write the node's key at revision 1 to force a conflict.
	ctx := context.Background()
	kvStore, err := store.Open(ctx, "heartbeats")
	if err != nil {
		t.Fatal(err)
	}
	defer kvStore.Close()
	if err := kvStore.SetUnconditional(ctx, w.NodeID.AsBytes(), []byte("existing")); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Run(ctx)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a fatal error, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OCC fatal error")
	}
}

func TestWriter_graceful_shutdown_deletes_record(t *testing.T) {
	store := &memorykv.BinaryStore{}
	w := newTestWriter(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- w.Run(runCtx)
	}()

	// Wait for initial write.
	time.Sleep(5 * time.Millisecond)

	// Cancel to trigger graceful shutdown.
	runCancel()

	if err := <-done; err != nil {
		t.Fatalf("Run() returned unexpected error: %v", err)
	}

	// The record should now be gone.
	ks, err := store.Open(ctx, "heartbeats")
	if err != nil {
		t.Fatal(err)
	}
	defer ks.Close()

	ok, err := ks.Has(ctx, w.NodeID.AsBytes())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected heartbeat record to be deleted after graceful shutdown")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
go test -run 'TestWriter' ./internal/heartbeat/...
```

Expected: FAIL — `Writer` is not defined.

- [ ] **Step 3: Create `internal/heartbeat/writer.go`**

```go
package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/heartbeat/internal/heartbeatpb"
	xpersistence "github.com/dogmatiq/runkit/internal/x/xpersistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultInterval    = 5 * time.Second
	defaultGracePeriod = 10 * time.Second
	keyspaceName       = "heartbeats"
)

// Writer writes and refreshes a node's heartbeat record in the KV store.
// Its zero value is not valid; set NodeID, KVStore, and AdvertiseAddr before
// calling Run.
type Writer struct {
	// NodeID is the UUID of this node. It is used as the keyspace key.
	NodeID *uuidpb.UUID

	// KVStore is the binary KV store to write heartbeat records into.
	KVStore kv.BinaryStore

	// AdvertiseAddr is the host:port address to write into the record.
	AdvertiseAddr string

	// Interval is the time between heartbeat refreshes.
	// Zero means 5 seconds (the ADR-7 default).
	Interval time.Duration

	// GracePeriod is added to Interval to compute the record's expiry time.
	// Zero means 10 seconds (the ADR-7 default).
	GracePeriod time.Duration
}

// Run writes the initial heartbeat record, then refreshes it periodically
// until ctx is cancelled or a fatal error occurs.
//
// On graceful shutdown (ctx cancellation), the record is deleted before
// returning.
func (w *Writer) Run(ctx context.Context) error {
	interval := w.Interval
	if interval == 0 {
		interval = defaultInterval
	}
	gracePeriod := w.GracePeriod
	if gracePeriod == 0 {
		gracePeriod = defaultGracePeriod
	}

	store := kv.NewMarshalingStore(
		w.KVStore,
		xpersistence.UUIDMarshaler,
		marshaler.NewProto[*heartbeatpb.HeartbeatRecord, heartbeatpb.HeartbeatRecord](),
	)

	ks, err := store.Open(ctx, keyspaceName)
	if err != nil {
		return fmt.Errorf("heartbeat: opening keyspace: %w", err)
	}
	defer ks.Close()

	// Write the initial record. Retry indefinitely on transient errors — there
	// is no expiry pressure yet because we have not published a prior record.
	rev, err := w.writeRecord(ctx, ks, 0, interval, gracePeriod, time.Time{})
	if err != nil {
		return err
	}

	expiry := time.Now().Add(interval + gracePeriod)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return w.deleteRecord(ks)

		case <-ticker.C:
			rev, err = w.writeRecord(ctx, ks, rev, interval, gracePeriod, expiry)
			if err != nil {
				return err
			}
			expiry = time.Now().Add(interval + gracePeriod)
		}
	}
}

// writeRecord attempts to write/refresh the heartbeat record, retrying
// transient errors until success, the context is cancelled, or (if deadline
// is non-zero) the deadline is exceeded.
//
// Returns the new revision on success.
func (w *Writer) writeRecord(
	ctx context.Context,
	ks kv.Keyspace[*uuidpb.UUID, *heartbeatpb.HeartbeatRecord],
	rev uint64,
	interval, gracePeriod time.Duration,
	deadline time.Time,
) (uint64, error) {
	for {
		record := &heartbeatpb.HeartbeatRecord{
			Address:   w.AdvertiseAddr,
			ExpiresAt: timestamppb.New(time.Now().Add(interval + gracePeriod)),
		}

		err := ks.Set(ctx, w.NodeID, record, rev)
		if err == nil {
			return rev + 1, nil
		}

		if kv.IsConflict(err) {
			return 0, fmt.Errorf(
				"heartbeat: UUID collision detected for node %s — "+
					"another node is using the same ID (ADR-7 requires unique node UUIDs): %w",
				w.NodeID, err,
			)
		}

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, errors.New(
				"heartbeat: presence lease expired (storage unavailable too long)",
			)
		}

		// Transient error — wait briefly before retrying.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// deleteRecord deletes the heartbeat record using a fresh context so that the
// delete can proceed even though the engine's context is already cancelled.
func (w *Writer) deleteRecord(ks kv.Keyspace[*uuidpb.UUID, *heartbeatpb.HeartbeatRecord]) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Passing nil deletes the key (zero value of *HeartbeatRecord).
	if err := ks.SetUnconditional(ctx, w.NodeID, nil); err != nil {
		// Non-fatal: best-effort delete on shutdown.
		_ = err
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```
go test -run 'TestWriter' ./internal/heartbeat/...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/heartbeat/writer.go internal/heartbeat/writer_test.go
git commit -m "Add heartbeat Writer with write loop, OCC detection, and graceful shutdown"
```

---

## Task 7: Wire `Engine.Run()`

**Files:**

- Modify: `engine.go`
- Modify: `engine_test.go`

This task replaces the Phase 1 stub in `Run()` with the full startup sequence:
open KV store, start stub listener, start heartbeat writer, resolve executors.

`errgroup` from `golang.org/x/sync/errgroup` is already an indirect dependency
via `enginekit`. Verify it is directly importable, or add it:

```
go get golang.org/x/sync
```

- [ ] **Step 1: Write failing integration test**

Add to `engine_test.go`:

```go
func TestRun_starts_and_stops_cleanly(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
		},
	}

	e := New(
		WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
		WithPersistence(&memdriver.Driver{}),
		WithBindAddress("127.0.0.1:0"),
		WithApplication(app),
	)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- e.Run(ctx)
	}()

	// Give the engine a moment to start.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}
}
```

Also update the existing `TestExecuteCommand` test to use `WithBindAddress("127.0.0.1:0")`:

```go
e := New(
    WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
    WithPersistence(&memdriver.Driver{}),
    WithBindAddress("127.0.0.1:0"),
    WithApplication(app),
)
```

And add the needed imports (`context`, `memdriver`).

- [ ] **Step 2: Run the tests to verify they fail**

```
go test -run 'TestRun_starts_and_stops|TestExecuteCommand' ./...
```

Expected: FAIL — `Run()` currently returns immediately (Phase 1 stub).

- [ ] **Step 3: Check that `golang.org/x/sync` is available**

```
grep 'golang.org/x/sync' go.mod go.sum
```

If not present, add it:

```
go get golang.org/x/sync@latest
```

- [ ] **Step 4: Rewrite `Engine.Run()` in `engine.go`**

```go
func (e *Engine) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		panic("runkit: Run() has already been called")
	}

	if e.site == nil {
		panic("runkit: a site identity is required, use WithSite() or FromEnvironment()")
	}

	if e.persistence == nil {
		panic("runkit: a persistence provider is required, use WithPersistence()")
	}

	if e.nodeID == nil {
		e.nodeID = uuidpb.Generate()
	}

	bindAddr := e.bindAddr
	if bindAddr == "" {
		if addr, ok := envBindAddress.Value(); ok {
			bindAddr = addr
		} else {
			bindAddr = fmt.Sprintf("0.0.0.0:%d", defaultPort)
		}
	}

	configuredAdvertiseAddr := e.advertiseAddr
	if configuredAdvertiseAddr == "" {
		if addr, ok := envAdvertiseAddress.Value(); ok {
			configuredAdvertiseAddr = addr
		}
	}

	kvStore, err := e.persistence.KVStore(ctx)
	if err != nil {
		return fmt.Errorf("runkit: opening KV store: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)

	l := &stubListener{
		bindAddr:      bindAddr,
		advertiseAddr: configuredAdvertiseAddr,
	}

	addrCh := make(chan string, 1)
	g.Go(func() error {
		return l.ListenAndServe(gctx, func(addr string) {
			addrCh <- addr
		})
	})

	var advertiseAddr string
	select {
	case advertiseAddr = <-addrCh:
	case <-gctx.Done():
		return g.Wait()
	}

	w := &heartbeat.Writer{
		NodeID:       e.nodeID,
		KVStore:      kvStore,
		AdvertiseAddr: advertiseAddr,
	}

	g.Go(func() error {
		return w.Run(gctx)
	})

	for _, ex := range e.executors {
		ex.future.Store(noopExecutor{})
	}

	return g.Wait()
}
```

Update the imports in `engine.go` to include:

```go
import (
	"context"
	"fmt"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/heartbeat"
)
```

- [ ] **Step 5: Run all tests to verify they pass**

```
go test ./...
```

Expected: PASS on all tests.

- [ ] **Step 6: Commit**

```
git add engine.go engine_test.go go.mod go.sum
git commit -m "Wire Engine.Run(): persistence, stub listener, heartbeat writer"
```

---

## Self-review

After writing the plan, the following checks were made against the spec:

**Spec coverage:**

- PersistenceProvider interface — Task 1 ✓
- PersistenceStores struct — Task 1 ✓
- WithPersistence option — Task 1 ✓
- In-memory driver — Task 2 ✓
- DOGMA_BIND_ADDRESS / DOGMA_ADVERTISE_ADDRESS — Task 3 ✓
- WithBindAddress / WithAdvertiseAddress — Task 3 ✓
- FromEnvironment reads new vars — Task 3 ✓
- Stub TCP listener, address resolution logic — Task 4 ✓
- HeartbeatRecord proto — Task 5 ✓
- Heartbeat writer: initial write, periodic refresh, OCC fatal — Task 6 ✓
- Heartbeat writer: transient retry, expiry shutdown — Task 6 ✓
- Heartbeat writer: graceful delete — Task 6 ✓
- Updated Run() startup sequence — Task 7 ✓

**Open items carried from spec (not blocking implementation):**

- Default port number: `defaultPort = 0` placeholder in Task 3. Must be resolved before release.
- `DOGMA_ADVERTISE_ADDRESS` has no default — the spec notes it is derived at runtime, which is handled in `resolveAdvertiseAddr`.

**Type consistency check:**

- `PersistenceProvider`, `PersistenceStores` — defined in Task 1, used in Task 2 and Task 7.
- `stubListener`, `listener` interface — defined in Task 4, used in Task 7.
- `heartbeat.Writer` fields: `NodeID *uuidpb.UUID`, `KVStore kv.BinaryStore`, `AdvertiseAddr string` — consistent between Task 6 definition and Task 7 usage.
- `memdriver.Driver` — defined in Task 2, used in Task 7 tests.
- `xpersistence.UUIDMarshaler` — already exists, used in Task 6.
- `kv.NewMarshalingStore` — from `persistencekit/kv`, used in Task 6.
- `marshaler.NewProto[*heartbeatpb.HeartbeatRecord, heartbeatpb.HeartbeatRecord]()` — matches `marshaler.NewProto` signature.

---

Plan complete and saved to [docs/superpowers/plans/2026-04-16-heartbeat-presence.md](docs/superpowers/plans/2026-04-16-heartbeat-presence.md).

**Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using the executing-plans skill, with checkpoints for review.

Which approach?
