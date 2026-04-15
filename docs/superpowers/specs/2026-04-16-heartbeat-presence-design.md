# Node Heartbeat and Presence Design

Date: 2026-04-16

## Overview

This document describes the first-cut implementation of the node heartbeat and
presence system defined in [ADR-7], along with the groundwork needed to support
it: persistence injection, network address configuration, and the stub TCP
listener that establishes the startup handoff pattern for the future RPC layer.

## Scope

This milestone implements:

- The `PersistenceProvider` interface and `PersistenceStores` helper struct
- The `WithPersistence(PersistenceProvider)` engine option
- An in-memory persistence driver, for use in tests
- `DOGMA_BIND_ADDRESS` and `DOGMA_ADVERTISE_ADDRESS` environment variables,
  plus `WithBindAddress` and `WithAdvertiseAddress` engine options
- A stub TCP listener that binds, resolves the advertise address, and then
  sits idle — establishing the startup contract for the future gRPC layer
- The `HeartbeatRecord` proto message
- The heartbeat writer: initial write, periodic refresh, graceful shutdown
  delete, and fatal error handling
- Updated `Engine.Run()` startup sequence wiring all of the above together

Out of scope:

- Any production persistence driver (DynamoDB, PostgreSQL, etc.)
- Environment variable-based driver configuration
- The heartbeat reader and live node set construction
- Real gRPC serving
- Real command routing (executor still resolves to `noopExecutor`)

## Persistence injection

### `PersistenceProvider` interface

```go
type PersistenceProvider interface {
    KVStore(ctx context.Context)      (kv.BinaryStore, error)
    JournalStore(ctx context.Context) (journal.BinaryStore, error)
    SetStore(ctx context.Context)     (set.BinaryStore, error)
}
```

The engine calls these methods during `Run()` to obtain stores, passing its
root context. Methods may return an error if the provider cannot produce the
store — for example, if it needs to establish a connection or validate
configuration. The interface is satisfied implicitly — persistence driver
packages implement the three methods on their own types without importing or
referencing runkit.

### `PersistenceStores` struct

```go
type PersistenceStores struct {
    KV      kv.BinaryStore
    Journal journal.BinaryStore
    Set     set.BinaryStore
}

func (s PersistenceStores) KVStore(_ context.Context)      (kv.BinaryStore, error)      { return s.KV, nil }
func (s PersistenceStores) JournalStore(_ context.Context) (journal.BinaryStore, error) { return s.Journal, nil }
func (s PersistenceStores) SetStore(_ context.Context)     (set.BinaryStore, error)     { return s.Set, nil }
```

This is a convenience type for operators who want to assemble stores from
different drivers. It satisfies `PersistenceProvider` via the three methods
above.

### `WithPersistence` option

```go
func WithPersistence(p PersistenceProvider) Option
```

Stores the provider on the engine. `Run()` panics if no provider has been
configured, consistent with the existing site identity check.

### In-memory driver

A thin internal wrapper around persistencekit's `driver/memory` stores,
satisfying `PersistenceProvider`. Used in tests and in the in-process
single-node case. Lives in `internal/memdriver` or similar. Not part of the
public API.

## Network address configuration

Two new environment variables, declared via ferrite alongside the existing
`DOGMA_SITE_KEY`, `DOGMA_SITE_NAME`, and `DOGMA_NODE_ID` declarations:

- `DOGMA_BIND_ADDRESS` — the address the engine listens on (e.g.
  `0.0.0.0:7831`). Defaults to `0.0.0.0:<default-port>`.
- `DOGMA_ADVERTISE_ADDRESS` — the address peers are told to connect to.
  If unset, derived from `DOGMA_BIND_ADDRESS` and interface introspection
  (see below).

Two corresponding options are also provided for programmatic configuration:

```go
func WithBindAddress(addr string) Option
func WithAdvertiseAddress(addr string) Option
```

If `FromEnvironment()` is also used, explicit options take precedence, consistent
with the existing pattern.

### Default port

Open question: assign a default port number. It must be an unregistered IANA
port. The value is a constant in the codebase; agree on it before implementation
begins.

### Advertise address resolution

The resolved advertise address is computed at `Run()` time in this order:

1. `DOGMA_ADVERTISE_ADDRESS` (or `WithAdvertiseAddress`) — used as-is if set.
2. If not set: take the host from `DOGMA_BIND_ADDRESS`. If the bind host is
   unspecified (`0.0.0.0` or `::`) enumerate non-loopback, non-link-local
   unicast IPv4 addresses on all network interfaces and take the first one.
   Combine with the bind port to form the advertise address.
3. If interface introspection yields no suitable address, `Run()` returns a
   fatal error.

## Stub TCP listener

A stub TCP listener is introduced to establish the startup contract between
the network layer and the heartbeat writer. Its sole purpose is to bind to
the configured address and return the resolved advertise address.

```go
// listener is the internal interface satisfied by both the stub and the
// future gRPC implementation.
type listener interface {
    // ListenAndServe binds to the configured address and begins serving.
    // It returns the resolved advertise address as "host:port".
    ListenAndServe(ctx context.Context) (advertiseAddr string, err error)
}
```

The stub implementation:

- Creates a `net.Listener` bound to the bind address
- Resolves the advertise address using the logic above
- Holds the listener open but never calls `Accept()`
- Closes the listener when the context is cancelled

The future gRPC server satisfies the same interface, replacing the stub
with no changes to the startup sequence.

## Heartbeat record proto schema

A new proto file, e.g. `internal/proto/heartbeatpb/heartbeat.proto`:

```proto
syntax = "proto3";

import "google/protobuf/timestamp.proto";

message HeartbeatRecord {
    string address = 1;
    google.protobuf.Timestamp expires_at = 2;
}
```

The generated Go code lives in `internal/proto/heartbeatpb/`. The project's
existing protoc makefile setup handles generation via `make generate`.

## Heartbeat writer

Lives in `internal/heartbeat`. A single type, `Writer`, owns the write loop.

### Keyspace

The writer opens the `"heartbeats"` keyspace from the KV store provided by
the persistence driver. Keys are 16-byte node UUID binary representations
(using the existing `xpersistence.UUIDMarshaler`). Values are
`HeartbeatRecord` proto messages (using `marshaler.NewProto`).

### Timing constants (per ADR-7)

- Heartbeat interval: 5 seconds
- Grace period: 10 seconds
- Record expiry: `now + heartbeat_interval + grace_period` (15 seconds from
  time of write)

### Write loop behavior

1. **Initial write** — writes the heartbeat record before signalling
   readiness. Blocks `Run()` until success. Uses `Keyspace.Set()` with
   revision `0` (new key).
2. **Periodic refresh** — every 5 seconds, refreshes the record by calling
   `Keyspace.Set()` with the current revision.
3. **OCC conflict** — treated as fatal: another node is using the same UUID,
   indicating misconfiguration. Cancels the engine's root context with an
   error.
4. **Transient write failure** — retried continuously. If the last committed
   record expires before a successful write, the engine shuts down: continuing
   to operate after expiry would disrupt peers that have already excluded this
   node.
5. **Graceful shutdown** — on context cancellation, deletes the heartbeat
   record using `Keyspace.SetUnconditional()` with a zero value before
   returning.

### Expired record pruning

When reading the keyspace (future milestone), any peer that encounters an
expired record may delete it. This is not implemented in this milestone.

## Updated `Engine.Run()` startup sequence

1. Validate configuration: site identity present, persistence provider
   present.
2. Open the `"heartbeats"` KV keyspace from the persistence driver.
3. Start the stub TCP listener; get the resolved advertise address.
4. Write the initial heartbeat record (with the resolved address). Block
   until success or fatal error.
5. Start the heartbeat refresh goroutine.
6. Resolve `executor.future` for all registered apps with `noopExecutor`
   (unchanged from current behavior — real routing is a future milestone).
7. Block until context cancelled or fatal error.
8. On shutdown: delete the heartbeat record, close the listener.

## Error handling

Fatal errors surface as the return value of `Run()`. Two categories:

- **Misconfiguration** — OCC conflict on heartbeat write. Indicates two nodes
  sharing the same UUID. `Run()` returns immediately with a descriptive error.
- **Storage liveness** — expiry window exhausted without a successful write.
  `Run()` returns with an error indicating the node was unable to maintain its
  heartbeat.

Transient errors are logged and retried silently within their respective retry
windows.

## File layout

```
internal/
  heartbeat/
    writer.go       -- Writer type and write loop
    writer_test.go
  memdriver/
    driver.go       -- In-memory PersistenceProvider implementation
  proto/
    heartbeatpb/
      heartbeat.proto
      heartbeat.pb.go        -- generated
      heartbeat_primo.pb.go  -- generated (primo plugin)
persistence.go      -- PersistenceProvider interface, PersistenceStores struct
option.go           -- WithPersistence, WithBindAddress, WithAdvertiseAddress
environment.go      -- DOGMA_BIND_ADDRESS, DOGMA_ADVERTISE_ADDRESS ferrite vars
engine.go           -- updated Run() sequence
```

<!-- references -->

[ADR-7]: ../../adr/0007-node-heartbeat.md
