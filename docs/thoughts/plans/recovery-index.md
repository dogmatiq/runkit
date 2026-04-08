# Plan: Recovery Index

This document describes the planned implementation of the recovery index as a
standalone internal package. The recovery index schema and semantics are fully
specified by [ADR-0006]. No design decisions remain; this is an implementation
plan.

## What it is

The recovery index is a per-node KV store that records which aggregate instances
and integration handlers have pending work on a given node. It is the mechanism
by which a restarting node (or a node adopting a dead peer's work) locates all
unfinished handler executions without scanning every handler's data store in the
cluster.

ADR-0006 specifies two entry types:

- **Aggregate entry:** `(node, app_key, handler_key, instance_id)`
- **Integration entry:** `(node, app_key, handler_key)`

An entry is written before the first command is appended to the handler's data
store and removed when no pending commands remain. One entry covers all pending
commands for a handler instance -- there is no per-command tracking in the index.

Index entries must be written *before* the corresponding data store write. A
crash between the two leaves an index entry with no pending work behind it, which
is safe to clean up on recovery. The reverse order would leave pending work with
no index entry pointing to it.

## Package

`internal/subsystem/recoveryindex`

The package is self-contained. It owns a KV keyspace and exposes a narrow
interface used by the aggregate and integration subsystems.

## Storage

One KV keyspace, scoped to the owning node via the key prefix. Both entry types
share the keyspace; a one-byte type discriminator distinguishes them.

```
key  = <node_uuid> / <type> / <app_key> / <handler_key> [/ <instance_id>]
value = <empty> | <version>
```

The value carries a version counter so that idempotent writes can be detected
without a read-then-write. Alternatively the value is empty and writes are
unconditional -- the entry's existence is the only meaningful fact.

> Open question: does the value need to carry anything beyond existence? The
> startup recovery procedure only needs to know that an entry exists; the
> contents of the data store carry the actual work. An empty value is simplest.
> Decide when implementing.

## Interface

```go
// Index tracks which aggregate instances and integration handlers have work
// in progress on a node.
type Index struct { ... }

// AggregateEntry identifies an aggregate instance with work in progress.
type AggregateEntry struct {
    Node       *uuidpb.UUID
    AppKey     *uuidpb.UUID
    HandlerKey *uuidpb.UUID
    InstanceID string
}

// IntegrationEntry identifies an integration handler with work in progress.
type IntegrationEntry struct {
    Node       *uuidpb.UUID
    AppKey     *uuidpb.UUID
    HandlerKey *uuidpb.UUID
}

func (x *Index) WriteAggregate(ctx context.Context, e AggregateEntry) error
func (x *Index) DeleteAggregate(ctx context.Context, e AggregateEntry) error
func (x *Index) WriteIntegration(ctx context.Context, e IntegrationEntry) error
func (x *Index) DeleteIntegration(ctx context.Context, e IntegrationEntry) error

// IterateNode calls fn for each entry belonging to node.
func (x *Index) IterateNode(
    ctx context.Context,
    node *uuidpb.UUID,
    fn func(context.Context, AggregateEntry) error,
    fn2 func(context.Context, IntegrationEntry) error,
) error
```

> The iteration signature is awkward with two callbacks. One option is a
> sum-type entry: `type Entry interface { isEntry() }` with concrete
> `AggregateEntry` and `IntegrationEntry`. Another is a single callback that
> receives an `Entry` interface. Decide when implementing; prefer whichever
> the persistencekit iteration pattern suits.

## Startup recovery procedure

On startup each node calls `IterateNode` with its own node UUID and, for each
entry, drives the handler's data store scan. This is the responsibility of the
aggregate and integration subsystems, not the recovery index package itself --
the index package only provides storage.

Dead-node adoption (surviving node iterates a dead peer's index) uses the same
`IterateNode` call with the dead node's UUID. The adoption mechanism itself is
out of scope for this package.

## Out of scope

- Handler data store scanning (aggregate/integration subsystems)
- Dead-node detection and adoption trigger (node registry / failover)
- Rerouting and quarantine (each handler subsystem)
- Any per-command tracking -- the index is per-handler-instance

## Dependencies

- `github.com/dogmatiq/persistencekit` -- KV keyspace
- `github.com/dogmatiq/enginekit/protobuf/uuidpb` -- UUID type

No dependency on the aggregate or integration subsystems.

<!-- references -->

[ADR-0006]: ../../adr/0006-durable-command-executor.md
