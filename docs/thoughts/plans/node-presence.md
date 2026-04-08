# Plan: Node Presence and Live Set Maintenance

This document describes the planned implementation of the node presence
subsystem. The design rationale and formal decision will be captured in
ADR-0007. This plan covers the implementation structure.

## What it is

Every node in the cluster needs a consistent, eventually-convergent view of
which other nodes currently exist and how to reach them. This view has two
consumers:

- **Rendezvous hashing** -- takes a `[]*uuidpb.UUID` slice and independently
  computes the same workload-to-node assignment as every other node.
- **Ranked instruction routing** -- dials a peer's gRPC address to offer an
  instruction.

The presence subsystem is the source of truth for both. It does not make
routing decisions; it supplies the inputs those decisions require.

## Goals

Four properties must hold:

1. **Live-set supply** -- the set of live node UUIDs is available in memory to
   any part of the engine that needs it, with no KV access on the hot path.
2. **Address discovery** -- the gRPC address of any live node is available
   alongside its UUID.
3. **Dead-node detection** -- when a node stops making progress (crashes,
   loses storage, is scaled down), it is removed from the live set within a
   bounded time. This removal is the trigger for recovery index adoption
   ([ADR-0006]).
4. **Graceful departure** -- a node that shuts down cleanly removes itself
   promptly, without waiting for the staleness window to expire.

## Package

`internal/subsystem/presence`

## Presence record

Each node writes a single record to a cluster-wide KV keyspace, keyed by its
own node UUID:

| Field | Type | Notes |
|---|---|---|
| `grpc_address` | string | Dialable address for inter-node gRPC |
| `written_at` | timestamp | Wall clock time of the write |

Nothing else. Additional fields will not be added unless there is a
demonstrated need; a node that encounters a record missing an expected field
should treat that node as incompatible and exclude it from the live set.

## Write discipline

Each node writes its presence record every **W = 5 seconds**. The write is
a full overwrite -- there is no append or CAS involved; the node is the sole
writer of its own record.

On graceful shutdown, the node deletes its record before stopping its gRPC
server. This allows peers to detect the departure on their next refresh cycle
rather than waiting up to T + R seconds for the record to go stale.

## Read discipline and staleness

Each node scans the full presence keyspace every **R = 5 seconds** and updates
its in-memory live set. An entry is excluded from the live set if:

```
written_at + T < now     where T = 15s
```

T = 3 * W, so a live node must miss **three consecutive writes** before it
is excluded. This tolerates up to two missed writes from GC pauses, transient
storage latency, or scheduling jitter without false eviction.

The maximum time between a node stopping and its peers excluding it is
**T + R = 20 seconds**. This lag is acceptable: OCC ([ADR-0003]) ensures
correctness during any transition window, and the ranked routing fallback
([ADR-0004]) routes around dead peers without requiring a current live set.

**Why T and R are not the same value**: a node could write at time 0, have
peers refresh at time 1 (seeing a fresh record), write at time W, and have
peers refresh at time W+1. Using the same value for both would create a race
between write timing and scan timing that could produce false evictions under
load. Keeping T >= 3W decouples them.

## Collaborative GC

A node that has crashed or been permanently removed will never delete its own
record. To prevent indefinite accumulation:

Any reader that encounters a stale entry (one excluded by the staleness
check) **deletes it** as a fire-and-forget background operation. This is
purely cosmetic -- the entry is already excluded from the live set -- but it
keeps the keyspace bounded even as nodes come and go over time.

## In-memory live set

The result of each refresh scan is an immutable snapshot exposed to the rest
of the engine:

```go
// Snapshot is an immutable view of the live node set at a point in time.
type Snapshot struct {
    // Nodes is the full live node UUID set, safe to pass directly to
    // rendezvous hashing. Includes the local node.
    Nodes []*uuidpb.UUID

    // Addr returns the gRPC address for the given node UUID.
    // Returns "" if the node is not in this snapshot.
    Addr func(*uuidpb.UUID) string
}
```

Snapshots are produced atomically at the end of each refresh scan and
distributed to subscribers. No KV access occurs on the hot path; all
rendezvous scoring and gRPC dialing work from the latest snapshot.

## Adoption trigger

When a refresh scan produces a snapshot whose node set is a strict subset of
the previous snapshot (i.e., one or more nodes have disappeared), the
departing node UUIDs are reported to the recovery subsystem. The recovery
subsystem then checks whether the local node is now the rendezvous winner
for any workloads indexed in the departed node's recovery index, and if so,
begins adoption.

The presence subsystem is responsible only for detecting the set difference
and reporting it. The adoption logic belongs to [ADR-0006].

> The adoption trigger fires on KV staleness, not on gRPC failure. A node
> that is network-unreachable but still writing presence records is still
> committing work. Triggering adoption in that case would race a working node
> to its own recovery index -- correct under OCC but unnecessary and
> semantically wrong. KV staleness means the node has either crashed or lost
> storage; either way it cannot commit new work, so adoption is safe.

## Dismissed approaches

**Gossip / SWIM** is dismissed on two independent grounds:

1. *Reachability is pairwise, not global.* SWIM propagates "X is unreachable"
   as a cluster-wide fact, but reachability is a property of a specific node
   pair. Node A may be unable to reach X while node B has a working
   connection. Propagating A's observation causes B to skip X in its offer
   sequence when B could route to X correctly -- misrouting work based on a
   false generalisation.

2. *SWIM detects the wrong condition for adoption.* SWIM fires on network
   unreachability; adoption must fire when a node has stopped committing work
   to shared storage. A network-unreachable node may still hold a storage
   connection and be draining its in-memory queue. Composing SWIM on top of KV
   cannot accelerate the safe adoption window: arriving faster than T + R
   requires either accepting adoption races (node still working) or
   re-introducing a global reachability view, which is the false generalisation
   already dismissed.

**External service registry** (etcd, Consul, ZooKeeper) would provide
authoritative membership with TTL-based expiry, but introduces a dependency
outside `persistencekit` and a new operational concern. The engine is designed
to require only one external dependency (the persistence backend).

**gRPC health probing** has a circular dependency: probing requires addresses,
addresses require presence, presence requires probing. It also detects network
reachability rather than storage liveness, which is the wrong condition for
adoption.

**Leader-managed registry** requires leader election, which [ADR-0005]
explicitly rules out.

## Out of scope

- gRPC server startup ordering and the sequence in which presence is
  established relative to serving traffic (Phase 10).
- Recovery index adoption logic (belongs to Phase 3/4 and [ADR-0006]).
- Circuit breaker behavior during offer iterations (belongs to
  `internal/routing` -- see the ranked instruction routing plan).
- Proto definitions for the presence record wire format.
- Concrete timing values as tunable configuration (defaults are fixed;
  tuning is a future concern).

## Dependencies

- `github.com/dogmatiq/persistencekit` -- KV keyspace for the presence
  store
- `github.com/dogmatiq/enginekit/protobuf/uuidpb` -- UUID type
- `internal/x/xpersistence` -- marshaling helpers

No dependency on any routing, subsystem, or gRPC package. The presence
subsystem is a foundation layer; nothing it depends on should depend on it.

<!-- references -->

[ADR-0002]: ../../adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-0003]: ../../adr/0003-optimistic-conflict-resolution.md
[ADR-0004]: ../../adr/0004-ranked-instruction-routing.md
[ADR-0005]: ../../adr/0005-homogeneous-cluster-nodes.md
[ADR-0006]: ../../adr/0006-durable-command-executor.md
