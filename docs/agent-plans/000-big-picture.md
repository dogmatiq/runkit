# runkit — Big Picture Architecture Plan

This document captures the high-level architecture and phased implementation roadmap for runkit.
It is a living plan; sections should be updated as decisions are revised or open questions are
resolved.

> **Terminology rule**: terms defined in this project are internal design vocabulary only.
> No term may appear in the public API (exported identifiers, option names, method names) or
> in user-facing documentation (godoc comments, README, error messages) unless its use in that
> context has been explicitly agreed.

**Related documents:**

- [Glossary](../glossary.md) — definitions of runkit-specific terms
- [Architecture Decision Records](../adr/README.md) — rationale for key decisions
- [Dogma glossary](https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md) — terms
  defined by the Dogma framework

## Overview

runkit is a horizontally scalable, multi-node Dogma engine. Key properties:

- **Distributed**: nodes share a persistence backend; no single point of coordination or
  failure. All nodes are peers; workload is assigned via rendezvous hashing over the
  live node set. See [ADR-0002](../adr/0002-rendezvous-hashing-for-workload-assignment.md).
- **Cloud-native persistence**: all state passes through `persistencekit` abstractions (journal,
  kv, set), making the engine backend-agnostic (PostgreSQL, DynamoDB, S3, in-memory).
- **OCC as the correctness primitive**: optimistic concurrency control on journal `Append`
  positions is the sole mechanism for correctness under concurrent execution. No distributed
  locks, no leader election.
- **Rendezvous hashing for routing**: workload→node assignment is computed from the live
  membership set, never stored. Any node can independently determine the current owner of any
  unit of work.
- **Multi-application**: multiple `dogma.Application` instances may be hosted by a single
  engine cluster; all storage is namespaced by application key.
- **Full durability**: every command is recorded in a per-command journal before execution. The
  engine provides ACID-like guarantees at the command level.
- **Inter-node gRPC**: command forwarding and event streaming use gRPC services.

---

## Foundational Design Decisions

### OCC is the correctness primitive; serialisation is the liveness optimisation

Two nodes may transiently believe they own the same instance during membership changes. This
does not corrupt state: journal `Append` at a given position succeeds for exactly one concurrent
writer; the other receives `ConflictError`, reloads state, and retries. Work may be duplicated
but never lost or corrupted.

In-process serialisation (one goroutine per active instance) eliminates the retry loop when
routing is stable, turning OCC from a recovery mechanism into a rarely-exercised safety net.

### Routing goal: warm in-memory instance state

The primary routing goal is to keep aggregate instance state warm on the node that will execute
commands for that instance, so that the common path involves zero storage reads and OCC is never
exercised. This is achieved by routing directly and consistently by instance UUID:

```
owner = rendezvous_hash(uuid5(app_key, instance_id), live_node_uuids)
```

Providing a stable node UUID (via `WithNodeID` / `DOGMA_NODE_ID`) ensures that a restarting node
re-enters the live set with the same UUID and thus continues to own the same instances it warmed
before the restart.

### Two orthogonal routing domains

**Command routing** assigns commands to nodes by handler instance. The routing key depends on
the handler type — see [ADR-0002](../adr/0002-rendezvous-hashing-for-workload-assignment.md).

**Partition ownership** assigns event-side work to nodes by partition UUID. The two domains
scale independently.

### Instance–stream binding is permanent

An aggregate instance is bound to a stream partition on the first successful event-producing
command and the binding never changes, guaranteeing that all events from the same aggregate
instance are delivered to consumers in order on the same stream.

---

## State Inventory

### Cluster-wide

| Store     | Key         | Value                          | Lifetime |
| --------- | ----------- | ------------------------------ | -------- |
| Heartbeat | `node_uuid` | `{ gRPC_address, updated_at }` | TTL      |

The heartbeat store is the **only** cluster-wide persistent structure. Everything else is scoped
to `(app, ...)`.

### Aggregate command path

| Store                      | Key                               | Type        | Lifetime                   |
| -------------------------- | --------------------------------- | ----------- | -------------------------- |
| Command backlog            | `(app, partition, command_uuid)`  | `Set[UUID]` | Until completion           |
| Poison backlog             | `(app, partition, command_uuid)`  | `Set[UUID]` | Until restart trickle-back |
| Command journal            | `(app, command_uuid)`             | Journal     | Until completion           |
| Aggregate instance journal | `(app, handler_key, instance_id)` | Journal     | Permanent (truncated)      |
| Snapshot                   | `(app, handler_key, instance_id)` | KV          | Until superseded           |
| Stream                     | `(app, stream_partition)`         | Journal     | Permanent                  |

### Integration command path

| Store           | Key                     | Type        | Lifetime                             |
| --------------- | ----------------------- | ----------- | ------------------------------------ |
| Command backlog | (shared with aggregate) | `Set[UUID]` | Until completion                     |
| Command journal | (shared with aggregate) | Journal     | Until completion                     |
| Handler journal | `(app, handler_key)`    | Journal     | Permanent (MinimizeConcurrency only) |
| Idempotency     | `(app, command_uuid)`   | KV          | Configurable retention               |

### Event-side (per partition)

| Store                 | Key                                  | Type    | Lifetime      |
| --------------------- | ------------------------------------ | ------- | ------------- |
| Stream                | `(app, stream_partition)`            | Journal | Permanent     |
| Process state         | `(app, handler_key, instance_id)`    | KV      | Until `End()` |
| Timeout journal       | `(app, handler_key, partition_uuid)` | Journal | Until `End()` |
| Projection checkpoint | handler's own store                  | —       | Permanent     |

---

## Phased Implementation

### Phase 1 — Engine Skeleton

Package root (`github.com/dogmatiq/runkit`).

Establish the public API surface. At this stage the engine compiles and can be instantiated, but
`Run()` returns `nil` immediately (no-op stub).

```go
func New(opts ...Option) *Engine

func (e *Engine) ExecutorFor(app dogma.Application) dogma.CommandExecutor
func (e *Engine) Run(ctx context.Context) error
```

Options: `WithApplication`, `FromEnvironment`, `WithNodeID`.

Persistence options (`WithJournals`, `WithKeyspaces`, `WithSets`) are introduced in Phase 2.

Error convention: programmer mistakes → panic; runtime failures → returned by `Run()`.

---

### Phase 2 — Node Registry

Package `internal/subsystem/noderegistry`.

Heartbeat kv keyspace (cluster-wide, TTL-based). All nodes write
`{ gRPC_address, updated_at }` keyed by node UUID every N seconds. The live node set is the
primary input to rendezvous hashing and gRPC endpoint discovery.

---

### Phase 3 — Aggregate Subsystem

Package `internal/subsystem/aggregate`.

Implements the aggregate command lifecycle.

**Execution per command:**

1. Router forwards command to accepting node.
2. Add `command_uuid` to command backlog (self-affinity partition).
3. Append to command journal at position 0 (dedup). **Acceptance point.**
4. Dispatch to instance-owning node.
5. Load instance: read aggregate instance journal tail → binding + offset hint + expected
   position. Read snapshot → application state. Read stream from offset hint → catch up.
6. Execute `HandleCommand()`.
7. Append to aggregate instance journal at expected position (OCC). `ConflictError` → retry
   from step 5.
8. Commit events to stream partition owner.
9. Finalize: truncate instance journal, write snapshot, delete command journal, remove from
   command backlog.

**Failure path:** in-memory retry counter, backoff. After N consecutive failures → move to
poison backlog. On restart → trickle poison backlog back to command backlog.

**Serialisation:** one goroutine per active instance with channel queue.

---

### Phase 4 — Integration Subsystem

Package `internal/subsystem/integration`.

Shares the command backlog and command journal with the aggregate subsystem. The distinction
between `MaximizeConcurrency` and `MinimizeConcurrency` is purely about routing and ordering.

| Preference            | Routing key                                            | Ordering        |
| --------------------- | ------------------------------------------------------ | --------------- |
| `MaximizeConcurrency` | `rendezvous(uuid5(app_key, command_uuid), live_nodes)` | None            |
| `MinimizeConcurrency` | `rendezvous(uuid5(app_key, handler_key), live_nodes)`  | Handler journal |

An idempotency kv prevents double-execution during preference transitions and after restarts.

---

### Phase 5 — Event Stream Subsystem

Package `internal/subsystem/eventstream`.

The event stream is a data layer: it owns stream journals and serves `ConsumeAPI`. It has no
handler awareness.

- Any node accepts `ConsumeAPI` requests and proxies to the partition owner.
- Server-side event type filtering.
- Consumers maintain their own checkpoints.

---

### Phase 6 — Process Subsystem

Package `internal/subsystem/process`.

Process handlers execute on the partition-owning node (the node that owns the stream). There is
no per-instance routing for processes — event delivery is always local to the stream owner.

**Execution per event:**

1. Stream owner evaluates `RouteEventToInstance()` locally.
2. Load process state from kv.
3. Call `HandleEvent()` or `HandleTimeout()`.
4. Execute produced commands via normal command forwarding (may cross nodes).
5. Append produced timeouts to timeout journal `(app, handler_key, partition_uuid)`.
6. Persist updated process state via CAS write.

**Timeout scheduler:** runs on partition-owning node. Timeout journals are keyed by
`(app, handler_key, partition_uuid)` — same partition UUID as the stream. On partition
reassignment the new owner inherits the timeout journals.

---

### Phase 7 — Projection Subsystem

Package `internal/subsystem/projection`.

No engine-side storage. Checkpoint offsets are owned by the projection handler via the Dogma OCC
contract.

Each node subscribes to the partitions it owns via rendezvous. Each
`(projection_handler, partition)` pair is processed by exactly one node at a time. OCC prevents
duplicates.

---

### Phase 8 — Poison Backlog

Package `internal/subsystem/poisonqueue`.

Partitioned `Set[UUID]` with the same shape as the command backlog. On startup, entries trickle
back to the command backlog. A reference implementation exists in
`_internal/subsystem/poisonqueue` — review for consistency with current patterns.

---

### Phase 9 — Inter-Node gRPC

Runkit-specific proto service definitions live in `internal/grpc/`. `enginekit` is for
engine-agnostic abstractions.

- **CommandForwardingService** — routes commands from router to accepting node, and from
  accepting node to instance-owning node.
- **ConsumeAPI** — serves event streams, proxied to partition owner.
- **Stream append** — executing node sends `AppendRequest` to partition owner.

Node discovery: gRPC addresses are read from the heartbeat store.

---

### Phase 10 — Engine Wiring

Complete the `Engine` skeleton from Phase 1 by wiring all subsystems together.

**Startup sequence:**

1. Resolve node identity (`WithNodeID` / `DOGMA_NODE_ID` / random).
2. Discover partition set.
3. Resolve persistence stores (`With*` options / environment).
4. Start heartbeat goroutine.
5. Start membership refresh goroutine.
6. Construct and wire all subsystems.
7. Start all subsystems under shared `errgroup`.
8. Drain pre-startup command queue.

**Graceful shutdown:** drain in-flight commands, flush checkpoints, remove heartbeat entry.

---

## Build Order

```
Phase 1 (engine skeleton) — establishes public API; compiles immediately

Phase 2 (node registry)
  ├─→ Phase 3 (aggregate)
  │     └─→ Phase 5 (event stream)
  │           ├─→ Phase 6 (process + timeouts)
  │           └─→ Phase 7 (projection)
  └─→ Phase 4 (integration)
        └─→ Phase 5 (event stream) [shared dependency]

Phase 8 (poison backlog) — implement when Phase 3/4 patterns are established
Phase 9 (inter-node gRPC) — can be developed in parallel with Phases 2–7
Phase 10 (engine wiring) — last; wires all subsystems into the Phase 1 skeleton
```

---

## Open Questions

### 1. Adding nodes

Adding a node changes rendezvous output, causing some work to migrate. Routing re-converges
once the new node's heartbeat propagates. No cluster-wide coordination is required.

### ~~2. Stable node identity mechanism~~ _(resolved)_

`WithNodeID(id uuid.UUID)` / `DOGMA_NODE_ID`. See [ADR-0002](../adr/0002-rendezvous-hashing-for-workload-assignment.md).

### 3. Handler metadata retention for removed handlers

When a handler is removed from an application's config, its historical instance data remains in
storage. The engine must retain metadata until the handler is both absent from the current config
AND its journals are fully idle. The exact GC policy is deferred.

### 4. Idempotency key and Dogma ADR-29

Dogma ADR-29 proposes removing `WithIdempotencyKey()` on scalability grounds. In runkit's design
this concern may not apply: the command journal is already keyed by command UUID, and supporting
an idempotency key could be as simple as using the key as the journal key instead. Assess before
Phase 4.

### 5. MinimizeConcurrency strictness during membership transitions

During transitions, two nodes may transiently both believe they own the handler's partition. OCC
prevents corruption but does not prevent momentary concurrent execution. Whether this violates
the intent of `MinimizeConcurrency` is deferred.

### 6. Snapshot GC policy

Retain recent snapshots inversely proportional to replay cost. The exact formula is deferred to
Phase 3.

### 7. Partition discovery mechanism

Partition UUIDs must be known to every node so that rendezvous hashing can assign ownership.
Two approaches are possible:

- **Deterministic derivation**: compute partition UUIDs from `(app_key, partition_count)` using
  a formula (e.g. `uuid5`). No storage read needed, but changing `partition_count` is awkward.
- **Keyspace enumeration**: store partition UUIDs in a keyspace and read them at startup. More
  flexible (partitions can be added/removed/split independently), but adds a startup read and
  introduces a consistency window while the keyspace propagates.

Resolve before Phase 5.

### 8. WithEventObserver completion detection

`ExecuteCommand` supports `WithEventObserver[T]` (Dogma ADR-30). The implementation is entirely
ephemeral — observer baggage is embedded in the command envelope and flows through the causal
chain. Completion detection is incremental:

- **V1**: client timeout (`ErrEventObserverNotSatisfied`).
- **V2**: heuristic (no process routes → chain exhausted → notify immediately).
- **V3**: full causal tree tracking (commands spawned / completed counters).

---

## Existing Code: Status and Role

Previous implementation attempts have been archived to `_internal/` (excluded from the build
by Go's `_`-prefix convention). Only the stable helper packages under `internal/x/` remain active.

**Active packages:**

| Package                   | Status | Role                                              |
| ------------------------- | ------ | ------------------------------------------------- |
| `internal/partition`      | Active | Rendezvous hashing with self-affinity.            |
| `internal/x/xtelemetry`   | Stable | Telemetry constants.                              |
| `internal/x/xpersistence` | Stable | Protobuf marshaling helpers.                      |
| `internal/x/xtesting`     | Stable | Failable stores for property-based chaos testing. |

**Archived in `_internal/`** (reference only — do not import):

| Package                           | Notes                                                      |
| --------------------------------- | ---------------------------------------------------------- |
| `_internal/partition`             | Earlier rendezvous hash implementation.                    |
| `_internal/subsystem/eventstream` | Journal-backed event stream. Reference, not template.      |
| `_internal/subsystem/poisonqueue` | Global kv-backed failed-command store. Review for Phase 8. |

---

## Key External Dependencies

| Dependency                           | Role                                                           |
| ------------------------------------ | -------------------------------------------------------------- |
| `github.com/dogmatiq/dogma`          | Handler interfaces, message types, `CommandExecutor`           |
| `github.com/dogmatiq/enginekit`      | Config resolution, envelope protobuf, gRPC service definitions |
| `github.com/dogmatiq/persistencekit` | journal, kv, set abstractions; all backends                    |
| `github.com/cespare/xxhash/v2`       | Hash function for rendezvous hashing                           |
| `pgregory.net/rapid`                 | Property-based testing                                         |
| `go.opentelemetry.io/otel`           | Observability (via enginekit telemetry)                        |
