# runkit — Big Picture Architecture Plan

This document captures the high-level design decisions and phased implementation roadmap for
runkit, produced through an extended design conversation in April 2026. It is a living plan;
sections should be updated as decisions are revised or open questions are resolved.

## Overview

runkit is a horizontally scalable, multi-node Dogma engine. Key properties:

- **Distributed**: nodes share a persistence backend; no single point of coordination or
  failure. Both persistent and ephemeral node roles are supported (see [Node](#node)).
- **Cloud-native persistence**: all state passes through `persistencekit` abstractions (journal,
  kv, set), making the engine backend-agnostic (PostgreSQL, DynamoDB, S3, in-memory).
- **OCC as the correctness primitive**: optimistic concurrency control on journal `Append`
  positions is the sole mechanism for correctness under concurrent execution. No distributed
  locks, no leader election.
- **Rendezvous hashing for routing**: workload→node assignment is computed from the live
  membership set, never stored. Any node can independently determine the current owner of any
  unit of work.
- **Multi-application**: multiple `dogma.Application` instances may be hosted by a single
  engine cluster; all storage is namespaced by application UUID.
- **Full durability**: every command is recorded in a per-command journal before execution. The
  engine provides ACID-like guarantees at the command level.
- **Inter-node gRPC**: command forwarding, event streaming, and cross-partition process delivery
  use gRPC services defined in `enginekit` (following the existing `eventstreamgrpc` pattern).

---

## Terminology

> Terms already defined in [`dogmatiq/dogma` glossary][dogma-glossary] are not redefined here.
> The terms below are specific to the engine implementation layer.

[dogma-glossary]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md

### Node

A single running process of the runkit engine. There are two kinds:

**Persistent node**: an operator-configured stable UUID that survives restarts. Sponsors exactly
one partition (whose UUID equals the node's own UUID). Writes to the
[heartbeat keyspace](#heartbeat-keyspace) like all other nodes. Participates in routing as a
viable candidate for any partition.

**Ephemeral node**: assigned a randomly generated UUID at each startup — not reused across
restarts. Never sponsors a partition. Participates in command execution like a persistent node
(rendezvous can assign partitions to it) but leaves no long-term storage footprint. Useful for
batch or short-lived deployments that must not permanently grow the partition set.

All nodes — persistent and ephemeral — write to the [heartbeat keyspace](#heartbeat-keyspace)
and are thus part of the live node routing pool.

The mechanism by which an operator assigns a stable UUID to a persistent node (e.g. environment
variable, config file, secrets manager) is a deployment concern, outside the scope of the engine
itself.

### Partition

The routing unit for aggregate and process handler execution, and the unit of recovery ownership.
Each persistent node sponsors exactly one partition, whose UUID equals that node's UUID.
Partitions are created on first use (the partition registry entry is written when the first
command is processed for that partition in a given app/subsystem pairing).

The node responsible for a given partition is computed dynamically:

```
owner = rendezvous_hash(partition_uuid, live_node_uuids)
```

Ownership is **never stored**; any node computes it independently. OCC on the underlying journals
provides correctness when ownership is transiently disputed during membership changes.

The rendezvous hash has a special case: `input_uuid == candidate_uuid` always wins (existing
behaviour in the `internal/partition` package). Because partition UUID = sponsor node UUID, a
persistent node always wins the rendezvous for its own partition when it is live, providing
natural home-node affinity.

Aggregate, process, and event stream subsystems all share the **same** partition UUID set (the
live persistent nodes). A dogma `StreamID` equals the partition UUID of the corresponding
persistent node; there is one event stream per persistent node. This guarantees that aggregate
and process instances sharing the same instance ID scheme always reside on the same node, and
that event stream assignment uses the same rendezvous candidate set as work routing.

> **Terminology note**: the `internal/subsystem/eventstream` package uses "partition" internally
> for each stream segment — one partition per `dogma.StreamID` — because `StreamID` == partition
> UUID. In this document, "partition" unqualified means the work-routing unit; "stream" means
> the event fan-out journal. They share UUIDs but serve different roles. Whether to rename the
> eventstream package internals is tracked in Open Question 10.

### Rendezvous hashing

The algorithm used to assign partitions and event streams to nodes. For each candidate UUID, a
score `hash(input_uuid ∥ candidate_uuid)` is computed; the candidate with the highest score is
the owner. Any participant with the same candidate set computes the same result independently,
with no coordination. Membership changes cause only the minimum necessary reassignment.

### Serialisation (concurrency)

Processing a sequence of operations one at a time, in order. **Not** marshaling or encoding.

Each aggregate and process _instance_ is a serialisation unit: commands or events for a given
instance are processed by a single goroutine with an in-process channel queue. OCC on the
underlying journal provides a correctness backstop if two nodes transiently believe they own the
same partition.

Serialisation is a **liveness** concern, not a correctness concern. OCC alone is sufficient for
correctness; serialisation prevents the unbounded retry loop that results from contention.

### Per-command execution journal

A per-`(app, command_uuid)` append-only journal that records the full lifecycle of a single
command execution, regardless of which handler type processes the command. The key deliberately
excludes the handler key and instance ID — these are recorded _inside_ the journal.

Three invariants are load-bearing; the exact record schema is an implementation detail for
Phase 3:

1. **A record is written before handler invocation.** This lets recovery detect in-flight
   commands independently of the pending-commands set scan.
2. **The routing decision `{handler_key, instance_id}` is written before execution begins.**
   Changes to `RouteCommandToInstance()` in application code can never re-route an in-flight
   command; the decision is immutable once written.
3. **Completion is detectable without reading records.** When a command completes, the journal
   is truncated (begin-offset advanced past all records). A journal with `begin > 0` and no
   readable records unambiguously signals completion — no record enumeration required. This is
   safe because all persistencekit backends preserve begin-offset metadata after truncation.

### Three-layer discovery model

The mechanism by which a node finds work it is responsible for and resumes it after a crash:

```
Layer 1 — Which partitions do I own?
          rendezvous_hash(partition_uuid, live_node_set) [purely computed]

Layer 2 — Which commands are in-flight in subsystem S, partition P?
          pending-commands set keyed by (app, subsystem, partition_uuid) [per-subsystem]

Layer 3 — What has been decided for command C?
          per-command execution journal keyed by (app, command_uuid) [universal]
```

Layer 1 is stateless. Layers 2 and 3 together form the durability guarantee: a command appears
in the set before its journal is created (set-first ordering), preventing an invisible-work gap.

### Instance–stream binding

A record of which event stream an aggregate instance's events are stored on. Established on the
**first successful event-producing command** for that instance and never changed thereafter.

The binding is stored as part of the aggregate snapshot — snapshots carry the bound partition
UUID (= `dogma.StreamID`) alongside application state. There is no separate kv store.

If no snapshot is available, read the instance's own event journal
(keyed by `(app, handler_key, instance_id)`). The bound partition UUID is in the first record.
Snapshots are purely a replay-cost optimisation; the instance journal is the canonical durable
record.

### Heartbeat keyspace

The **only** cluster-wide persistent store. Written by every live node at a regular interval:

```
node_uuid → { gRPC_address, updated_at }
```

A node whose record has not been refreshed within a configured TTL window is considered dead and
is excluded from the live node set.

The live node set (all non-stale entries) is the primary input to rendezvous hash calculations
and to gRPC endpoint discovery. Both persistent and ephemeral nodes participate as candidates
in rendezvous: routing to a node uses all live nodes; routing to a partition uses all live nodes
with the partition UUIDs (input set) drawn from the partition registry.

High write frequency (every N seconds for every live node).

### Partition registry

A per-`(app, subsystem)` set of partition UUIDs that have ever had work within that
app/subsystem pairing. Because partition UUID = sponsoring persistent node UUID, entries are
always persistent node UUIDs. The entry for a given `(app, subsystem, partition_uuid)` is
written on first use (when any node first processes work for that partition). Entries are never
removed.

Used exclusively during recovery: a recovering node consults the partition registry to enumerate
every partition UUID that may have in-flight commands pending, then checks the pending-commands
set for each.

Ephemeral nodes service existing partitions but do not create new ones, so they never add new
UUIDs to this registry. A node may appear in registries for different subsystems at different
times.

### Pending-commands set

A per-`(app, subsystem, partition_uuid)` persistent set of command UUIDs that have been received
but not yet completed. Written (set-first) **before** the per-command journal is created — this
is the invariant that makes Layer 2 of the discovery model reliable.

Scoped per subsystem so that each subsystem can recover independently without examining another
subsystem's in-flight work. This also provides subsystem-level fault isolation: a subsystem can
crash and be restarted without affecting other subsystems running in the same process.

### Idempotency keyspace (integration)

A per-app kv keyspace recording completed integration commands:

```
command_uuid → completion_record
```

Acts as the cross-cutting guard that prevents double-execution when the same command appears in
both the `MaximizeConcurrency` and `MinimizeConcurrency` execution paths simultaneously (e.g.
during a preference transition). Entries are GC'd after a configurable retention window.

### Poison queue

A durable store of commands that could not be successfully executed after all retry attempts,
together with identifying information about the failed handler. Keyed by command message UUID.
Shared across all nodes for a given application; no partition assignment.

TODO: when is the poison queue retried, if at all?

---

## Foundational Design Decisions

### OCC is the correctness primitive; serialisation is the liveness optimisation

Two nodes may transiently believe they own the same partition during membership changes. This
does not corrupt state: journal `Append` at a given position succeeds for exactly one concurrent
writer; the other receives `ConflictError`, reloads state, and retries. Work may be duplicated
but never lost or corrupted.

In-process serialisation (one goroutine per active instance) eliminates the retry loop when
routing is stable, turning OCC from a recovery mechanism into a rarely-exercised safety net.

### Partition UUID = sponsor node UUID

A persistent node sponsors exactly one partition, identified by its own UUID. This collapses two
previously separate concepts (node identity and partition identity) into one. It also means the
partition set grows naturally as persistent nodes are added, without any separate partition
configuration.

The rendezvous hash special case (`input == candidate` wins) ensures a persistent node is the
preferred home for its own partition while the node is live. If the node is absent, the partition
migrates to the highest-scoring live candidate via normal rendezvous.

Ephemeral nodes do not sponsor their own partition; they may receive rendezvous assignments for
other partitions while live, but leave no permanent storage footprint beyond whatever partition
registry entries and pending-commands sets they accumulate (which are recoverable by other nodes).

### Partition ownership is computed, never stored

The assignment of partitions to nodes is a pure function of `(partition_uuid, live_node_uuids)`.
It is never written to storage, never communicated between nodes, and requires no coordination.
Nodes independently derive the same result from the same inputs.

### Routing decisions are recorded before execution

Before calling any handler, the engine writes a `routing_decided {handler_key, instance_id}`
record to the per-command journal (position 1). This makes the routing decision durable and
immutable. Future changes to `RouteCommandToInstance()` and `RouteEventToInstance()` in
application code cannot affect in-flight commands, and recovery always re-executes with the
original routing.

### The per-command journal is universal for all handler types

All commands — whether targeting an aggregate, integration, or process handler — are executed
against the same per-`(app, command_uuid)` journal schema. The handler key and instance ID live
inside the journal records, not in the storage key. This decouples the durability layer from the
application handler taxonomy.

### Event streams and partitions share the same UUID space

A dogma `StreamID` equals the runkit partition UUID of the corresponding persistent node.
Partitions, event streams, and persistent nodes all use a single UUID namespace. Adding a
persistent node adds a routing partition and an event stream simultaneously.

The roles served remain distinct even though the UUIDs are shared:

- **Partitions** are the unit of work assignment: rendezvous selects which node executes a
  given aggregate or process instance.
- **Streams** (= stream journals keyed by `(app, partition_uuid)`) are the unit of event
  fan-out: projections and process handlers consume events by reading stream journals
  sequentially.

The instance–stream binding assigns an aggregate instance to a specific partition UUID for
event fan-out, using `rendezvous_hash(derive_uuid(instance_id), partition_uuids)` — the same
candidate set as for work routing. If the executing node does not own the bound stream
(partition), the event write is a cross-node journal append — an acceptable normal-case
operation.

### Instance–stream binding is permanent

An aggregate instance is bound to a stream on the first successful event-producing command and
the binding never changes, guaranteeing that all events from the same aggregate instance are
delivered to projections in order on the same stream.

The binding is durably recoverable from the instance's own event journal (the stream UUID,
which equals a partition UUID, is recorded in the first event entry). Snapshots cache the
binding as an optimisation but are not the authoritative source. Stream assignment at binding
time uses `rendezvous_hash(derive_uuid(instance_id), partition_uuids)` — the same candidate
set as partition ownership. The permanence of the binding means the assignment strategy can
change in future without affecting correctness for existing instances.

### Integration has two execution schemas based on ConcurrencyPreference

`dogma.IntegrationMessageHandler` declares either `MaximizeConcurrency` or `MinimizeConcurrency`.
These require different routing and ordering:

| Preference            | Routing key                                             | Ordering mechanism                                  | Parallelism                             |
| --------------------- | ------------------------------------------------------- | --------------------------------------------------- | --------------------------------------- |
| `MaximizeConcurrency` | `rendezvous(derive_uuid(command_uuid), live_nodes)`     | None (per-command journals are independent)         | Unbounded goroutine pool per handler    |
| `MinimizeConcurrency` | `rendezvous(derive_uuid(handler_key_uuid), live_nodes)` | Single ordered handler journal `(app, handler_key)` | Single sequential goroutine per handler |

Both schemas use the universal per-command journal for execution. The handler journal under
`MinimizeConcurrency` is an additional ordering layer, not a replacement for the per-command
journal.

**Switching preference without losing commands**: the recovery path always reads both schemas.
The preference controls only the write path. Old-schema commands drain naturally; new commands
are written to the new schema. The idempotency keyspace prevents double-execution during the
transition window.

---

## Cluster-Wide State

The heartbeat keyspace is the **only** cluster-wide (non-app-scoped) persistent store:

| Store              | Key         | Value                          | Lifetime | Write frequency                 |
| ------------------ | ----------- | ------------------------------ | -------- | ------------------------------- |
| Heartbeat keyspace | `node_uuid` | `{ gRPC_address, updated_at }` | TTL      | Every N seconds, all live nodes |

Everything else is scoped to `(app, ...)` or `(app, subsystem, ...)`.

---

## Subsystem State Inventory

| Store                         | Storage key                        | Type     | Lifetime                        | Growth driver                  |
| ----------------------------- | ---------------------------------- | -------- | ------------------------------- | ------------------------------ |
| Heartbeat                     | `node_uuid`                        | kv (TTL) | Ephemeral                       | Live node count                |
| Partition registry            | `(app, subsystem, node_uuid)`      | set      | Permanent                       | Nodes × subsystems             |
| Pending-commands set          | `(app, subsystem, partition_uuid)` | set      | Until completion                | In-flight throughput × latency |
| Per-command execution journal | `(app, command_uuid)`              | journal  | Until completion + GC           | Same                           |
| Instance event journal        | `(app, handler_key, instance_id)`  | journal  | Permanent                       | Business entity count          |
| Event stream journal          | `(app, partition_uuid)`            | journal  | Permanent                       | Event volume                   |
| Instance–stream binding       | in snapshot / instance journal     | —        | Permanent (in journal)          | Business entity count          |
| Aggregate snapshot            | `(app, handler_key, instance_id)`  | kv       | Until superseded or GC          | Snapshotted instances          |
| Integration handler journal   | `(app, handler_key)`               | journal  | Permanent (MinimizeConcurrency) | Command volume                 |
| Integration idempotency       | `(app, command_uuid)`              | kv       | Configurable retention window   | Integration throughput         |
| Process state                 | `(app, handler_key, instance_id)`  | kv       | Until End()                     | Active workflow count          |
| Process timeout journal       | `(app, handler_key, instance_id)`  | journal  | Until End()                     | Active workflow count          |
| Projection checkpoint         | handler's own store                | —        | Permanent                       | Handlers × active streams      |
| Poison queue                  | `(app, command_uuid)`              | kv       | Until consumed                  | Failed command accumulation    |

---

## Phased Implementation

### Phase 1 — Engine Skeleton

Package root (`github.com/dogmatiq/runkit`).

Establish the public API surface that all subsequent phases will fill in. At this stage the
engine compiles and can be instantiated, but `Run()` returns `nil` immediately (no-op stub).

**Constructor:**

```go
func New(opts ...Option) *Engine
```

**Engine methods:**

```go
// ExecutorFor returns a CommandExecutor for the given application.
// Panics if app was not registered with WithApplication.
// Commands submitted before Run() is called are queued internally and
// drained once the engine starts.
func (e *Engine) ExecutorFor(app dogma.Application) dogma.CommandExecutor

// Run starts the engine and blocks until ctx is cancelled or a fatal error
// occurs. All runtime and environmental failures are returned here.
func (e *Engine) Run(ctx context.Context) error
```

**Option type and built-in options:**

```go
type Option func(*engine)

// WithApplication registers an application with the engine.
// Panics if app is nil or already registered.
func WithApplication(app dogma.Application) Option

// FromEnvironment configures infrastructure from environment variables.
// Records intent only; env vars are read and resources constructed during
// Run(). Any slot already set by an explicit With* option is skipped.
func FromEnvironment() Option

// WithNodeID sets the stable node UUID for this engine instance.
// If not set, FromEnvironment() checks the RUNKIT_NODE_ID env var.
// If still unset, Run() assigns a random ephemeral UUID.
func WithNodeID(id uuid.UUID) Option
```

Persistence options (`WithJournals`, `WithKeyspaces`, `WithSets`) are **not** defined here;
they are introduced in Phase 2 when the first storage-backed subsystem is built.

**Error convention:**

- Programmer mistakes (nil app, duplicate registration, unregistered app in `ExecutorFor`) → panic.
- Runtime and environmental failures → returned by `Run()`.

No internal config-wrapping package is created speculatively. Each subsystem defines what it
needs from `dogma.Application` when it is built; shared abstractions emerge from that rather
than being pre-designed.

---

### Phase 2 — Node Registry

Package `internal/subsystem/noderegistry`.

**Heartbeat kv keyspace** (cluster-wide, TTL-based):

- All nodes write `{ gRPC_address, updated_at }` keyed by node UUID every N seconds.
- Live node set = all entries where `updated_at + TTL >= now`.
- All live nodes (persistent and ephemeral) are rendezvous candidates for both command routing
  and partition ownership. The partition UUID input set comes from the partition registry, not
  from filtering the heartbeat.
- Consulted by every node on every membership refresh cycle to compute rendezvous hashes and
  locate gRPC endpoints.
- High write frequency; must remain fast.
- This is the **only** cluster-wide persistent structure.

---

### Phase 3 — Aggregate Subsystem

Package `internal/subsystem/aggregate`.

**Storage**:

- Per-`(app, command_uuid)` **per-command execution journal** — universal durability record,
  written before execution (see [Per-command execution journal](#per-command-execution-journal)).
- Per-`(app, subsystem, partition_uuid)` **pending-commands set** — set-first durability anchor
  (see [Three-layer discovery model](#three-layer-discovery-model)).
- Per-`(app, subsystem, node_uuid)` **partition registry** — first-use record for recovery
  enumeration (see [Partition registry](#partition-registry)).
- Per-`(app, handler_key, instance_id)` **instance event journal** — permanent, per-instance
  event history; the authoritative source for aggregate state replay and stream binding recovery.
- Per-`(app, partition_uuid)` **event stream journal** — fan-out stream consumed by projections
  and processes; events are written here (after the instance journal) keyed by the bound
  partition UUID (= `dogma.StreamID`).
- Per-`(app, handler_key, instance_id)` **snapshot kv** — optional; bounds replay cost on OCC
  retry. An aggregate handler that embeds `dogma.NoSnapshotBehavior` omits this.
- No separate instance–stream binding store — the binding is held in the aggregate snapshot
  (see [Instance–stream binding](#instance-stream-binding)).

**Execution per command**:

1. Receive command (locally or forwarded via gRPC from another node).
2. **Set-first**: add command UUID to the aggregate subsystem's pending-commands set for the
   target partition.
3. Open per-command journal; append `received` record.
4. Call `RouteCommandToInstance()` → instance ID. Append `routing_decided {handler_key,
instance_id}` to journal. This decision is now immutable for this command.
5. On first use of this partition, write partition registry entry.
6. Load aggregate state: read latest snapshot; replay events from the instance event journal
   since the snapshot offset (or from the beginning if no snapshot).
7. Call `AggregateMessageHandler.HandleCommand()`.
8. Resolve instance–stream binding: read from snapshot if present; otherwise read from the
   instance's own event journal (stream UUID is in the first record).
9. Append produced events to the event stream journal using OCC. On `ConflictError`, reload
   state from step 6 and retry. Snapshot bounds the cost of each reload.
10. Append a completion checkpoint to per-command journal.
11. Truncate per-command journal to advance begin-offset (signals completion).
12. Remove command UUID from pending-commands set.
13. Write snapshot: mandatory on the first event-producing command (capturing the stream binding
    alongside application state); periodic thereafter based on a configurable event threshold.
    GC policy retains the most recent snapshot; older snapshots may be pruned inversely
    proportional to replay cost.
14. On non-retriable failure: route the command to the poison queue.

**Serialisation**: one goroutine per active instance, with an in-process channel queue. Multiple
instances within the same partition execute concurrently. The node processes only instances
whose partition `rendezvous_hash(partition_uuid, live_node_uuids)` resolves to this node.

---

### Phase 4 — Integration Subsystem

Package `internal/subsystem/integration`.

Integration uses the same universal per-command execution journal and two-layer discovery as the
aggregate subsystem. The distinction between `MaximizeConcurrency` and `MinimizeConcurrency` is
purely about routing and ordering — not the journal structure.

**MaximizeConcurrency** (default) routing:

- Routing key: `rendezvous_hash(derive_uuid(command_uuid), live_node_uuids)`.
- Commands are independent; execute concurrently on an unbounded goroutine pool per handler.
- No additional ordering layer beyond the per-command journal.

**MinimizeConcurrency** routing and ordering:

- Routing key: `rendezvous_hash(derive_uuid(handler_key_uuid), live_node_uuids)`.
- Commands are appended to an additional ordered handler journal `(app, handler_key)` that
  provides sequencing. A single goroutine per handler drains in position order.
- The per-command journal is still written (universal record); the handler journal is the
  ordering mechanism on top.

**Execution (both schemas)**:

1. Receive command (locally or forwarded via gRPC).
2. **Set-first**: add command UUID to the integration subsystem's pending-commands set for the
   target partition.
3. For `MinimizeConcurrency`: append command reference to handler journal.
4. Open per-command journal; append `received`. Append `routing_decided {handler_key}`.
5. Check idempotency kv — if already complete: clean up and return.
6. On first partition use, write partition registry entry.
7. Call `IntegrationMessageHandler.HandleCommand()`. Must tolerate at-least-once invocation.
8. Append produced events to the event stream.
9. Append completion records to per-command journal; truncate.
10. Write completion record to idempotency kv.
11. Remove from pending-commands set / advance handler journal (schema-dependent cleanup).
12. On non-retriable failure: route to poison queue.

**Recovery** (run on startup and periodically):

- Consult partition registry to enumerate all partitions for this app/subsystem.
- Scan pending-commands set for each partition; for each command UUID, open its per-command journal
  to determine current state; resume from the last completed position.
- Scan handler journal for `MinimizeConcurrency`: resume from last incomplete position.
- The idempotency kv guards against double-execution during transitions and after restarts.

**Preference transitions**: both schemas are always readable by the recovery path. New commands
are written to the schema matching the current preference. The idempotency kv prevents
double-execution during the transition window. No migration step is required.

---

### Phase 5 — Event Fan-out

Package `internal/subsystem/eventdispatch`.

After events are appended to any stream by the aggregate or integration subsystems:

- Fans out to local process and projection subsystems via in-process channels.
- Fans out to remote nodes that have subscribed to this stream via gRPC streaming (using the
  `ConsumeAPI` service in `enginekit`).
- Tracks a per-stream watermark (last dispatched offset). Per-consumer checkpoints are
  maintained by each consumer, not here.

---

### Phase 6 — Process Subsystem

Package `internal/subsystem/process`.

**Storage**:

- Per-`(app, handler_key, instance_id)` **state kv** — `ProcessRoot` serialised state, CAS
  writes.
- Per-`(app, handler_key, instance_id)` **timeout journal** — pending scheduled timeouts with
  due-at times.

**Partition**: same rendezvous over the live persistent node set as the aggregate subsystem.
Aggregate and process instances sharing the same instance ID scheme are guaranteed to reside on
the same node.

**Execution per event**:

1. Call `RouteEventToInstance()` → instance ID. Record this routing decision durably before
   execution (analogous to `routing_decided` in the aggregate path) so that future code changes
   cannot re-route an in-flight event.
2. Compute owning node via `rendezvous_hash(derive_uuid(instance_id), live_node_uuids)`.
3. If remote: forward event to the owning node via ProcessEventService gRPC.
4. Load process state from state kv (CAS revision).
5. Call `Handle()` or `HandleTimeout()`.
6. Enqueue produced commands by writing to the appropriate aggregate pending-commands set /
   per-command journal, or to the integration execution path.
7. Append produced timeouts to the timeout journal.
8. Persist updated process state with CAS write; retry on revision conflict.
9. If `End()` called: delete state kv entry and truncate timeout journal.

**Timeout scheduler**: one goroutine per process handler (not per partition) manages timeouts
for that handler across all partitions owned by the node. Because the handler set is small and
fixed, this is inexpensive. Each goroutine maintains an in-memory priority queue of due times
and sleeps until the next timeout fires, rather than polling at a fixed tick interval. On
startup, all overdue timeouts are enqueued immediately.

---

### Phase 7 — Projection Subsystem

Package `internal/subsystem/projection`.

**Storage**: none in the engine. Checkpoint offsets are owned entirely by the projection handler
via the dogma OCC contract (typically stored atomically alongside the projection's own read-model
in the handler's database). The engine holds no checkpoint state.

**Consumption**: subscribes to ALL streams, both locally (in-process channels from event fan-out)
and remotely (gRPC `ConsumeAPI` to the owning node for each remote stream). Because the stream
set is small and fixed (one per persistent node), subscribing to all streams is inexpensive.

**Execution per event**:

1. Call `CheckpointOffset(streamID)` on the handler to determine the resume offset for this
   stream.
2. Deliver events in order from that offset.
3. Call `HandleEvent()`. The handler must atomically apply the event to its own data store and
   update its checkpoint to `event.offset + 1`. Returns `cp`, the new checkpoint offset.
4. If `cp == event.offset + 1`: event was applied; advance to the next event.
5. If `cp != event.offset + 1`: OCC conflict — another node or goroutine already processed this
   event (or the handler skipped it). Resume delivery from `cp`.
6. A non-nil error indicates a runtime failure; back off and retry.

**Compaction**: `Compact()` is called periodically in a background goroutine per handler. The
engine supplies a `ProjectionCompactScope`; the handler decides what to prune.

---

### Phase 8 — Poison Queue

Package `internal/subsystem/poisonqueue`.

A reference implementation exists in `_internal/subsystem/poisonqueue`. Global per application;
no partition assignment. Any node may enqueue a failed command. Keyed by command message UUID.

Review the archived implementation for consistency with the patterns established in earlier
phases before writing the new implementation.

---

### Phase 9 — Inter-Node gRPC

Runkit-specific proto service definitions live in runkit itself (e.g. `internal/grpc/`), not
in `enginekit`. `enginekit` is for engine-agnostic abstractions; the services below are
internal runkit coordination protocols.

The existing `eventstreamgrpc` package in `enginekit` may be reused as-is for event stream
consumption — it is engine-agnostic. Assess its API coverage against runkit's requirements
before implementing; extend it in `enginekit` only if the extension is also engine-agnostic,
otherwise implement runkit-specific consumption support in `internal/grpc/`.

**CommandForwardingService** (`internal/grpc/`):

- Accepts a command and its routing metadata from a gateway or remote node.
- The receiving node is the computed partition owner; it journals and executes the command.
- Used by the aggregate and integration subsystems.

**Event stream consumption** (via `enginekit/grpc/eventstreamgrpc` or `internal/grpc/`):

- Stream-owner nodes expose their event journals for remote consumers.
- Used by projections and by the process subsystem to receive events from non-local streams.

**ProcessEventService** (`internal/grpc/`):

- Used when a process instance's owning node differs from the node that received an event.
- Forwards the event envelope to the process-owning node.

**Node discovery**: gRPC addresses are read from the heartbeat keyspace. No separate discovery
service is required.

---

### Phase 10 — Engine Wiring

Complete the `Engine` skeleton from Phase 1 by wiring all subsystems together.

**Startup sequence inside `Run()`:**

1. **Resolve node identity**: use `WithNodeID` value if set; otherwise check the `RUNKIT_NODE_ID`
   env var (if `FromEnvironment()` was applied); otherwise assign a random ephemeral UUID.
2. **Resolve persistence stores**: use values from explicit `With*` options; for any slot still
   unset and `FromEnvironment()` was applied, construct a store from the corresponding DSN env
   var.
3. Start background **heartbeat** goroutine: writes `{ gRPC_address, updated_at }` to the node
   registry keyspace every N seconds.
4. Start background **membership refresh** goroutine: reads the heartbeat keyspace periodically;
   recomputes rendezvous hashes; notifies subsystems of topology changes.
5. **Construct all subsystems** and wire their in-process channels.
6. **Start all subsystems** under a shared `errgroup`.
7. **Drain the pre-startup command queue**: replay any commands submitted via `ExecutorFor`
   before `Run()` was called into the live execution path.

**`dogma.CommandExecutor` implementation** (wired here, callable from Phase 1):

- Identify target handler via message-type routing over registered applications.
- If aggregate: route via rendezvous over live node set; forward locally or via
  `CommandForwardingService` gRPC.
- If integration: routing depends on `ConcurrencyPreference` (see Phase 4).

**Graceful shutdown** (on ctx cancellation):

- Drain in-flight commands.
- Flush subsystem checkpoints.
- Remove this node's heartbeat entry.

---

## Build Order

```
Phase 1 (engine skeleton) — establishes public API; compiles immediately

Phase 2 (node registry)
  ├─→ Phase 3 (aggregate)
  │     └─→ Phase 5 (event fan-out)
  │           ├─→ Phase 6 (process + timeouts)
  │           └─→ Phase 7 (projection)
  └─→ Phase 4 (integration)
        └─→ Phase 5 (event fan-out) [shared dependency]

Phase 8 (poison queue) — implement when Phase 3/4 patterns are established
Phase 9 (inter-node gRPC) — can be developed in parallel with Phases 2–7
Phase 10 (engine wiring) — last; wires all subsystems into the Phase 1 skeleton
```

---

## Open Questions

### 1. Adding new persistent nodes to a running cluster

Since event stream UUIDs equal persistent node UUIDs, adding a new persistent node to the
cluster introduces a new partition and a new stream simultaneously. Existing instance–stream
bindings are unaffected — they are permanent (recorded in the instance event journal) and never
re-derived.

Adding a node changes the rendezvous hash output for future bindings and for partition ownership
of unbound instances. Because node membership is derived from the heartbeat keyspace rather than
from static configuration, no explicit cluster-wide coordination is required: the new node starts
up, writes its UUID to the heartbeat, and existing nodes absorb it on the next membership refresh
cycle. The only gap is the membership refresh lag, which is inherent to the TTL-based design.

What remains unspecified is whether any additional operator guidance (runbook, rollout checklist)
is needed for production deployments. Resolve before Phase 3 is considered complete.

### 2. Stable node identity mechanism

Persistent nodes require a stable UUID supplied as an engine option. The option will have a
helper to read the value from an environment variable. If the option is not set the node
operates in ephemeral mode (random UUID per run). The exact option name and env var key must
be defined before Phase 10 is started.

### 3. Handler metadata retention for removed handlers

When a handler is removed from an application's config, its historical instance data (journals,
snapshots, instance–stream bindings) remains in storage. The engine must be able to reason about
these entries during recovery:

- Record handler key and handler type inside journal entries, not just as storage keys.
- Retain metadata for removed handlers indefinitely until the handler is both absent from the
  current config AND its journals are fully idle (no in-flight commands, no unconsumed events).
- The exact GC policy is deferred but must not be made impossible by early implementation
  decisions.

### 4. Idempotency key and ADR-29

ADR-29 (`dogma`) proposes removing `WithIdempotencyKey()` on the grounds that implementing it
requires routing all keyed commands through a single shared pipeline, limiting scalability.

In runkit's design this concern may not apply: the per-command execution journal is already
keyed by command UUID, and supporting an idempotency key could be as simple as using the key as
the journal key instead. If so, the original objection dissolves and `WithIdempotencyKey()` could
be retained (or restored). Assess this before taking a position on ADR-29 and before Phase 4 is
started.

### 5. MinimizeConcurrency strictness during membership transitions

`MinimizeConcurrency` via a single handler journal provides structural serialisation when routing
is stable. During membership transitions, two nodes may transiently both believe they own the
handler's partition. OCC on the journal position prevents corrupted state but does not prevent
momentary concurrent execution of two different commands. Whether this transient concurrency
violates the intent of `MinimizeConcurrency` is deferred until the strictness requirement is
clarified. A distributed handler-level mutex (CAS kv entry) could provide strict at-all-times
serialisation at the cost of an extra round-trip per command.

### 6. Aggregate/process partition set independence

The current design uses the same partition UUID set (the live persistent nodes) for both
aggregate and process subsystems. This enables guaranteed colocation but couples their scaling.
If the workload profile requires independent scaling, independent partition sets are feasible —
the colocation guarantee becomes probabilistic rather than structural. Revisit if scaling
requirements demand it.

### 7. Snapshot GC policy

The snapshot GC policy should retain snapshots inversely proportional to replay cost. Concretely:
prefer to retain the most recent snapshot for instances with high event counts (expensive replay)
and allow earlier GC for instances with low event counts. The exact formula — configurable event
threshold for capture, and minimum event distance for GC — is deferred to Phase 3 implementation.

### 8. Eventstream package: "partition" vs "stream" naming

The `internal/subsystem/eventstream` package uses the word "partition" internally for each stream
segment (one partition = one dogma `StreamID`). This document uses "stream" for that concept,
matching dogma's `StreamID()` terminology. The terms are equivalent; the inconsistency is a
technical debt item.

Options:

- Rename `eventstream` internals from "partition" to "stream" (aligns code with dogma and with
  this document; relatively contained change).
- Adopt "partition" everywhere in the plan too (consistent with the `internal/partition` package
  naming, but conflicts with dogma's `StreamID` language).

The first option is preferred. Resolve before Phase 5 work begins.

---

## Existing Code: Status and Role

Previous implementation attempts have been archived to `_internal/` (excluded from the build
by Go's `_`-prefix convention). Only the stable helper packages under `internal/x/` remain active.

**Active packages:**

| Package                   | Status         | Role                                              |
| ------------------------- | -------------- | ------------------------------------------------- |
| `internal/x/xtelemetry`   | Exists; stable | Telemetry constants.                              |
| `internal/x/xpersistence` | Exists; stable | Protobuf marshaling helpers.                      |
| `internal/x/xtesting`     | Exists; stable | Failable stores for property-based chaos testing. |

**Archived in `_internal/`** (reference only — do not import):

| Package                           | Notes                                                                                                                                                               |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_internal/partition`             | Rendezvous hash core. Accepts `*uuidpb.UUID` for both partitions and workloads. Each subsystem derives a UUID from its own inputs before calling `SelectPartition`. |
| `_internal/subsystem/eventstream` | Journal-backed event stream. Implementation style is a reference, not a template.                                                                                   |
| `_internal/subsystem/poisonqueue` | Global kv-backed failed-command store. Review for consistency when implementing Phase 8.                                                                            |

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
