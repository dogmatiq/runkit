# Aggregate Event Storage: Single-Copy Events

This document captures the design direction for aggregate event storage. A
future ADR will formalize these decisions.

> **Naming**: a "factspace" is a private, authoritative, compactable store for
> a specific entity. It is not tied to a particular storage primitive -- a
> factspace may be backed by a journal, KV entries, or a combination. The name
> conveys durability and authority -- in contrast with a "scratchspace," which
> would imply disposability. The pattern may apply to other handler types. The
> aggregate's factspace is the "aggregate factspace."

## Design Tension

A naive event-sourced aggregate stores events in two places:

1. A **per-instance event journal** -- used for state reconstruction via event
   replay.
2. The **stream partition journal** -- the canonical event log consumed by
   processes and projections.

This would double write volume and storage footprint. The per-instance journal
exists primarily to support state reconstruction, but the same events already
exist in the stream.

## Decision: Single-Copy Events in the Stream

Events are stored exactly once, in the stream partition journal. Each aggregate
instance has a per-instance journal that stores operational metadata but never
stores event data. The stream is the sole source of truth for events.

## Aggregate Factspace

Each aggregate instance has a factspace keyed by
`(app, handler_key, instance_id)`. It is private to the aggregate subsystem and
its record format can evolve freely without affecting other subsystems. In this
design the aggregate factspace is backed by a journal, but the factspace
concept is not tied to a particular storage primitive.

The aggregate factspace stores:

- **OCC state**: append at expected position remains the conflict detection
  primitive.
- **In-flight command tracking**: which command is being handled.
- **Stream binding**: the partition this instance is bound to (permanent, set
  on first event-producing command).
- **First-event offset**: the stream partition offset of the instance's first
  event, set at binding time (permanent). Useful metadata even if not directly
  used in the current implementation -- essentially cost-free to record.
- **Snapshot**: the serialized `AggregateRoot` state, written inline. Survives
  compaction. May be absent on any given attempt -- `MarshalBinary()` can
  return `ErrNotSupported` (e.g. via `NoSnapshotBehavior`) or fail
  transiently. Snapshot availability is per-attempt, not a static property of
  the aggregate type.
- **Stream offset hint**: the stream partition offset at the time of the last
  snapshot or clean unload. Always written at finalization/clean-unload
  regardless of whether a snapshot was also produced.

The factspace is compactable. After compaction it contains the offset hint,
binding, first-event offset, and snapshot (if one was produced).

## Stream Partition Journal

The stream partition journal keyed by `(app, stream_partition)` is the sole
location for event data. It is permanent and append-only (immutable). Stream
offsets are stable forever.

The stream is consumed by processes and projections. Integrations produce
events into the stream but do not consume from it.

## State Reconstruction

On reload, the engine reads the factspace and reacts to what it finds. Snapshot
availability is per-attempt -- `AggregateRoot.MarshalBinary()` can return
`ErrNotSupported` or valid data on any given call -- so the factspace records
the outcome of each attempt rather than classifying the instance.

### With a snapshot present

Three cases, ordered by likelihood:

#### Clean reload (common)

When an instance is unloaded cleanly -- graceful shutdown, membership
transition, idle eviction -- the engine attempts a snapshot. If
`MarshalBinary()` succeeds, the snapshot is written to the aggregate factspace
with the current stream offset. An offset hint is always written regardless.
If the snapshot is marked as reflecting the newest events for this instance, no
stream scan is necessary on reload.

#### Post-crash recovery (rare)

If the node crashes while an instance is active, events may have been committed
to the stream but no snapshot written. The scan starts from the last known
offset hint. However, the stream may contain any number of events from other
instances/integrations since that offset, so the scan is not bounded or
zero-cost -- it reads and skips non-matching events.

#### Stale snapshot (very rare)

If the snapshot write itself failed, the scan window extends to all partition
events since the last successful snapshot. This is somewhat bounded by snapshot
frequency, which is operator-tunable.

### Without a snapshot

A snapshot may be absent for several reasons: the aggregate root embeds
`NoSnapshotBehavior`, `MarshalBinary()` returned `ErrNotSupported` for the
current state, a transient error occurred, or no clean unload has happened yet.

The offset hint is always written at finalization/clean-unload regardless of
whether a snapshot was also produced. On cold start, reconstruction scans the
stream partition from the first-event offset to the last known offset hint,
reading and skipping non-matching events, then replays all matching events
through `ApplyEvent()`.

Cold starts are only rare for high-traffic instances that stay warm via
rendezvous routing. Low-traffic instances cold-start frequently (evicted for
idleness), making the scan the default reload cost -- but these instances also
tend to have shorter event histories, bounding the scan naturally. The
problematic case is a formerly high-traffic instance that has gone quiet: long
event history, scanned on every load. This scenario is what would most likely
motivate introducing a per-instance offset index in the future (see
Extensibility below).

## No Stream-Side Per-Instance Index

A per-instance index on the stream (e.g. last-offset-per-instance in a KV
store) was considered but is unnecessary. The aggregate factspace is upstream
of the stream -- by the time events reach the stream, the factspace has already
gone through the OCC append. Offset hints stored in the factspace accomplish
the same goal without additional writes or infrastructure.

If a stream-side index were added in the future, it would be derived data --
rebuildable from the stream at any time -- and could be maintained
asynchronously.

## Aggressive Snapshotting

When `MarshalBinary()` succeeds, the design relies on aggressive snapshotting
to minimize scan windows:

- **On clean unload**: always attempt a snapshot. When `MarshalBinary()`
  succeeds, this ensures the common reload path requires zero stream scanning.
- **Periodically during operation**: snapshot frequency is tunable. More
  frequent snapshots reduce the scan window after a crash at the cost of more
  writes.

Combined with rendezvous hashing's warm-routing properties (instances stay
pinned to the same node), cold starts are rare by design. The scan-heavy
rebuild scenario essentially disappears under normal operation.

Snapshot frequency could potentially be auto-tuned based on event stream
volume, snapshot size, etc.

## Extensibility: Per-Instance Offset Index

The scan-based reconstruction when no snapshot is available is accepted as the
initial approach. If scan cost becomes a problem in practice -- particularly
for the formerly-high-traffic-now-quiet scenario described above -- a
per-instance offset index can be introduced without changing the factspace.

The offset index would store event offset ranges for an instance, enabling
targeted reads at known stream positions rather than scanning and skipping.
For example, an instance producing events in bursts:

    Events at offsets: [10, 11, 12, 50, 51, 52, 53, 100]
    Stored as ranges:  [(10, 3), (50, 4), (100, 1)]

Two storage approaches were considered:

- **Journal-based offset index**: a per-instance append-only journal storing
  offset ranges. Compactable via the same mechanics as factspace compaction:
  read all records, build merged range list, write summary record, truncate
  prefix. The more natural storage primitive for ordered, unbounded data.

- **KV-based offset index**: a single KV entry per instance, updated in place
  on every write until it hits a size cap -- providing incremental compaction
  for free, a property journals don't have. When the entry exceeds the cap,
  spill to a new key. The spill mechanic resembles a journal (ordered,
  append-only sequence of keys), but the head is always in compact form.
  Trade-off: high caps risk unbounded entry sizes; low caps increase key count
  and reconstruction read volume.

Either approach eliminates scanning by enabling targeted reads at known
offsets. Instances without an index fall back to scan behavior (backward
compatible). The index is additive -- the factspace doesn't need to change.

## Trade-offs

### Advantages

- Events stored once, not twice. Halves event write volume and storage.
- One journal per instance (not two). No separate snapshot KV store.
- Compaction is straightforward -- no downstream consumers depend on the
  per-instance journal.
- Simpler mental model: the stream is the sole source of truth for events.

### Costs

- State reconstruction after a crash requires both a factspace read and a
  stream scan. Mitigated by aggressive snapshotting and warm routing making
  cold starts rare.
- Crash recovery scan traverses potentially irrelevant events in the stream
  partition. Per-event cost is low (read and skip), but I/O can be significant
  on a high-throughput partition.

## Revised State Inventory (Aggregate Command Path)

| Store                        | Key                               | Type    | Lifetime              |
| ---------------------------- | --------------------------------- | ------- | --------------------- |
| Per-node acceptance keyspace | `(node, app, command_uuid)`       | KV      | Until completion      |
| Poison backlog               | `(app, partition, command_uuid)`  | Set     | Until restart trickle |
| Idempotency journal          | `(app, idempotency_key)`          | Journal | Until completion      |
| Aggregate factspace          | `(app, handler_key, instance_id)` | Journal | Permanent (compacted) |
| Stream                       | `(app, stream_partition)`         | Journal | Permanent             |

Compared to the naive two-journal approach, there is no separate per-instance
event journal and no separate snapshot KV store. The snapshot lives inside the
aggregate factspace.

---

## Notes for ADR

This section contains guidance for drafting the ADR that formalizes these
decisions. Remove this section from the ADR itself.

- Use the `write-adr` skill. Do not eagerly assign an ADR number; determine
  the next available number at draft time.
- **Framing**: This is a greenfield design choice, not a migration. Use "We
  will..." not "We will replace..." There is no existing implementation.
- **Context section**: Frame the duplication as a design tension discovered
  during planning. "Events would be stored in two places if..." not "Events
  are stored in two places."
- **Dismissed alternatives**:
  - _Per-instance event journal (naive approach)_: stores full event data per
    instance, duplicating the stream. Works but doubles writes and storage.
  - _Offset-only event history journal_: a second per-instance journal storing
    only stream offsets instead of event data. Smaller data but still a second
    journal, adds read roundtrips, increases snapshot dependency.
  - _Stream-side per-instance index_: unnecessary because the factspace is
    upstream of the stream. Would be derived data, rebuildable, and
    async-maintained -- but adds infrastructure for a problem already solved by
    offset hints in the factspace.
  - _Requiring snapshot support_: would reject valid Dogma aggregates using
    `NoSnapshotBehavior`. No stricter contract than Dogma mandates. Also
    ignores that snapshot availability is per-attempt -- even aggregates that
    usually support snapshots can fail to marshal in specific states.
- **Considered alternatives (not implemented, future extensibility)**:
  - _Journal-based per-instance offset index_: stores event offset ranges,
    enabling targeted reads. The more natural storage primitive for ordered,
    unbounded data. Compactable via merge + truncate.
  - _KV-based per-instance offset index_: single entry updated in place until
    cap (incremental compaction for free), spill to new key on overflow.
    Head stays compact. Trade-off: high caps risk unbounded entry size; low
    caps increase key count.
- **Consequences section**: capture the cold-start cost profile when no
  snapshot is available. Low-traffic instances cold-start frequently (evicted
  for idleness) but tend to have shorter event histories, bounding the scan
  naturally. The problematic case is a formerly high-traffic instance that has
  gone quiet -- long event history, scanned on every load. This is the
  scenario most likely to motivate the offset index extensibility path.
- **Snapshot availability model**: note that snapshot availability is
  per-attempt (`MarshalBinary()` can return `ErrNotSupported` or fail on any
  call). `NoSnapshotBehavior` is the most common reason but not the only one.
  The engine reacts to the outcome of each attempt rather than classifying
  the aggregate type.
- **Relationship**: References ADR-3 (OCC remains the correctness primitive).
  Add counterpart annotation to ADR-3.
- **Glossary**: Introduce two terms: "factspace" (the general pattern of a
  private, authoritative, compactable store -- not tied to a specific storage
  primitive) and "aggregate factspace" (the specific factspace for an aggregate
  instance).
- **Update 000-big-picture.md**: State Inventory table (replace "Aggregate
  instance journal" + "Snapshot KV" rows with one "Aggregate factspace" row)
  and Phase 3 steps 5, 7, 9 to reflect the new flow.

## Open Questions

### Integrations and factspaces

Integrations produce events into the stream but do not have instances or
event-sourced state. They may still need a factspace of some sort -- for
example to track idempotency keys or handler-level OCC state. This is out of
scope for this document but should be revisited when the integration subsystem
is designed.

### Aggregates without snapshot support

Resolved. Snapshot availability is per-attempt, not per-aggregate-type. The
engine always attempts `MarshalBinary()` and handles `ErrNotSupported`
gracefully. See "Without a snapshot" under State Reconstruction, and the
Extensibility section.
