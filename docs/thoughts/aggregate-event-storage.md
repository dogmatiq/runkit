# Aggregate Event Storage: Single-Copy Events

This document captures the design direction for aggregate event storage.
A future ADR will formalize these decisions.

> **Terminology note**: early drafts of this document used the term "factspace"
> for the per-instance private store. That term has been dropped. The settled
> name for the store will be decided in the ADR.

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
instance has a per-instance journal that stores operational metadata but
never stores event data. The stream is the sole source of truth for events.

## Per-Instance Journal

Each aggregate instance has a private journal keyed by
`(app, handler_key, instance_id)`. It is private to the aggregate subsystem and
its record format can evolve freely without affecting other subsystems. This is
the aggregate handler's data store introduced in ADR-6; the ADR will settle its
name.

The per-instance journal stores:

- **OCC state**: append at expected position remains the conflict detection
  primitive.
- **In-flight command tracking**: which command is being handled.
- **Stream binding**: the partition this instance is bound to (permanent, set
  on first event-producing command). The binding is written to the per-instance
  journal before the first stream append; a crash after writing the binding but
  before the stream append leaves the instance in a valid, empty state on
  recovery. The reverse -- stream events without a binding -- cannot occur.
- **First-event offset**: the stream partition offset of the instance's first
  event, set at binding time (permanent). Useful metadata even if not directly
  used in the current implementation -- essentially cost-free to record.
- **Last-event offset**: the stream partition offset of the instance's most
  recent event, updated on every event-producing write. Provides the upper
  bound for scan termination -- reconstruction scans from `first_event_offset`
  (or the snapshot offset) to `last_event_offset`, not to the stream tail. This
  bounds the scan to the instance's active lifetime window only, even on a
  stream that has grown enormously since the instance went quiet.
- **Event offset deltas**: per-transaction stream offsets for this instance's
  events, appended incrementally to the per-instance store with each write
  (typically one uint64 per transaction). Accumulated post-snapshot deltas are
  merged into the Roaring Bitmap at snapshot time and the pre-snapshot records
  are compacted away.
- **Snapshot**: the serialized `AggregateRoot` state, written inline, together
  with a Roaring Bitmap (serialised using the [Roaring Format Spec][roaring])
  covering all stream offsets belonging to this instance up to the snapshot
  point. Survives compaction. If no snapshot has been taken yet, the bitmap
  does not exist as a serialised artifact; reconstruction assembles it from the
  incremental offset delta records in the per-instance store instead.
  The aggregate state may be absent if `MarshalBinary()` returned
  `ErrNotSupported` (e.g. via `NoSnapshotBehavior`) or failed transiently;
  snapshot availability is per-attempt, not a static property of the aggregate
  type.
- **Stream offset hint**: the stream partition offset at the time of the last
  snapshot or clean unload. Always written at finalization/clean-unload
  regardless of whether a snapshot was also produced.

The per-instance journal is compactable. After compaction it contains the offset hint,
binding, first-event offset, and snapshot (if one was produced).

## Stream Partition Journal

The stream partition journal keyed by `(app, stream_partition)` is the sole
location for event data. It is permanent and append-only (immutable). Stream
offsets are stable forever.

The stream is consumed by processes and projections. Integrations produce
events into the stream but do not consume from it.

## State Reconstruction

On reload, the engine reads the per-instance journal and reacts to what it
finds. Snapshot availability is per-attempt -- `AggregateRoot.MarshalBinary()`
can return `ErrNotSupported` or valid data on any given call -- so the journal
records the outcome of each attempt rather than classifying the instance.

### With a snapshot present

Three cases, ordered by likelihood:

#### Clean reload (common)

When an instance is unloaded cleanly -- graceful shutdown, membership
transition, idle eviction -- the engine attempts a snapshot. If
`MarshalBinary()` succeeds, the snapshot is written to the per-instance journal
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
stream partition from the first-event offset to `last_event_offset`, reading
and skipping non-matching events, then replays all matching events through
`ApplyEvent()`. The scan terminates at `last_event_offset`, not at the current
stream tail -- for an instance that went quiet long ago, the scan covers only
that instance's active lifetime window, not everything that has accumulated in
the stream since.

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
store) was considered but is unnecessary. The per-instance journal is upstream
of the stream -- by the time events reach the stream, the journal has already
gone through the OCC append. Offset hints stored in the journal accomplish the
same goal without additional writes or infrastructure.

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

## Per-Instance Offset Index

Rather than treating the offset index as future extensibility, it is part of
the settled design. The per-instance store maintains a Roaring Bitmap of all
stream offsets belonging to the instance. This section explains the rationale,
the write path, the reconstruction path, and the dismissed alternatives.

### Scan window bounds

Each instance has a `[first_event_offset, last_event_offset]` range. A stream
scan for reconstruction terminates at `last_event_offset`, not the current
stream tail. For an instance that was active for five minutes two years ago,
the scan covers five minutes worth of stream data -- not two years. This makes
scans for historical inactive instances much less expensive than the naive
framing ("scan the whole stream from first_event_offset") suggests.

The skip rate within that window still matters. On a busy stream with many
concurrent writers, the instance may own only a small fraction of the records
in its lifetime window. The index eliminates that skip cost by enabling
targeted random fetches at known offsets.

The cost comparison is sequential scan vs random key lookups at the database
level. The underlying storage primitives are database-backed (not raw file
I/O), so sequential-vs-random magnitudes vary by backend and workload. The
qualitative point holds without specific numbers: the index wins when the
skip rate is high, and scan wins when the instance dominated its lifetime
window and the window is short.

### Why maintain the index unconditionally

It is tempting to introduce the index only for instances that cross some
size or history threshold. This does not work:

- The instances that will benefit most from an index cannot be identified at
  birth. A recently-created instance with 10 events looks identical to one
  that will eventually accumulate 10 million events.
- Once an instance is large enough to need an index, retrofitting it requires
  a full stream scan -- exactly the cost the index avoids. You cannot build
  the index cheaply after the fact.
- The per-write cost of maintaining the index is negligible: appending the
  new offsets (typically one uint64) to the per-instance store on each
  event-producing write.

Accept the overhead universally. For small instances it is negligible. For
large instances it is necessary.

### Snapshots and the index are complementary

Snapshots and the offset index address different axes of reconstruction cost:

- The **snapshot** eliminates event replay cost. Starting from a snapshot at
  offset K means you only need events after K; you do not re-apply historical
  events through `ApplyEvent`.
- The **index** eliminates stream scan cost. Given the offsets from the index,
  reconstruction fetches exactly the records it needs without forward-scanning
  past irrelevant events.

Used together: load the snapshot at offset K, advance the index iterator to
K+1 (O(log containers) for a Roaring Bitmap), then fetch exactly the
post-snapshot events by random lookup. Neither mechanism replaces the other.

The index supports snapshot-absent reconstruction natively. Deserialise the
bitmap, iterate from `first_event_offset`, fetch only matching records. The
`last_event_offset` bound ensures termination. No snapshot dependency.

### Snapshot invalidation

A snapshot encodes the decoded in-memory state of an `AggregateRoot` -- the
result of calling `ApplyEvent` many times. If the aggregate type changes its
internal state representation in a new deployment, old snapshots may be
undecodable or semantically stale. `UnmarshalBinary` can fail on a valid but
out-of-date snapshot.

The offset bitmap has no such vulnerability. It encodes stream offsets --
purely positional data with no semantic content. A bitmap written under one
version of the aggregate type is valid under every future version.

For cold-start-after-upgrade -- where the most recent snapshot is invalid
and there is no valid fallback -- the bitmap is the only path to bounded
reconstruction. The engine discards the snapshot, starts from the known state
(empty or last valid snapshot, wherever that may be), and uses the bitmap to
fetch exactly the events to replay. Without the bitmap, this degrades to a
full window scan.

The practical implication: treat the bitmap as the durable, version-agnostic
artifact and treat the snapshot as optional acceleration on top of it.

### Settled design

**Write path.** On each event-producing handler invocation, the new stream
offsets (typically one) are appended as a raw delta to the per-instance store
alongside the existing operational metadata. No bitmap materialisation occurs
on the write path.

**Snapshot path.** At snapshot time (clean unload, graceful shutdown, or
periodic snapshot), the engine merges all post-previous-snapshot deltas into
the cumulative Roaring Bitmap, serialises it using the [Roaring Format
Spec][roaring], and writes it inline with the snapshot record. Pre-snapshot
records are then compacted away. The bitmap materialisation cost at snapshot
time is O(events since last snapshot) -- bounded by snapshot frequency, and
done on the unload path where latency is acceptable.

**Reconstruction path.** Load the snapshot record. If a Roaring Bitmap is
present, advance an iterator to max(snapshot_offset, first_event_offset) + 1.
Read any post-snapshot delta records from the per-instance store and add their
offsets to the in-memory iterator. Fetch stream records at each offset by
random lookup, replay through `ApplyEvent`. The scan terminates when the
iterator is exhausted; `last_event_offset` is the authoritative upper bound.

If no snapshot is present, read the incremental offset delta records from the
per-instance store to assemble the offset set, then fetch stream records at
those offsets by random lookup. If the per-instance store has no records at
all (the instance has never produced events), there is nothing to reconstruct.

**Coverage bound.** A Roaring Bitmap is a plain set with no intrinsic
watermark. The maximum set bit equals `last_event_offset` for a correctly
maintained bitmap, but `last_event_offset` is stored separately and is the
authoritative coverage bound. If the bitmap and `last_event_offset` diverge
(e.g. a corrupt or partial bitmap from a crash during snapshot), `last_event_offset`
is the ground truth and the fallback scan uses it.

**Serialisation format.** The [Roaring Format Spec][roaring] is stable,
cross-language, and explicitly versioned for interoperability. The dependency
is on the wire format, not on any specific library's internals. A bitmap
written under one engine version can be deserialised by any other.

[roaring]: https://github.com/RoaringBitmap/RoaringFormatSpec/

**Stream offsets, not journal positions.** The bitmap encodes stream offsets
rather than the stream journal's internal positions. Journal positions are an
implementation detail of the stream subsystem; stream offsets are the stable
public contract (exposed by the Dogma API and used by projections for
checkpointing). Encoding journal positions would couple the per-instance store
format to the stream's internal layout, violating the subsystem boundary in
the wrong direction. The compactness argument (one journal position per
batch vs one offset per event) provides negligible benefit in practice since
batches are almost always one event, and does not justify the coupling.

### Dismissed index alternatives

Several other approaches were considered before settling on the Roaring Bitmap.

**Backward pointer chain.** Each event record in the per-instance store
carries the stream offset of the previous event from the same instance.
Reconstruction follows the chain backward from the last known offset to
`first_event_offset`, then replays forward -- the same structure as a
singly-linked list threaded through a log, analogous to Git's parent pointers.
No separate data structure; one additional field per record. Dismissed because
reconstruction cost is O(n) seeks where n is the instance's event count, and
long chains degrade proportionally. Provides no compression benefit over a
plain list.

**Skip pointers.** Extend the backward pointer with additional pointers
that skip back 2, 4, 8, ... events in the instance's own sequence. Each record
carries O(log n) pointers where n is its position in that sequence.
Reconstruction becomes O(log n) seeks rather than O(n). Used in some LSM-based
event stores. Dismissed because the write path bloat and implementation
complexity exceed that of the bitmap, with no compactness advantage: a Roaring
Bitmap over the same offsets is smaller and supports the same O(log n) skip
via container-level binary search.

**Run-length encoded offset ranges.** Store event offsets as (start, length)
pairs rather than individual values -- e.g. `[(10, 3), (50, 4), (100, 1)]`
for offsets {10, 11, 12, 50, 51, 52, 53, 100}. Dismissed because Roaring
Bitmap uses run containers internally for exactly this case (dense consecutive
ranges), so the per-record encoding is strictly dominated by the bitmap
representation. The bitmap also handles sparse offsets efficiently via array
containers, without needing to choose a representation at write time.

## Trade-offs

### Advantages

- Events stored once, not twice. Halves event write volume and storage.
- One journal per instance (not two). No separate snapshot KV store.
- Compaction is straightforward -- no downstream consumers depend on the
  per-instance journal.
- Simpler mental model: the stream is the sole source of truth for events.

### Costs

- State reconstruction after a crash requires both a per-instance journal read
  and a stream scan (or random lookups). Mitigated by the offset index,
  aggressive snapshotting, and warm routing making cold starts rare.
- Crash recovery without a valid snapshot traverses potentially irrelevant
  events in the stream partition up to `last_event_offset`. Per-event cost is
  low (read and skip), but the window may be large on a high-throughput
  stream if the instance had a long active lifetime and no clean unload.

## Revised State Inventory (Aggregate Command Path)

| Store                        | Key                               | Type    | Lifetime              |
| ---------------------------- | --------------------------------- | ------- | --------------------- |
| Per-node acceptance keyspace | `(node, app, command_uuid)`       | KV      | Until completion      |
| Poison backlog               | `(app, partition, command_uuid)`  | Set     | Until restart trickle |
| Idempotency journal          | `(app, idempotency_key)`          | Journal | Until completion      |
| Per-instance journal         | `(app, handler_key, instance_id)` | Journal | Permanent (compacted) |
| Stream                       | `(app, stream_partition)`         | Journal | Permanent             |

Compared to the naive two-journal approach, there is no separate per-instance
event journal and no separate snapshot KV store. The snapshot lives inside the
per-instance journal.

---

## Notes for ADR

This section contains guidance for drafting the ADR that formalizes these
decisions. Remove this section from the ADR itself.

- This ADR has not yet been drafted. Do not pre-allocate a number; determine
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
  - _Stream-side per-instance index_: unnecessary because the aggregate
    instance journal is upstream of the stream. Would be derived data,
    rebuildable, and async-maintained -- but adds infrastructure for a problem
    already solved by offset hints in the journal.
  - _Requiring snapshot support_: would reject valid Dogma aggregates using
    `NoSnapshotBehavior`. No stricter contract than Dogma mandates. Also
    ignores that snapshot availability is per-attempt -- even aggregates that
    usually support snapshots can fail to marshal in specific states.
- **Per-instance offset index (settled design, not dismissed)**:
  - _Roaring Bitmap_: offsets accumulated incrementally as raw deltas per
    write; merged into a serialised Roaring Bitmap ([Roaring Format Spec][roaring])
    at snapshot time; compacted with the snapshot record. Reconstruction loads
    the snapshot bitmap, advances the iterator past the snapshot offset, applies
    post-snapshot deltas, then random-fetches stream records at each offset.
    Backward-compatible: instances without a bitmap fall back to scan.
  - _Dismissed alternatives_: backward pointer chain (O(n) seeks),
    skip pointers (O(log n) seeks but write bloat exceeds bitmap),
    plain sorted offset list (not compressible; 8 bytes/entry vs bitmap's
    2 bytes/entry, no run compression for dense ranges),
    run-length encoded ranges (subsumed by bitmap's internal run containers).
- **Consequences section**: capture the cold-start cost profile. The scan
  window is `[first_event_offset, last_event_offset]` -- bounded to the
  instance's active lifetime, not the full stream. Low-traffic instances
  cold-start frequently but have shorter event histories, bounding the scan
  naturally. The problematic case (formerly high-traffic, now quiet, invalid
  or absent snapshot) is handled by the offset index. Snapshot invalidation
  on handler upgrades is the key reason the bitmap is durable and the snapshot
  is optional.
- **Snapshot availability model**: note that snapshot availability is
  per-attempt (`MarshalBinary()` can return `ErrNotSupported` or fail on any
  call). `NoSnapshotBehavior` is the most common reason but not the only one.
  The engine reacts to the outcome of each attempt rather than classifying
  the aggregate type.
- **Relationships**: References ADR-3 (OCC), ADR-5 (no cross-store atomics,
  relevant to binding write ordering), ADR-6 (fills in the internal structure
  of the data store introduced there). Add `Referenced by` back-annotations
  to each.
- **Glossary**: Introduce a term for the per-instance journal (keyed by
  `(app, handler_key, instance_id)`, storing OCC state, stream binding, offset
  hint, and inline snapshot). The name is decided in the ADR itself.
- **Update 000-big-picture.md**: Remove the separate "Snapshot" KV row from
  the Aggregate command path state inventory table; the snapshot is stored
  inline in the aggregate instance journal. Review Phase 3 execution steps
  to ensure they reflect the finalization flow (offset hint + snapshot written
  together at clean unload).

## Open Questions

### Integration handler data store

Integrations produce events into the stream but do not have instances or
event-sourced state. They may still need a per-handler private store of some
sort -- for example to track idempotency keys or handler-level OCC state. This
is out of scope for this ADR but should be revisited when the integration
subsystem is designed.

### Aggregates without snapshot support

Resolved. Snapshot availability is per-attempt, not per-aggregate-type. The
engine always attempts `MarshalBinary()` and handles `ErrNotSupported`
gracefully. See "Without a snapshot" under State Reconstruction, and the
Extensibility section.
