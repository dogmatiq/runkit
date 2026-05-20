# Multiple Event Streams Per Application

## Overview

Split the single global event stream into multiple streams to eliminate write
serialization as a throughput bottleneck. Streams are identified by UUID, each
with an independent offset counter. Aggregate instances are permanently assigned
to a stream; integration commands pick one per-invocation. The engine
auto-creates new streams when all existing streams exceed 1000 events/sec
(lifetime average). Consumers track per-stream offsets (Kafka-style), but
consumer-side changes are out of scope for this plan.

## Phase 1 -- Schema changes

1. Create `event_streams` table -- holds stream identity, per-stream offset
   counter, and creation timestamp.
   - `stream_id UUID PRIMARY KEY`
   - `next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0)`
   - `created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()`

2. Rename `event_stream` table to `events` -- add `stream_id` column, change
   PK.
   - Add `stream_id UUID NOT NULL REFERENCES event_streams(stream_id)`
   - PK becomes `(stream_id, event_offset)` (stream-first for per-stream
     sequential reads)
   - Update all index names: `idx_events_by_type`,
     `idx_events_by_aggregate_instance`, `idx_events_by_correlation_id`
   - `idx_events_by_aggregate_instance` adds `stream_id` prefix

3. Drop `event_stream_offset` table -- replaced by `event_streams.next_offset`.

4. Add `stream_id` to `aggregate_instances` table -- stores the instance's
   permanent stream assignment.
   - `stream_id UUID NOT NULL REFERENCES event_streams(stream_id)`

## Phase 2 -- Stream selection logic

5. Implement `eventstream.Select()` function -- selects the least-loaded stream
   or creates a new one. Runs within the caller's transaction.
   - Queries all streams, computes lifetime average rate:
     `next_offset / EXTRACT(EPOCH FROM (now() - created_at))`
   - Returns the stream with the lowest rate
   - If ALL streams have rate >= 1000 events/sec, creates a new stream
     (generates UUID, inserts row) and returns it
   - On first boot (no streams exist), creates the initial stream

6. Aggregate workers call `Select()` at instance creation -- store result in
   `aggregate_instances.stream_id`. Subsequent commands reuse the stored
   stream_id. (depends on 5)

7. Integration workers call `Select()` at append time -- pick a stream
   per-command. No sticky binding. (depends on 5)

## Phase 3 -- Append/read path updates

8. Update `eventstream.Append()` -- accept a `stream_id` parameter. UPDATE the
   specific stream's `next_offset` row (not a singleton). INSERT events with
   `stream_id`. (depends on 1, 2)

9. Update `eventstream.Read()` -- accept a `stream_id` parameter. Read events
   from a specific stream sequentially by offset. (depends on 2)

10. Update `eventstream.ReadByAggregateInstance()` -- filter by `stream_id` in
    addition to handler_key and instance_id. The instance's stream_id is known
    from `aggregate_instances`. (depends on 2)

11. `eventstream.ReadByCorrelationID()` unchanged -- queries across all streams
    via the correlation_id index (no stream_id filter needed). (no dependency on
    stream changes)

## Phase 4 -- Worker integration

12. Update aggregate worker -- pass instance's `stream_id` to `Append()`. On
    instance creation, call `Select()` and persist `stream_id`. (depends on 6,
    8)

13. Update integration worker -- call `Select()` to pick a stream, pass to
    `Append()`. (depends on 7, 8)

## Relevant files

- `internal/database/schema.sql` -- all schema changes (new table, renamed
  table, new columns, updated indexes)
- `internal/eventstream/eventstream.go` -- `Append()`, `Read()`,
  `ReadByAggregateInstance()` gain stream_id parameter; new `Select()` function
- `internal/eventstream/offset.go` -- may need minor updates for per-stream
  offset handling
- `internal/aggregate/worker.go` -- calls `Select()` at instance creation;
  passes stream_id to `Append()`
- `internal/aggregate/controller.go` -- may need to thread stream_id through
- `internal/integration/worker.go` -- calls `Select()` per-command; passes
  stream_id to `Append()`
- `engine.go` -- `ReadByCorrelationID` unchanged; observer path unaffected

## Verification

1. `make precommit` -- all existing tests pass (after updating for new
   signatures)
2. Appending to different streams produces independent offset sequences
3. `Select()` returns the least-loaded stream
4. `Select()` creates a new stream when all streams exceed 1000 events/sec
   threshold
5. `Select()` creates the first stream on an empty database
6. `ReadByAggregateInstance()` correctly filters by stream
7. `ReadByCorrelationID()` finds events across multiple streams
8. Aggregate instance creation stores stream_id and reuses it on subsequent
   commands
9. Integration worker picks a stream per-command (non-sticky)

## Decisions

- Offset allocation: Row lock per stream (dense, gapless offsets). Post-commit
  numbering is a known future optimization but out of scope.
- Per-stream throughput ceiling: ~500-2500 events/sec (row lock through durable
  commit). Acceptable; scale horizontally with more streams.
- Scaling heuristic: Lifetime average rate (`next_offset / age`). Fixed
  threshold of 1000 events/sec. Not configurable.
- Initial state: 1 stream created on first boot.
- Table naming: `event_streams` (metadata) + `events` (event rows). Drop
  `event_stream_offset`.
- No table partitioning -- single physical table for events. Partitioning is a
  future operational optimization.
- Consumer changes out of scope -- consumers will eventually track per-stream
  offsets, but that's a separate plan.
- Stream selection runs in caller's transaction -- exposed as
  `eventstream.Select()`.
- Capacity estimate: db.t4g.large saturates at ~3-6 streams sustained / ~10-20
  burst. Multi-stream gives 3-6x throughput improvement on the same hardware.
  Larger instances scale with vCPU count.
