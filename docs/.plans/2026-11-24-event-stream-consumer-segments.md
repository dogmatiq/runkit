# Plan: Serve ListEventStreams and ReadEventSegment (#155)

## Where the work stands

Planned, and the plan was approved in planning. The first task is done, as one
commit on the branch `bump-protobuf-v0.9.0`. The second task is written and
awaits review.

## Context

[#155](https://github.com/dogmatiq/runkit/issues/155) is part of
[#151](https://github.com/dogmatiq/runkit/issues/151), for RunKit v0.3.0. It
adds the two calls that dogmatiq/protobuf adds to `EventStreamConsumerAPI`:
`ListEventStreams`, which lists each stream with its length, and
`ReadEventSegment`, which returns one bounded range of a stream as one segment.
It also sets the optional `stream_length` field of `EventStreamSegment`.

What the code shows:

- The protobuf bump the issue asks for is done: [go.mod](/Users/james/grit/github.com/dogmatiq/runkit/go.mod)
  already pins dogmatiq/protobuf v0.10.0 (commit `d955d92` bumped to v0.9.0). No
  bump task is needed.
- [internal/grpcserver/eventstreamconsumer.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/grpcserver/eventstreamconsumer.go)
  serves `WatchEvents` and `ReadEvents` over a `source.Source`. Its doc comment
  says the other two calls are unimplemented, which must change. The engine
  ([engine.go](/Users/james/grit/github.com/dogmatiq/runkit/engine.go)) gives it only `*source.Local`, so the
  server never relays a remote engine's streams. The service is already
  registered on the one gRPC server, so the calls reach clients through the same
  path, with the same `grpc.ServerOption` values for authentication.
- `splitSegment` already cuts a segment to `MaxSegmentBytes`, so
  `ReadEventSegment` can reuse it and send only the first part. That part ends
  where the server chose, which the protocol allows ("the server may end the
  range before the requested end").
- `source.Local` loads a segment with one query
  ([local.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/eventstream/source/local.go),
  `loadSegment`) that also reads the stream's `next_offset`, but `Segment` has
  nowhere to keep it. That value is the stream length that both new pieces need.
  At most `SegmentSize` (100) events come from one query, which already bounds
  the work and memory of one `ReadEventSegment` call.
- `runkit.event_streams` has one row per stream with `id` and `next_offset`.
  [eventstreamlengths.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics/eventstreamlengths.go)
  reads the same table for the `EventStreamLength` metric.

## Decided in planning

- **Local only.** The new methods go on `source.Local` and not on the
  `source.Source` interface. `EventStreamConsumerService.EventStreams` is a
  `*source.Local`, since the engine gives it nothing else, so no interface or
  name for one is needed.
- **The listing method is `Streams`**, returning a map from each stream's ID to
  its length, because the order of the streams means nothing and a slice would
  promise one. It needs no new type.
- **An unknown length is `optional.Optional[uint64]`** (enginekit's `optional`
  package, already a dependency), because zero is a valid length.

## Tasks

Work one task at a time, with a review after each.

### 1. Carry the stream length in segments, and send it — [x] 4921ac2

- Add `StreamLength optional.Optional[uint64]` to `source.Segment`
  ([segment.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/eventstream/source/segment.go)): the
  stream's length as the source knew it when it loaded the segment, never less
  than `End`.
- `Local.loadSegment` sets it from the `next` it already scans. The opening
  segment that `openStream` delivers sets it to `end`. `Local` records the length
  of the last segment delivered for each stream beside its offset, in
  `s.progress`, so the opening segments that `register` builds carry it too.
  `Local` sets it on every segment it delivers. `Memory` sets it from the end of
  the stream. `Remote` sets it from the peer's `stream_length` when the peer
  sends one, and rejects a segment whose length is less than its end. A length
  is absent only when its producer can't know it: the segments that
  [feed.go](../../internal/eventstream/feed.go) builds from the cache.
- `splitSegment` sets `stream_length` on each part when the segment has one.
- Tests: existing `Segment` comparisons in
  [local_test.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/eventstream/source/local_test.go) will
  need the new field, so update them. Add cases to
  [eventstreamconsumer_test.go](/Users/james/grit/github.com/dogmatiq/runkit/internal/grpcserver/eventstreamconsumer_test.go)
  that `WatchEvents` and `ReadEvents` set `stream_length`, including on each part
  of a split segment.

### 2. Serve ListEventStreams — [x] written, awaiting review

- Add `(*Local).Streams(ctx) (map[uuid.UUID]uint64, error)`, which runs
  `SELECT id, next_offset FROM runkit.event_streams`. The lengths are read in
  one statement, and the protocol allows each to be from a different moment.
- Add `EventStreamConsumerService.ListEventStreams`. It lists streams that have
  no events too. The number of streams is small (one per aggregate handler
  plus the application's own, not one per instance), and one response stays far
  under gRPC's 4 MiB default limit, so it needs no paging. It runs under the
  call's context, so canceling the call or stopping the engine (the server stops
  and cancels in-flight calls) abandons the query.
- Tests: `Local.Streams` in `local_test.go` (no streams, an empty stream, several
  with different lengths). `ListEventStreams` in the service test against a real
  engine, after events are recorded in more than one stream.

### 3. Serve ReadEventSegment

- Add `(*Local).ReadSegment(ctx, streamID, types, begin uint64, end optional.Optional[uint64]) (Segment, error)`.
  It runs `loadSegment` once, then:
  - returns an error wrapping `ErrOffsetOutOfRange` if `begin` or the given
    `end` is beyond the stream's length (`loadSegment` already checks `begin`),
  - drops the events at or after `end`, and sets `End` to the smaller of `End`
    and `end`, and
  - always returns the stream length.
  An unrecognized stream wraps `ErrUnrecognizedStream`, as `Read` does.
- Add `EventStreamConsumerService.ReadEventSegment`:
  - `INVALID_ARGUMENT` for an invalid stream ID, an invalid filter (reuse
    `requestedTypes`) and `begin > end`, and checks these before it queries;
  - `NOT_FOUND` and `OUT_OF_RANGE` from the source's errors, as `ReadEvents`
    does;
  - cuts the segment with `splitSegment` and sends only the first part, so the
    message never exceeds `MaxSegmentBytes`, and the work is at most one query of
    `SegmentSize` events.
- Tests: `Local.ReadSegment` in `local_test.go` for each rule above, and
  `ReadEventSegment` in the service test with the errors for each status code,
  an empty range at the end of a stream, an open end, an end in the middle of a
  segment, a filter, and **a segment shorter than the range requested**, made by
  recording more than 100 events (`SegmentSize`) and requesting all of them. The
  existing `largeEventsRecordedTogetherAreSplit` test is the pattern for a
  segment cut short by `MaxSegmentBytes`.

### 4. Document

- Fix the doc comment on `EventStreamConsumerService`, which says that the two
  methods are unimplemented, and describe the limits of `ReadEventSegment`.
- Add a line to the `[Unreleased]` "Added" list in
  [CHANGELOG.md](/Users/james/grit/github.com/dogmatiq/runkit/CHANGELOG.md), next to the entry for
  `MetricsAPI`, saying that the engine's gRPC server now serves `ListEventStreams`
  and `ReadEventSegment`, and sets `stream_length`, naming no tables.
- Run `maintaining-changelogs` and `writing-prose` for the wording.

## Risks

- Adding a field to `Segment` changes every test that compares whole segments,
  in `local_test.go`, `memory_test.go` and `remote_test.go`, so task 1 touches
  more test lines than code lines.
- `ReadSegment` reads up to `SegmentSize` events even when `end` is close to
  `begin`, then drops the rest. That is bounded and keeps one query shape, so
  prepared statements are still reused. Adding an end parameter to the SQL is
  possible later if it measures badly.

## Verification

- `make` targets from the `building` skill: run the tests of
  `internal/eventstream/source` and `internal/grpcserver` after each task, then
  `make precommit` once before presenting a large piece of work.
- The service tests use `xtesting.RunEngines`, which starts real engines with a
  real PostgreSQL database, so they check both calls through a real gRPC client,
  including the shorter segment.
- Before the final task, call both methods with a gRPC client (such as
  `grpcurl`) against an engine started with `WithListenAddress()`, if the user
  wants a manual check.
