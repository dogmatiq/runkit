# Projection Event Consumption over `WatchEvents` and `ReadEvents`

> **For agentic workers:** Implement this plan step-by-step. Every numbered
> step is one commit and one review stop; do not batch steps. Build and test
> exclusively through `make` (`make test`, `make precommit`) per the repository
> conventions — never invoke language toolchains directly. Tests spin up
> PostgreSQL via testcontainers, so a working Docker environment is required.

**Goal:** Projections consume events from this application's streams and from
foreign applications on remote engines through one model: a per-stream cache of
recent events fed by a contemporary source, and a per-consumer historical read
for a consumer that has fallen behind the cache. Locally the contemporary source
is a scanner per stream over PostgreSQL. Remotely it is one `WatchEvents` call
per remote engine, so that in steady state a node holds one gRPC call per remote
engine, however many projections consume its streams.

This supersedes the catalog model in [the previous plan] from its Task A.6
onward. Everything that plan delivered before A.6 — remote engine discovery,
`remoteengine.Connector`, the `xtesting` cluster harness, and the end-to-end
tests — is kept and adapted.

"Peer" is reserved for another node in the same cluster. An application that
this engine consumes events from is a "foreign application", and the engine it
is consumed from is a "remote engine". A "source" is where a stream's events
come from: this application's database, or a remote engine.

**Depends on:** `github.com/dogmatiq/protobuf` v0.3.0, which implements
[dogmatiq/protobuf#11], replacing `ListEventStreams` and `ConsumeEvents` with
`WatchEvents` and `ReadEvents`, both returning `EventStreamSegment`, plus the
follow-up that makes an empty `message_type_ids` match every event. Steps 0–10
do not need it; step 11 onward does.

---

## The protocol

- `WatchEvents(message_type_ids) returns (stream EventStreamSegment)` is held
  open for as long as the client uses the engine. It delivers events of the
  requested types as they are appended to any stream. There is no stream
  listing — a stream appears on the feed with its first segment, which begins
  at the stream's tail, and every later segment for that stream begins where
  the previous one ended. A server that can no longer deliver a stream
  contiguously ends the call; a client that reconnects expects to have missed
  events and takes the new call's first segment per stream as the offset from
  which that stream is delivered. A server may include every stream on the
  feed or only streams with a matching event. Runkit's server includes every
  stream.
- `ReadEvents(event_stream_id, begin_offset, message_type_ids) returns (stream
  EventStreamSegment)` delivers events from `begin_offset` to the end of the
  stream as it stands, then closes. The caller cancels it once it has caught
  up. `NOT_FOUND` and `OUT_OF_RANGE` keep their meaning. Nothing is claimed
  about the future, so the server needs no knowledge of which types it can
  record.
- `EventStreamSegment { event_stream_id, begin_offset, end_offset, envelopes }`
  is a contiguous piece of one stream, `[begin_offset, end_offset)`, carrying
  the events in it that match the requested types. It may be empty.

---

## Decisions that shape the work

- **One split, two sources.** A per-stream cache of recent events is fed by a
  contemporary source: locally, a scanner per stream; remotely, one
  `WatchEvents` feed per engine, filtered to the union of every projection's
  event types, which is known when the engine is built and never changes. A
  consumer whose checkpoint is before the cached segment reads history on its
  own goroutine: locally with the historical SQL, remotely with one
  `ReadEvents` call that is canceled once the consumer reaches the segment.
- **The cache holds a sparse segment.** The remote feed is type-filtered, so
  the segment a cache holds has gaps between the events it holds, but it is
  complete for the types delivered. Its end advances with every segment
  appended, empty or not. Its beginning is the smallest offset for which the
  cache is complete: when the oldest event at offset `o` is evicted, the
  beginning becomes `o+1`. A consumer at checkpoint `cp` is servable if the
  cached segment contains `cp`, and receives every cached event at or after
  `cp` of a type it wants. The local scanner's output is the dense special
  case, so one cache type serves both sources.
- **A synchronous callback, on the consumer's goroutine.**
  `Consume(ctx, streamID, types, begin, fn func(ctx, Segment) error) error`
  blocks until `ctx` is canceled, `fn` returns an error, or the catalog no
  longer has the stream, in which case it returns `ErrStreamNotFound`; it never
  returns nil. `fn` receives contiguous segments starting at `begin`, across
  cache, history, resets and source changes, empty segments included. The
  eventstream side owns the loop; `fn` owns what happens per segment. There is
  no channel, no unsubscribe function, no error future and no buffer size:
  "nothing is delivered after the subscription ends" is true by construction,
  the reason for ending is the return value, and back-pressure is the time `fn`
  takes. History-read failures surface to the caller; nothing is retried inside
  `Consume`.
- **Projections never see sources.** Each stream is consumed via one of its
  feeds, which changes only when that feed's source stops being watched. A
  second source delivering a stream that already has a current feed is queued
  behind it; its feed is appended to but consumers ignore it until the current
  feed is closed, then it becomes current and in-flight `Consume` calls
  continue from their checkpoint. Reads go to the current feed's source.
  Projections see only which streams the catalog has, and start or stop a
  stream consumer per stream as they do today.
- **The whole source contract is one interface, mirroring the protocol.** A
  source implements `Watch(ctx, types, fn)`, which delivers each of its
  streams' events of the given types to `fn` in contiguous segments as they
  are appended (nil meaning every type, as an empty filter does on the wire,
  which is how the local source is watched so that the server can serve any
  filter), and `Read(ctx, streamID, types, begin, fn)`, which delivers a
  range. The catalog passes the types to `Watch`, so it knows what each
  source delivers and `Consume` rejects a request for any other type. As on
  the wire, there is no "open": a stream's first segment adds it to the
  catalog from its begin offset, and a segment that does not begin where the
  previous one ended restarts the feed from its begin offset (a scanner
  restarting, or a `WatchEvents` call reconnecting; a reconnect that happens
  to be contiguous keeps the cache). The catalog's own `Watch` runs the
  source's under the supervisor until its context is canceled, then removes
  the source's feeds, so a source never manages a lifetime, and a stream stays
  in the catalog across a failed and retried `Source.Watch`, so a blip does
  not stop and restart every stream consumer.
- **The server never relays.** `ConsumerAPI` consumes only streams watched via
  the local source, never a remote engine's watch of a local stream (this
  engine dialed as a remote engine), so nothing is served on from here.
  `WatchEvents` delivers each local stream from its `next_offset` and runs a
  tail consumer per stream with the client's filter, serializing sends onto
  the one gRPC stream; if the client is slow, each per-stream consumer reads
  from the source inline, so the call stays contiguous. `ReadEvents`
  consumes from `begin_offset` until the stream's `next_offset` as loaded at
  the start of the call.
- **Remote segments are validated** before they touch a cache: the stream ID
  validates and matches; the segment begins where the previous one ended; each
  envelope carries an `EventStreamPosition` for the same stream with offsets
  strictly increasing and within the segment; each type is one that was
  requested. A violation is a persistent failure of the feed or the read.
- **Failures.** `UNIMPLEMENTED` from a remote engine is a persistent failure of
  its feed, retried on the slow schedule; the consumer can not tell a server
  without the service from an engine without event streams, so it handles both
  the same way. `NOT_FOUND` and `OUT_OF_RANGE` from `ReadEvents` are persistent
  failures of that read. The feed ending for any reason, `io.EOF` included, is
  a failure, since it must never end.
- **An application with no projection event types** adds no remote sources:
  since an empty filter means every type, a `WatchEvents` call with the empty
  union would deliver every event of every stream to a node that consumes
  none of them. The connector still runs its discoverer, so the engine's
  behavior is unchanged in every other respect.
- **The `scanner` subpackage is folded into `eventstream`**, and
  `internal/ringbuffer` is deleted; the cache is copy-on-write.

---

## Design

Names are proposals; confirm or rename as each step lands.

| Type | Role |
| --- | --- |
| `source.Segment` | In `internal/eventstream/source`; `eventstream.Segment` is an alias. `{StreamID, Begin, End uint64, Envelopes}` — a contiguous piece `[Begin, End)` of one stream with the matching events. Replaces `scanner.Batch`. |
| `source.Source` | The whole source contract. `Watch(ctx, types, fn func(ctx, Segment)) error` delivers each of the source's streams' events of the given types (nil: every type) to `fn` in contiguous segments as they are appended, until `ctx` is canceled or it fails, in which case it is called again; a stream's first segment, or a non-contiguous one, begins a new delivery of the stream from its begin offset. `Read(ctx, streamID, types, begin, fn) error` delivers contiguous segments from `begin` to the end of the stream as it stands, then returns nil, returning `fn`'s error unchanged. |
| `source.Local`, `source.Remote` | The two sources (Steps 7 and 13). Neither knows about the catalog or caches. `source.Memory` is a third, holding its streams in memory, for tests. |
| `cache.Cache` | In `internal/eventstream/internal/cache`. Holds a bounded, sparse, copy-on-write `cache.Segment` of one stream's recent events in an `xsync.Observable`. Writers call `Append(begin, end, envelopes)` (evicts from the front) and `Reset(begin)`; readers take `Segment()`, an `xsync.ObservableR[cache.Segment]`, and call `Read(cp, types)` on a snapshot, which returns the matching envelopes from `cp` and a `Relation` (`Before`, `Within`, `After`, valued like `cmp.Compare`). The snapshot carries `Begin` and `End`. The package knows nothing of `source.Segment`; the stream assembles those from what it reads. |
| `feed` | In `eventstream`. The catalog's feed of one event stream from one source — a `cache.Cache`, the `source.Source` to read from, a closed latch and a context canceled on closing. `Append` (a non-contiguous segment restarts the feed) and `Close` for the catalog; `consume` runs the loop. |
| `Catalog` | In `eventstream`. `Watch(ctx, src, types) error` runs `src.Watch` with the given types under the supervisor, with an `fn` bound to the source that adds, appends to, or restarts the segment's feed, until `ctx` is canceled, then removes the source's feeds and returns. Maps each stream ID to its feeds under one mutex, one of them current, published through an `xsync.Observable` per ID; publishes `StreamIDs()` as an `xsync.ObservableR`; `Consume()` runs the current feed's loop and moves to the next when it is closed. Removal republishes the stream set before closing the feed, so a consumer never spins on a stale set. Carries `CacheSize` and `Logger`. |
| `ConsumerAPI` | `WatchEvents` and `ReadEvents`, consuming only streams watched via `source.Local` (how the catalog exposes that is settled at Step 8). |

The consume loop, per stream:

```
cp := begin
loop:
  seg, changed := cache.Segment().Load(); envelopes, rel := seg.Read(cp, types)
  Within:  fn(segment [cp, seg.End) of envelopes); cp = seg.End
  Before:  cp = source.Read(cp) into fn, stopping once cp reaches the cached
           segment (compared after every segment delivered); nil means the
           cached segment moved on, so loop
  After:   wait on ctx, changed, or closed
closed → the catalog reloads the current feed and continues from cp, or
returns ErrStreamNotFound
```

---

## Steps

Every step is green under `make test` unless marked otherwise.

### Phase 0: Reset

- [x] **Step 0.** Discard the uncommitted Task A.6 work (kept on
      `remote-engines-a6-wip`); `make test` green at HEAD.
- [x] **Step 1.** Add this plan; mark [the previous plan] superseded from Task
      A.6. Commit.

### Phase 1: Detach remote consumption (old protobuf)

- [x] **Step 2.** Delete `compositecatalog.go`, `remotecatalog.go` and their
      tests. `engine.go` drops `allEventStreams`, its wiring and
      `eventStreamRecheckInterval`, and gives projections
      `e.localEventStreams`. `remoteengine/connector.go`'s `add` returns a
      no-op remove (discoverer supervision, panic recovery and the "returned
      early" error stay) and loses `EventStreamRecheckInterval`;
      `connector_test.go` keeps only the two discoverer-retry cases.
      `projection/foreignevents_test.go` gets a `t.Skip` per test naming this
      plan. One commit; nothing in it survives.

### Phase 2: New building blocks, no callers yet (old protobuf)

- [x] **Step 3.** `segment.go`, and the `cache` package with tests: append,
      evict and bounds arithmetic on dense and sparse input; `Read` status at
      every boundary; type filtering; reset; the change channel closes on
      append and reset; a `rapid` property — for random ranges and random `cp`
      within the cached segment, `Read` yields exactly the matching events at
      or after `cp`.
- [x] **Step 4.** The `source` package (`Segment`, `Source`, and `Memory`, an
      in-memory source for tests) and `feed.go`, tested through the
      catalog in step 5 over a memory
      source: before → read from the source then cache, no duplicates,
      contiguous segments into `fn`; the cached segment moving on triggers a
      second read; the read stops as soon as the checkpoint reaches the cached
      segment; `fn`'s error propagates unwrapped; closure during a wait and
      during a read; after waits; empty segments delivered.
- [x] **Step 5.** `catalog.go` with tests over the memory source: `Watch` runs
      `Watch` and retries it; a stream's first segment adds it, a contiguous
      one extends it, a non-contiguous one restarts it; the stream set is
      published when a source first delivers a stream and when the last source
      that delivered it is removed,
      before a consumer observes `ErrStreamNotFound`; `Consume` on a stream
      the catalog does not have returns at once; canceling `Watch` closes the
      source's feeds before it returns. The old `Catalog`
      interface is renamed out of the way (its file becomes `legacy.go` until
      step 8) so the new type can take the name.

### Phase 3: Local source (old protobuf)

- [x] **Step 6.** `source.Local` with tests through its own `Watch` and
      `Read`, ported from the `scanner` tests: `Watch` delivers each stream
      from its most recent events, from the beginning if it is short, events
      appended later, in several segments, streams created later, only the
      given types, and an empty segment for an empty stream; `Read` delivers
      one segment to the end, several when over the segment size, from the
      given offset, past events of other types, every type if none given, an
      empty segment when only other types exist, nothing from an empty stream
      or from beyond its end, `fn`'s error unchanged, and fails for a missing
      stream. The `scanner` package is left in place for `LocalCatalog`.
      `Local` discovers and watches streams through `signal.Bus.Watch`, an
      iterator that yields once when iteration begins and then after each
      signal on a topic, coalescing signals that arrive while the loop body
      runs, so that a watcher neither subscribes before reading nor keeps a
      buffer; `Subscribe` remains for the heartbeat loop, which needs each
      signal's topic.
- [x] **Step 7.** `Catalog.Legacy()`, a temporary adapter that presents the
      catalog as a `LegacyCatalog`, so that `ConsumerAPI` and `projection` can
      take it unchanged. Each `Subscribe` consumes the stream on its own
      goroutine, retrying failed reads, and reports the subscription lost
      once the catalog no longer has the stream. Tests for the adapter, and
      `localcatalog_test.go`'s intent against a catalog watching a
      `source.Local`: from the database then as appended, in order;
      filtering across both; streams independently; many slow consumers;
      a clean stop. `fn` is now always given the consumer's own context, so
      that a segment it has been given is handled to completion even if the
      feed closes. `LocalCatalog` is still wired.
- [x] **Step 8.** Switch over: `engine.go` builds the `Catalog`, watches a
      `source.Local` with it for the life of the engine, and gives `ConsumerAPI`
      and `projection.Consumer` the adapter. The engine runs its components in
      an errgroup it holds, including each handler's; `runComponent` and the
      `component` interface are gone. `ConsumerAPI` takes the
      `LegacyCatalog` interface; with only the local source in the catalog until
      step 15, nothing can be relayed yet, so the never-relay rule is settled at
      step 11. The adapter waits for a stream the catalog has not discovered
      yet, as the server may be asked for one that is already in the database.
      `eventStreamBatchSize` becomes `eventStreamSegmentSize`.
      `localcatalog.go`, the `scanner` package, `internal/ringbuffer` and
      `signal.Bus.Subscribe` are deleted, and the node tests watch signals
      instead. The existing suite passes; under three concurrent runs of every
      package it shows the same timing failures as before the switch.

### Phase 4: Projection on `Consume` (old protobuf)

- [x] **Step 9.** `streamConsumer.consume` becomes one `Consume` call whose
      `fn` holds today's dispatch loop. The handler's checkpoint is advanced
      only by handled events (dogma#221), never by a segment's end. On
      `ErrStreamNotFound` it waits until the catalog has the stream again and
      consumes from the handler's checkpoint; any other error, including a
      failed read, returns to the supervisor, which resumes from the handler's
      checkpoint. `consumeSubscription` and `BufferSize` go. `consumer_test.go`
      is rewritten over a real `Catalog` with memory sources: a stream that
      joins the catalog is consumed; a stream that leaves and returns is
      consumed from where the handler left off; a stream continues when
      another source takes over. That the consumer stops a stream consumer
      when its stream leaves is not observable through the catalog, which
      can deliver nothing for a stream it does not have, so it has no test.
- [x] **Step 9a.** `cluster.Node` publishes its view of the cluster as one
      observable, `Node.Membership() xsync.ObservableR[*cluster.Membership]`,
      replacing `Node.Peers`. A `Membership` is an immutable snapshot of the
      live nodes. `LocalNodeIsResponsibleFor(workload)` is the rendezvous
      hash over them, moved from the node, and is false for every workload
      while the local node can not hear its own heartbeats. A nil `Membership` has no live nodes, and a stopped node
      publishes an empty one. `Peers()` replaces `Node.Peers`.
      `ObserveResponsibility` stays, computing responsibility from the
      membership, until step 9b. `node_test.go` and `xtesting`'s
      cluster convergence move onto `Membership`, with a new case for a
      stopped node being responsible for nothing.
- [x] **Step 9b.** The projection consumer reconciles in one place. Before
      this, three layers react to overlapping changes: the `Consumer` starts
      and stops a stream consumer as streams join and leave the catalog; each
      stream consumer starts and stops consumption as the node's
      responsibility for its workload changes; and a stream consumer whose
      stream leaves waits for it to return while the `Consumer` stops it.
  - `projection.Consumer.Run` is one loop. It waits for the catalog's stream
    IDs or the membership to change, works out the streams this node is
    responsible for, and starts or cancels a stream consumer for each
    difference. A canceled stream consumer is waited for before the same
    stream can be consumed again on this node.
  - `streamConsumer` loses `Node`, `Workload`, `step` and `waitForStream`.
    It loads the handler's checkpoint and calls `Consume` under the
    supervisor until it is canceled. `ErrStreamNotFound`, which it can see
    only in the moment before the loop cancels it, is retried like any other
    failure.
  - `ObserveResponsibility` and its observers are deleted, and its tests in
    `node_test.go` move onto `Membership`; the two about unobserving go.
    `supervisor.Go`, which only `step` used, is deleted.
  - `consumer_test.go` gains a case for a node that becomes responsible for
    a stream, by starting the consumer's node after the stream is in the
    catalog. Responsibility moving away is not tested here: a consumer that
    has not yet noticed may still handle an event appended after the test
    sees the new membership, so the test would be racy. The distribution
    tests cover it end to end.
- [ ] **Step 10.** `ConsumerAPI.ConsumeEvents` rewritten over `Consume` on the
      old protocol, deleting the shim; `grpc_test.go` passes unchanged. This
      shrinks step 11 to the wire mapping.

### Phase 5: Protocol switch (new protobuf; one red-window commit)

- [ ] **Step 11.** Bump `github.com/dogmatiq/protobuf`. `grpc.go` gains
      `WatchEvents` and `ReadEvents` and loses `ListEventStreams`,
      `ConsumeEvents`, `RecordableEventTypes`, `BufferSize`, `canRecordAny`
      and `loadEventStream`; `engine.go` loses `recordableEventTypes()` and
      `eventStreamBufferSize`. `grpc_test.go` is rewritten: `NOT_FOUND` with
      its detail, `OUT_OF_RANGE`, read to the end then close, the head
      reported, events appended after connecting, empty segments advancing
      past irrelevant types, a slow client receiving a thousand events in
      order, `ReadEvents` closing at the end as of the start of the call, a
      stream created mid-call appearing on the feed, per-stream contiguity, an
      empty filter delivering every type.
      `grpcserver/server_test.go` and `engine_test.go` swap `ListEventStreams`
      for `WatchEvents`. The server must not relay a remote source's feed of
      a local stream, and must not answer `Unavailable` for a stream that is
      in the database but not yet discovered; reading straight from the
      `source.Local` for `ReadEvents`, and watching it for `WatchEvents`,
      would settle both. Once the server no longer goes through the catalog,
      the engine watches the local source for the union of its projections'
      event types instead of every type, and not at all if there are none.
      Until then, every node reads every event appended to every local stream,
      which made the slow-client test five times slower; it was cut to 200
      larger events to stay within its time limit.

### Phase 6: Remote source (new protobuf)

- [ ] **Step 12.** `validatesegment.go` with a test per rule, each error
      naming the stream and offsets.
- [ ] **Step 13.** `source.Remote` with deterministic tests over the client
      stub or an in-process stub server: `Watch` delivers each segment as it
      arrives; the call ending → `Watch` returns an error, and the next call's
      first segment per stream restarts it at its new offset;
      a validation failure, `UNIMPLEMENTED`, `NOT_FOUND` and `OUT_OF_RANGE`
      → persistent; `Read` cancels its call when `fn` stops it; reconnect
      catch-up through a `Catalog` — the first call delivers `[0, 1)` then
      fails, the second delivers from `[3, 3)`, `ReadEvents` is called from
      1, and the consumer sees offsets 0, 1 and 2 once each, in order.
- [ ] **Step 14.** Tests for `source.Remote` against a real cluster: streams
      appear in the catalog; the range via `ReadEvents` then the tail via
      `WatchEvents`; a stopped node → reconnect → consumers continue. Tests
      only.

### Phase 7: Wire it up end to end (new protobuf)

- [ ] **Step 15.** `Connector` adds a `source.Remote` per connection to the
      catalog with the projection event-type union, and removes it on
      removal. `engine.go` adds `consumedEventTypeIDs()` — the union of
      inbound event types of enabled projections — and passes it to the
      connectors; the local source is added with no limit.
      `connector_test.go` is rebuilt over a `WatchEvents` stub server.
- [ ] **Step 16.** Un-skip `foreignevents_test.go`. The historical,
      contemporary, local-and-foreign and upstream-node-stops cases pass as
      they are. "Does not retry immediately" becomes: a projection handling
      none of a remote engine's types opens at most the initial `ReadEvents`
      per stream and none after a quiet period, and the node holds exactly one
      `WatchEvents`. A per-method call-counting dial helper replaces the inline
      interceptor.
- [ ] **Step 17.** New end-to-end scenarios: two projections on one node share
      one `WatchEvents`; a feed reconnect (an interceptor fails the first
      `RecvMsg` after N messages) leaves every event handled once. Tests only.

### Phase 8: Takeover between sources

- [ ] **Step 18.** `Catalog`: a second source delivering a stream is queued
      behind the current one, its cache appended to, and ignored until the
      first source is removed, then becomes current and in-flight `Consume`
      calls continue from their checkpoint. Unit tests without a database.
- [ ] **Step 19.** End to end: two connections to one upstream cluster; remove
      the one serving a stream; events keep arriving, each handled once.
      Tests only.

### Phase 9: Docs and housekeeping

- [ ] **Step 20.** CHANGELOG check (the existing entries stay true; nothing
      shipped with the old protocol, so no `[BC]`); fix the
      `providesutilities` typo in `eventstream/doc.go`; `make precommit`
      green.
- [ ] **Step 21.** Rewrite the PR 64 body. Close runkit#65 (one feed per
      engine) and #61 (the catalogs it compares no longer exist); close #67
      with step 12; edit #60 to drop the `Subscribe`/`lost` bullet.

---

## Out of scope (explicitly deferred)

- **Delivering only streams with a matching event** on the server. The
  protocol allows it; runkit's server includes every stream on the feed for
  now.
- **Discoverers other than `StaticRemoteEngines`**, process handlers consuming
  foreign events, hot-spot mitigation, heterogeneous connectivity and relay
  topologies, as in [the previous plan].

<!-- references -->

[dogmatiq/protobuf#11]: https://github.com/dogmatiq/protobuf/issues/11
[the previous plan]: 2026-09-29-projection-event-consumption.md
