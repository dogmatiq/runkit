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
`WatchEvents` and `ReadEvents`, both returning `EventStreamSegment`. Steps 0–10
do not need it; step 11 onward does.

---

## The protocol

- `WatchEvents(message_type_ids) returns (stream EventStreamSegment)` is held
  open for as long as the client uses the engine. It delivers events of the
  requested types as they are appended to any stream. There is no stream
  listing — the first segment for a stream positions it at the tail, and every
  later segment for that stream begins where the previous one ended. A server
  that can no longer deliver a stream contiguously ends the call; a client that
  reconnects expects to have missed events and treats the new call's first
  segment per stream as a fresh position. A server may position every stream
  or only streams with a matching event. Runkit's server positions every
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
  consumer whose checkpoint is below the cache window reads history on its own
  goroutine: locally with the historical SQL, remotely with one `ReadEvents`
  call that is canceled once the consumer reaches the window.
- **The cache is sparse.** The remote feed is type-filtered, so the cache holds
  `(offset, envelope)` pairs with gaps and tracks its window separately. The
  window's end advances with every segment, empty or not. Its beginning is the
  smallest offset for which the cache is complete: when the oldest event at
  offset `o` is evicted, the beginning becomes `o+1`. A consumer at checkpoint
  `cp` is servable if the window contains `cp`, and receives every cached event
  at or after `cp` of a type it wants. The local scanner's output is the dense
  special case, so one cache type serves both sources.
- **A synchronous callback, on the consumer's goroutine.**
  `Consume(ctx, streamID, types, begin, fn func(ctx, Segment) error) error`
  blocks until `ctx` is canceled, `fn` returns an error, or the stream is no
  longer offered, in which case it returns `ErrStreamNotOffered`; it never
  returns nil. `fn` receives contiguous segments starting at `begin`, across
  cache, history, resets and source changes, empty segments included. The
  eventstream side owns the loop; `fn` owns what happens per segment. There is
  no channel, no unsubscribe function, no error future and no buffer size:
  "nothing is delivered after the subscription ends" is true by construction,
  the reason for ending is the return value, and back-pressure is the time `fn`
  takes. History-read failures surface to the caller; nothing is retried inside
  `Consume`.
- **Projections never see sources.** Each stream has one current source — the
  source that offered it and is still running. A second source offering a
  stream that already has a live offer is appended behind it; its cache is fed
  but consumers ignore it until the first offer is withdrawn, then it becomes
  current and in-flight `Consume` calls continue from their checkpoint. History
  reads go to the stream's current source. Projections see only which streams
  are offered, and start or stop a stream consumer per stream as they do today.
- **A stream stays offered across a feed reconnect.** A new `WatchEvents` call
  repositions the source's existing offer of each stream, the same path as a
  local scanner restarting, rather than withdrawing and re-offering it, so a
  blip does not stop and restart every stream consumer. Offers are withdrawn
  only when their source is removed or stops.
- **The server never relays.** `ConsumerAPI` consumes from the local source
  directly, never from the catalog, so a local stream whose current offer
  happens to be remote (this engine dialed as a remote engine) is not served
  on from here. `WatchEvents` positions each local stream at its `next_offset`
  and runs a tail consumer per stream with the client's filter, serializing
  sends onto the one gRPC stream; if the client is slow, each per-stream
  consumer reads history inline, so the feed stays contiguous. `ReadEvents`
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
- **An application with no projection event types** still opens a feed per
  remote engine, with an empty filter. Streams are positioned and nothing is
  delivered. One idle call per engine, and no special case.
- **The `scanner` subpackage is folded into `eventstream`**, and
  `internal/ringbuffer` is deleted; the cache is copy-on-write.

---

## Design

Names are proposals; confirm or rename as each step lands.

| Type | Role |
| --- | --- |
| `Segment` | `{StreamID, Begin, End uint64, Envelopes}` — a contiguous piece `[Begin, End)` of one stream with the matching events. Replaces `scanner.Batch`. |
| `cache` | Bounded, sparse, copy-on-write snapshot of one stream's recent events in an `xsync.Observable`. `append(seg)` (single writer; evicts from the front), `reset(begin)`, `read(cp, types)` returning the segment from `cp`, a status (behind, within, caught up, ahead) and the snapshot's change channel. |
| `Offer` | One source's offer of one stream — a `cache`, the `Source` for history, a withdrawn latch and a context canceled on withdrawal. `Append`, `Reset` and `Withdraw` for sources; `consume` runs the loop. |
| `Source` | The one cross-source interface: `ReadHistory(ctx, streamID, types, begin, fn) error` delivers contiguous segments from `begin` to the end of the stream as it stands, then returns nil. There is no contemporary-side abstraction; local reads the tail per stream and remote per engine, so what they share is what they produce, pushed through `Catalog.Offer`, `Offer.Append` and `Offer.Reset`. |
| `Catalog` | Maps each stream to its offers under one mutex, with the current offer in an `xsync.Observable` per stream; publishes `StreamIDs()`; `Consume()` runs the current offer's loop and moves to the next offer when the current one is withdrawn. No `Run`. `Withdraw` republishes the stream set before setting the offer's latch, so a consumer never spins on a stale set. |
| `LocalSource` | Discovers streams as `LocalCatalog` does, runs a supervised contemporary scanner per discovered stream into an `Offer`, implements `ReadHistory` with the historical SQL, and exposes `StreamIDs()` and `Consume()` over its own offers for the server. |
| `RemoteSource` | One per remote engine. Runs one `WatchEvents` call under the supervisor, offering or resetting a stream on its first segment and appending after; implements `ReadHistory` with one `ReadEvents` call. |
| `ConsumerAPI` | `WatchEvents` and `ReadEvents` over `*LocalSource`. |

The consume loop, per offer:

```
cp := begin
loop:
  seg, status, changed := cache.read(cp, types)
  within:            fn(seg); cp = seg.End
  behind:            cp = source.ReadHistory(cp) into fn, stopping once cp
                     reaches the window (re-read after every segment); nil
                     means the window moved, so loop
  caught up, ahead:  wait on ctx, changed, or withdrawn
withdrawn → the catalog reloads the current offer and continues from cp, or
returns ErrStreamNotOffered
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

- [ ] **Step 3.** `segment.go` and `cache.go` with tests: append, evict and
      window arithmetic on dense and sparse input; `read` status at every
      boundary; type filtering; reset; the change channel closes on append and
      reset; a `rapid` property — for random segment sequences and random `cp`
      within the window, `read` yields exactly the matching events at or after
      `cp`.
- [ ] **Step 4.** `source.go` and `offer.go` with tests over a slice-backed
      stub source: behind → history then cache, no duplicates, contiguous
      segments into `fn`; the window moving during a read triggers a second
      read; `fn`'s error propagates unwrapped; withdrawal during a wait and
      during a read; ahead and caught up wait; empty segments delivered.
- [ ] **Step 5.** `catalog.go` with tests: the stream set is published on the
      first offer and the last withdrawal, before a consumer observes
      `ErrStreamNotOffered`; `Consume` on an unoffered stream returns at once;
      a stream offered again after withdrawal serves a new `Consume`. The old
      `Catalog` interface is renamed out of the way (its file becomes
      `legacy.go` until step 8) so the new type can take the name.

### Phase 3: Local source (old protobuf)

- [ ] **Step 6.** New local readers inside `eventstream`, copied from the
      `scanner` package without touching it: a contemporary reader delivering
      segments through a `Deliver func(Segment)` field, and a history-read
      function with the historical SQL. Tests ported from `scanner`.
- [ ] **Step 7.** `localsource.go` with tests ported from
      `localcatalog_test.go`'s intent against `Consume`: historical then
      contemporary in order; filtering across both; many slow consumers each
      receive every event; a clean stop with consumers in both phases;
      discovery. A temporary `Subscribe` shim over `Consume` keeps
      `ConsumerAPI` and `projection` compiling. `LocalCatalog` is still wired.
- [ ] **Step 8.** Switch over: `engine.go` builds `Catalog` and `LocalSource`;
      `ConsumerAPI` and `projection.Consumer` take the shim; delete
      `localcatalog.go`, `legacy.go`, the `scanner` package and
      `internal/ringbuffer`. The existing suite passes unchanged.

### Phase 4: Projection on `Consume` (old protobuf)

- [ ] **Step 9.** `streamConsumer.consume` becomes one `Consume` call whose
      `fn` holds today's dispatch loop. The handler's checkpoint is advanced
      only by handled events (dogma#221), never by a segment's end. On
      `ErrStreamNotOffered` it waits until the stream is offered again and
      consumes from the handler's checkpoint; any other error returns to the
      supervisor. `consumeSubscription` and `BufferSize` go; `consumer_test.go`
      is rewritten over a real `Catalog` with a stub source.
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
      stream created mid-call positioned, per-stream contiguity, an empty
      filter positioning only. `grpcserver/server_test.go` and
      `engine_test.go` swap `ListEventStreams` for `WatchEvents`.

### Phase 6: Remote source (new protobuf)

- [ ] **Step 12.** `validatesegment.go` with a test per rule, each error
      naming the stream and offsets.
- [ ] **Step 13.** `remotesource.go` with deterministic tests over the client
      stub or an in-process stub server: positioning and appending; the feed
      ending → retried with the existing offer repositioned; a validation
      failure, `UNIMPLEMENTED`, `NOT_FOUND` and `OUT_OF_RANGE` → persistent;
      `ReadHistory` cancels its call when `fn` stops it; reconnect catch-up —
      the first call delivers `[0, 1)` then fails, the second positions at
      `[3, 3)`, `ReadEvents` is called from 1, and the consumer sees offsets 0,
      1 and 2 once each, in order.
- [ ] **Step 14.** Tests for `RemoteSource` against a real cluster: streams
      appear in the catalog; history via `ReadEvents` then the tail via the
      feed; a stopped node → reconnect → consumers continue. Tests only.

### Phase 7: Wire it up end to end (new protobuf)

- [ ] **Step 15.** `Connector` builds a `RemoteSource` per connection and
      runs it; `remove` cancels and waits. `engine.go` adds
      `consumedEventTypeIDs()` — the union of inbound event types of enabled
      projections — and passes it with the cache size. `connector_test.go` is
      rebuilt over a `WatchEvents` stub server.
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

- [ ] **Step 18.** `Catalog`: a second offer for a stream is appended, its
      cache fed, and ignored until the first is withdrawn, then becomes
      current and in-flight `Consume` calls continue from their checkpoint.
      Unit tests without a database.
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

- **Positioning only streams with a matching event** on the server. The
  protocol allows it; runkit's server positions every stream for now.
- **Discoverers other than `StaticRemoteEngines`**, process handlers consuming
  foreign events, hot-spot mitigation, heterogeneous connectivity and relay
  topologies, as in [the previous plan].

<!-- references -->

[dogmatiq/protobuf#11]: https://github.com/dogmatiq/protobuf/issues/11
[the previous plan]: 2026-09-29-projection-event-consumption.md
