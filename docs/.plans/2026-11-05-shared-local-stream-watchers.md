# Sources That Mirror the gRPC API, and Each Local Stream Followed Once

> **For agentic workers:** Implement this plan step-by-step. Every numbered
> step is one commit and one review stop; do not batch steps. Build and test
> exclusively through `make` (`make test`, `make precommit`) per the repository
> conventions — never invoke language toolchains directly. Tests spin up
> PostgreSQL via testcontainers, so a working Docker environment is required.

**Goal:** Two things, in this order.

First, `source.Source` mirrors the gRPC `EventStreamConsumerAPI` exactly, as
[dogmatiq/protobuf#18] defines it. `Watch` and `Read` mean what `WatchEvents`
and `ReadEvents` mean, return a distinct error for each failure that those
calls document, and hold no recovery or retry policy. Everything above that —
caching, historical reads, failover, watching again, and how failures are
retried — lives in the catalog. `source.Local` behaves like a server of the
API and `source.Remote` like a client of it, so the server and the client
become thin mappings between the two.

Second, each append to one of this application's streams costs one query per
node, however many remote engines are watching. Today every `WatchEvents` call
runs its own `source.Local.Watch`. A node with M remote engines connected
therefore makes M + 1 segment queries per append, even when no remote engine
wants the appended event's type, and each connect or reconnect issues 1 + 2N
queries for N streams in one burst. Before [PR 64], the engine's one shared
`Reader` served every client.

This came out of the review of PR 64. It changes the local source built in
Phase 3 of [the event feed plan], the catalog's handling of source failures,
and the server and remote source built in its Phases 5 and 6.

**Depends on:** `github.com/dogmatiq/protobuf` v0.7.0, which keeps the filters
from dogmatiq/protobuf#18, removes every error detail, and ends a
`WatchEvents` call with `ABORTED` when the client can't keep up. Only Step 5
needs it.

---

## Decisions that shape the work

- **`Source` mirrors the protocol.**
  - `Watch(ctx, types, fn)` is `WatchEvents`. Each stream's delivery begins at
    its tail with an opening segment, which may be empty and says where delivery
    begins. Every later segment of the stream begins where the previous one
    ended, for the whole call. A new call may begin after events that the caller
    missed, and a caller that needs them reads them with `Read`. When the caller
    can't keep up, `Watch` returns an error that wraps `ErrConsumerTooSlow`,
    which is `ABORTED` on the wire. When a stream can't be delivered
    contiguously for any other reason, it returns another error.
  - `Read(ctx, streamID, types, begin, fn)` is `ReadEvents`. It delivers from
    `begin` until it reaches the end of the stream, as the protocol says. An
    offset equal to the end delivers nothing. An offset past the end returns
    `ErrOffsetOutOfRange`, which is `OUT_OF_RANGE` on the wire, and an unknown
    stream returns `ErrUnrecognizedStream`, which is `NOT_FOUND`.
  - A nil set of types is the filter that matches every type, and any other
    set is a filter of specific types. An empty set is invalid, as an empty
    filter is on the wire.
- **The catalog owns every policy.** It watches a source again at once when the
  source returns `ErrConsumerTooSlow`, without logging an error or backing
  off. It decides which failures are persistent, as `source.Remote` decides
  today: a source that doesn't offer event streams at all, such as a remote
  engine that answers with `UNIMPLEMENTED`; a read of an unrecognized stream or
  from past its end; and an invalid segment from a remote engine, whether
  malformed, holding events of types that weren't requested, or not beginning
  where the previous one ended. No source wraps an error with
  `supervisor.Persistent`.
- **`Watch` is real-time only.** It never reads history. A caller that falls
  behind ends its call and watches again, and the catalog behind it reads what
  it missed with `Read`. A slow caller must never delay delivery to another
  caller.
- **`Local.Run` follows each stream once, for every `Watch` call.** `Run`
  discovers streams and follows each of them for the engine's lifetime, as
  `signal.Bus.Run` does for the bus. A call to `Watch` only registers a
  watcher, which receives the segments that `Run` loads. No `Watch` call
  manages a goroutine that other calls share.
- **Every delivery begins at the stream's end** — for a stream when it's
  discovered, and for a caller when it starts watching. The head start of
  `SegmentSize` events before the end goes, as the protocol's feed starts at
  the tail. Every caller of `Watch` is a caching catalog that already reads
  history for a consumer that's behind. A new call that missed nothing begins
  where the previous call ended, so the receiver keeps its cache.
- **Each watcher has a buffered channel.** `Run` sends each segment to every
  watcher's channel without blocking. When a send would block, `Run` removes
  the watcher and closes its channel, recording the stream it couldn't
  send, under the same mutex that guards sends, as the signal bus does with its
  subscribers. `Watch` delivers what's left in the channel, then returns an
  error that wraps `ErrConsumerTooSlow` and names that stream.
- **Failing to follow a stream ends every call.** After a failure, `Run`
  follows the stream again from its end, so it can't continue any call
  contiguously. `Run` removes every watcher and closes its channel, recording
  the failure, and each `Watch` returns it as an error. The catalog backs off
  and watches again, as it does after any failure.
- **Registering takes a snapshot.** Under that mutex, registering records the
  offset at which each stream's next segment begins, and adds the watcher, so
  the segments sent to the watcher afterwards begin at those offsets. `Watch`
  delivers the opening segment of each stream, empty and at its recorded
  offset, itself, not through the channel, so an opening segment can't be
  dropped.

Calls made in writing this plan, to check while reading it:

- Discovery is supervised separately from following each stream, so a failed
  discovery query only delays finding new streams, rather than ending every
  call. That fixes runkit#76.
- `Watch` may be called before `Run`, as with the bus, and is served once `Run`
  starts.
- Each `Watch` call has one goroutine, which calls `fn` for every stream in
  turn. The contract allows `fn` to be called concurrently for different
  streams, but doesn't require it.
- `Run` loads events of every type. Each watcher's segments are filtered to
  its types in memory and keep their begin and end offsets, so a segment with
  no matching events still moves a watcher past events of other
  types, as it does today.
- `Local` gains a required `BufferSize`, the number of segments buffered for
  each `Watch` call. The engine passes a new constant of 100.
- The engine runs `Local.Run` only when something watches the local source,
  which is when a projection handles events or the gRPC server is enabled.
- An empty set of types is a programming error, so every source panics on it,
  as `Catalog.Consume` does. The server validates the wire filter before
  `Local` sees it.
- The new errors are sentinels in `source` — `ErrConsumerTooSlow`,
  `ErrUnrecognizedStream`, `ErrOffsetOutOfRange` and `ErrInvalidSegment` —
  each wrapped in an error that says what was wrong, including the stream
  where there is one. None needs to carry a stream ID, as the protocol has no
  details. A source that doesn't offer event streams returns an error that
  wraps `errors.ErrUnsupported`.
- The catalog logs `ErrConsumerTooSlow` at INFO before watching again.
- `Read` reads until it reaches the end of the stream, as `ReadEvents` does
  today, rather than stopping at the end as it stood when the call began. The
  offset is checked against the end as of the first query.
- The server drops its own contiguity check, since every source now
  guarantees contiguity within a call, and `source.Remote` validates it
  anyway.
- `source.Remote` recognizes each documented status by its code alone:
  `NOT_FOUND` and `OUT_OF_RANGE` from `ReadEvents`, and `ABORTED` from
  `WatchEvents`. gRPC's own receive limit ends a call with
  `RESOURCE_EXHAUSTED` (runkit#74), which stays an ordinary failure, so
  watching again at once can't loop on the same oversized segment.

---

## Design

Names are proposals; confirm or rename as each step lands.

| Name | Role |
| --- | --- |
| `source.Source` | `Watch` and `Read`, documented to mean exactly what `WatchEvents` and `ReadEvents` mean, with the errors above. |
| `source.ErrConsumerTooSlow` | Wrapped by the error that `Watch` returns when the caller can't keep up. |
| `source.ErrUnrecognizedStream` | Returned by `Read` for an unknown stream. |
| `source.ErrInvalidSegment` | Wrapped by `source.Remote`'s error for a segment that fails validation, from `Watch` or `Read`. |
| `source.ErrOffsetOutOfRange` | Returned by `Read` for an offset past the end of the stream. |
| `Local.Run` | Runs until `ctx` is canceled, and is called at most once. Discovers streams on each `eventstream.create` signal under its own supervisor, and starts following each new stream. Holds the registered watchers and the offset at which each stream's next segment begins, under one mutex. |
| `Local.watchStream` | Unexported. Follows one stream, under its own supervisor. Begins at the stream's end, sending `[end, end)` to every watcher as the stream's opening segment. On each append signal, loads segments of every type from the offset that it has reached and sends each one to every watcher without blocking. After a failure it ends every watcher's call and begins again at the end. Watches signals with a context scoped to each attempt, so a failed attempt leaves no subscriber behind. |
| `watcher` | Unexported, one per `Watch` call — its types, a channel of `BufferSize` segments, and the error that ended it. |
| `Local.Watch` | Registers a watcher, delivers the snapshot's opening segments, then delivers each segment from the channel, filtered to its types. When `Run` closes the channel, returns the error it recorded. Unregisters and returns `ctx`'s error or `fn`'s error, unchanged. |
| `Local.BufferSize` | Required. The number of segments buffered for each `Watch` call. |
| `Catalog.Watch` | Watches the source again at once on `ErrConsumerTooSlow`, and treats `ErrInvalidSegment` and an error that wraps `errors.ErrUnsupported` as persistent. Other failures back off as now. |
| `feed.read` | Treats `ErrUnrecognizedStream`, `ErrOffsetOutOfRange` and `ErrInvalidSegment` from `Source.Read` as persistent. |
| `consumerapi.Server` | A mapping over `source.Local`. Validates the filter, returning `INVALID_ARGUMENT` for an invalid one, and turns it into types. Turns `ErrConsumerTooSlow`, `ErrUnrecognizedStream` and `ErrOffsetOutOfRange` into their statuses. Loses `loadNextOffset`, its `DB` field and its contiguity check. Becomes `source.Server` at Step 6. |
| `source.Remote` | A mapping over the client. Builds the filter from the types, and turns each documented status back into the matching error. Wraps nothing with `supervisor.Persistent`. |

`Local.Watch`, per call:

```
register: under the mutex, record each stream's next offset and add the watcher
for each recorded stream: fn([offset, offset))
for each segment received from the channel: fn(segment filtered to types)
channel closed by Run: return the error Run recorded
ctx canceled, or fn returns an error → unregister and return that error
```

---

## Steps

Every step is green under `make test` unless marked otherwise.

- [x] **Step 1.** Every delivery begins at the stream's end. `loadWatchOffset`
      returns the stream's `next_offset`, so `watchStream`'s opening segment is
      `[end, end)`. The docs of `Local.Watch`, `SegmentSize` and `WatchEvents`
      change to match. The `local_test.go` tests of where a stream begins are
      rewritten, as are the `server_test.go` tests that expect `WatchEvents` to
      begin a stream with the events already on it —
      `TestServer_contemporaryEvents`, `TestServer_historicalEvents`, "it begins
      each existing stream with the events already on it" and "it delivers
      events of every type when no types are requested".
- [x] **Step 2.** The `Watch` contract, as decided above. `ConsumerTooSlowError`
      is added, and the catalog watches again at once when a source returns
      it. `Local.Watch` ends when it fails to follow a stream, instead of
      following the stream again in the middle of the call, so it never begins
      a new delivery of a stream within a call. Every source panics on an empty
      set of types. Catalog tests that deliver a segment that doesn't follow
      the previous one in the middle of a call deliver it on a new call
      instead.
- [x] **Step 3.** The `Read` contract and the error policy. `Local.Read` and
      `Memory.Read` return `ErrUnrecognizedStream` for an unknown stream and
      `ErrOffsetOutOfRange` for an offset past the end. `ReadEvents` becomes a
      mapping over `Local.Read`, losing `loadNextOffset` and the server's `DB`
      field. `source.Remote` turns `NOT_FOUND` and `OUT_OF_RANGE` into those
      errors, and `UNIMPLEMENTED` into an error that wraps
      `errors.ErrUnsupported`, and its errors for invalid segments wrap
      `ErrInvalidSegment`. The catalog, not the remote source, treats all of
      them as persistent. `ConsumerTooSlowError` from Step 2 becomes the
      `ErrConsumerTooSlow` sentinel, as the protocol drops error details.
- [x] **Step 4.** `Local.Run` follows each stream once for every `Watch` call,
      with `BufferSize` and the engine's constant, as described above. The
      engine runs `Local.Run` alongside the bus when the catalog watches the
      local source or the gRPC server is enabled. Tests cover every watcher
      receiving the same segments; a watcher that falls behind getting the
      segments already buffered for it, then `ErrConsumerTooSlow` for the
      stream it couldn't be sent; a stream created while a watcher's channel
      is full; `fn`'s error ending only its own call; `Watch` called before
      `Run`; a failed discovery query leaving every call running; and failing
      to follow a stream ending every call with an error.
- [x] **Step 5.** Take protobuf v0.7.0. `source.Remote` builds the filter from
      the types, and turns `ABORTED` from `WatchEvents` into an error that
      wraps `ErrConsumerTooSlow`. The server validates the filter, returning
      `INVALID_ARGUMENT` for an invalid one, and turns it into types. It
      returns `ABORTED` for `ErrConsumerTooSlow`, and a plain `NOT_FOUND` for
      `ErrUnrecognizedStream` in place of
      `messaginggrpc.UnrecognizedEventStreamError`, which v0.7.0 removes. It
      also drops its own contiguity check. The server test "it delivers every
      event in order to a client that falls behind" now sees `ABORTED`, which
      makes it the server's test of that path. A remote test covers
      `RESOURCE_EXHAUSTED` being an ordinary failure.
- [x] **Step 6.** `consumerapi.Server` moves into `source` as `source.Server`,
      taking the `Source` interface rather than `*Local`, so that it mirrors
      `source.Remote`: one turns a client into a `Source`, and the other turns
      a `Source` into the service. Its tests move with it, and the
      `consumerapi` package goes. The engine still gives it only the local
      source, so the server never relays a remote engine's streams. A move
      only, with no change in behavior.
- [ ] **Step 7.** CHANGELOG check. Comment on runkit#74, #75 and #76 with what
      this changed, and close #76. `make precommit` green.

---

## Out of scope (explicitly deferred)

- **Sharing work between reads.** Each `Read` call, and so each `ReadEvents`
  call, still queries on its own.
- **The other findings from the review of PR 64**, recorded as runkit#73, #74,
  #77, #78 and #79.

<!-- references -->

[dogmatiq/protobuf#18]: https://github.com/dogmatiq/protobuf/pull/18
[PR 64]: https://github.com/dogmatiq/runkit/pull/64
[the event feed plan]: 2026-11-04-event-feed-consumption.md
