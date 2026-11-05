# One Watcher per Local Event Stream

> **For agentic workers:** Implement this plan step-by-step. Every numbered
> step is one commit and one review stop; do not batch steps. Build and test
> exclusively through `make` (`make test`, `make precommit`) per the repository
> conventions — never invoke language toolchains directly. Tests spin up
> PostgreSQL via testcontainers, so a working Docker environment is required.

**Goal:** Each append to one of this application's event streams costs one
query per node, however many remote engines are watching the node's streams.
Today every `WatchEvents` call runs its own `source.Local.Watch`. A node with M
remote engines connected therefore makes M + 1 segment queries per append, even
when no remote engine wants the appended event's type, and each connect or
reconnect issues 1 + 2N queries for N streams in one burst. Before [PR 64], the
engine's one shared `Reader` served every client.

This came out of the review of PR 64. It changes the local source built in
Phase 3 of [the event feed plan], and how the server and the remote source
handle a stream that can't be delivered contiguously.

---

## Decisions that shape the work

- **`Watch` is real-time only.** It never reads history. A caller that falls
  behind gets a new delivery of every stream from the current end, and the
  catalog behind it reads what it missed with `Read`, just as `WatchEvents`
  and `ReadEvents` divide the work on the wire. A slow caller must never delay
  delivery to the engine's own catalog or to any other caller.
- **`Local.Run` owns one watcher per stream, shared by every `Watch` call.**
  `Run` discovers streams and runs their watchers for the engine's lifetime,
  as `signal.Bus.Run` does for the bus, and `Watch` only registers a caller
  with them. No `Watch` call manages a goroutine that other calls share.
- **Every delivery begins at the stream's end** — for a stream when it's
  discovered, for a caller when it starts watching, and for a watcher when it
  restarts after a failure. The head start of `SegmentSize` events before the
  end goes. Every caller of `Watch` is a caching catalog, the engine's own or a
  remote engine's, and a catalog already serves a consumer that's behind by
  reading history. Starting at the end also makes a restart that missed
  nothing contiguous, so it no longer discards the receiver's cache.
- **The `Source` contract says that `Watch` may skip events.** It already says
  that a segment that doesn't begin where the stream's previous segment ended
  begins a new delivery, and `feed.Append` restarts the feed for one. It now
  says plainly that a new delivery may begin after the previous segment ended,
  that `Watch` never delivers the events in between, and that a caller that
  needs them reads them with `Read`.
- **Each caller has a buffered channel.** `Run`'s watchers send segments to
  each caller's channel without blocking. When a send would block, `Run`
  removes the caller and closes its channel, under the same mutex that guards
  sends, as the signal bus does with its subscribers. `Watch` delivers what's
  left in the channel, and when it finds the channel closed while its context
  is live, it registers again, starting a new delivery of every stream.
- **Registering takes a snapshot.** Under that mutex, registering records each
  stream's position and adds the caller, so the segments sent to the caller
  afterwards begin at those positions. `Watch` delivers an empty first segment
  for each stream at its recorded position itself, not through the channel, so
  a first segment can't be dropped. For a stream that lost nothing, the empty
  segment begins where the caller already is and changes nothing. For a stream
  that lost segments, it begins after the caller's last end, so the receiver
  starts a new delivery there. Without this, a caller would notice a lost
  segment only when a later one arrived, which never happens for a stream that
  goes quiet, and a lost first segment would hide a stream until its next
  append.
- **On the wire, a gap ends the call with `OUT_OF_RANGE`.** `WatchEvents`
  delivers each stream contiguously within a call. When `Local.Watch` begins a
  new delivery of a stream mid-call, the server ends the call with
  `OUT_OF_RANGE`, in place of today's `UNAVAILABLE`. `source.Remote` starts a
  new `WatchEvents` call at once when it sees that status, so its caller sees
  only new deliveries — no error, no backoff, and no ERROR log.

Calls made in writing this plan, to check while reading it:

- Discovery is supervised separately from the stream watchers, so a failed
  discovery query only delays finding new streams. That fixes runkit#76.
- `Watch` may be called before `Run`, as with the bus, and is served once `Run`
  starts.
- When `Watch` finds its channel closed, it has already delivered what was
  buffered, rather than discarding it, since those segments are still valid
  and in order.
- Each `Watch` call has one goroutine, which calls `fn` for every stream in
  turn. The contract allows `fn` to be called concurrently for different
  streams, but doesn't require it.
- The shared watchers load events of every type. Each caller's segments are
  filtered to its types in memory and keep their begin and end offsets, so a
  segment with no matching events still moves a caller past events of other
  types, as it does today.
- `Local` gains a required `BufferSize`, the number of segments buffered for
  each `Watch` call. The engine passes a new constant of 100.
- The engine runs `Local.Run` only when something watches the local source,
  which is when a projection handles events or the gRPC server is enabled.
- Once the server has delivered a stream on a call, it doesn't send that stream
  a segment that neither advances it nor carries events, such as the empty
  first segment that registering again delivers for a stream that lost
  nothing.
- `source.Remote` starts the new call on `OUT_OF_RANGE` without a delay. A
  correct server returns that status only after events were appended, so the
  client can't spin.

---

## Design

Names are proposals; confirm or rename as each step lands.

| Name | Role |
| --- | --- |
| `Local.Run` | Runs until `ctx` is canceled, and is called at most once. Discovers streams on each `eventstream.create` signal under its own supervisor, and starts a stream watcher for each new stream. Holds the registered callers and each stream's position under one mutex. |
| stream watcher | Unexported, one per stream, each under its own supervisor. Begins at the stream's end, sending `[end, end)` to every caller as the stream's first segment. On each append signal, loads segments of every type from its position and sends each one to every caller without blocking. After a failure it begins again at the end, so its first segment may begin a new delivery. Watches signals with a context scoped to each attempt, so a failed attempt leaves no subscriber behind. |
| caller | Unexported, one per registration of a `Watch` call — its types and a channel of `BufferSize` segments. |
| `Local.Watch` | Registers a caller, delivers the snapshot's first segments, then delivers each segment from the channel, filtered to its types. Registers again when `Run` closes the channel while `ctx` is live. Unregisters and returns `ctx`'s error or `fn`'s error, unchanged. |
| `Local.BufferSize` | Required. The number of segments buffered for each `Watch` call. |
| `consumerapi.Server` | `WatchEvents` ends the call with `OUT_OF_RANGE` in place of `UNAVAILABLE`, delivers each stream from its end, and skips segments that neither advance a delivered stream nor carry events. `ReadEvents` is unchanged. |
| `source.Remote` | `Watch` starts a new `WatchEvents` call at once on `OUT_OF_RANGE`, with fresh contiguity checks. Any other ending is still a failure. |

`Watch`, per caller:

```
loop:
  register: under the mutex, record each stream's position and add the caller
  for each recorded stream: fn([pos, pos))
  for each segment received from the channel: fn(segment filtered to types)
  channel closed: return ctx's error if ctx is done, otherwise loop
fn returns an error → unregister and return it
```

---

## Steps

Every step is green under `make test` unless marked otherwise.

- [ ] **Step 1.** The `Source` contract says that `Watch` may skip events — a
      new delivery may begin after the previous segment ended, `Watch` never
      delivers the events in between, and a caller that needs them reads them
      with `Read`. Docs only, in `source.go`.
- [ ] **Step 2.** Every delivery begins at the stream's end. `loadWatchOffset`
      returns the stream's `next_offset`, so `watchStream`'s first segment is
      `[end, end)`, after a retry as well. The docs of `Local.Watch`,
      `SegmentSize` and `WatchEvents` change to match. The `local_test.go`
      tests of where a stream begins are rewritten, as are the
      `server_test.go` tests that expect `WatchEvents` to begin a stream with
      the events already on it — `TestServer_contemporaryEvents`,
      `TestServer_historicalEvents`, "it begins each existing stream with the
      events already on it" and "it delivers events of every type when no
      types are requested".
- [ ] **Step 3.** `WatchEvents` ends the call with `OUT_OF_RANGE` when it can't
      deliver a stream contiguously, and `source.Remote.Watch` starts a new
      call at once when it sees that status. Server and remote tests cover
      both, including the first segments of the new call beginning new
      deliveries. Before Step 4, a test can make `Local` begin a new delivery
      mid-call only with a fault that fails a stream's watcher, such as a
      stored envelope that can't be decoded, after which the watcher restarts
      at the end. The server test either uses such a fault or moves to Step 4.
- [ ] **Step 4.** `Local.Run` shares one watcher per stream between every
      `Watch` call, with `BufferSize` and the engine's constant, as described
      above. The engine runs `Local.Run` alongside the bus when the catalog
      watches the local source or the gRPC server is enabled. Tests cover two
      `Watch` calls served by one watcher; a caller that falls behind getting
      a new delivery of every stream, including one that went quiet after its
      segment was dropped; a stream created while a caller's channel is full
      still reaching that caller; `fn`'s error ending only its own call;
      `Watch` called before `Run`; a failed discovery query leaving the
      existing watchers running; and a watcher that restarts beginning a new
      delivery at the end. The server test "it delivers every event in order
      to a client that falls behind" now sees the call end with
      `OUT_OF_RANGE`, which makes it the server's test of that path.
- [ ] **Step 5.** CHANGELOG check. Comment on runkit#75 and #76 with what this
      changed, and close #76. `make precommit` green.

---

## Out of scope (explicitly deferred)

- **Sharing work between reads.** Each `Read` call, and so each `ReadEvents`
  call, still queries on its own.
- **Naming `OUT_OF_RANGE` in the protocol.** The `WatchEvents` docs in
  dogmatiq/protobuf say only that the server closes the call, so other servers
  can't be relied on to use the same status until they say more.
- **The other findings from the review of PR 64**, recorded as runkit#73, #74,
  #77 and #78.

<!-- references -->

[PR 64]: https://github.com/dogmatiq/runkit/pull/64
[the event feed plan]: 2026-11-04-event-feed-consumption.md
