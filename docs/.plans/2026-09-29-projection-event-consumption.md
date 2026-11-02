# Projection Event Consumption from Peers — Remaining Work

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events recorded by applications running on _peer_
engines, through the same path they already use for this application's streams.
`eventstream.Catalog` and `eventstream.Reader` are that path; today they are
local-only, and this work makes them span peers. Foreign events are never
persisted locally.

**Done already (on `main`):** local consumption through `Catalog`/`Reader`,
handler-owned checkpoints, `cluster.Node` responsibility by rendezvous hashing,
retry through the supervisor, the gRPC server side (`ConsumerAPI`,
`grpcserver.Server`, `WithListenAddress()`).

---

## Decisions that shape the remaining work

- **`Catalog` and `Reader` are the abstraction.** Today's `Catalog` and
  `Reader` become `LocalCatalog` and `LocalReader`. `RemoteCatalog` and
  `RemoteReader` provide the same two shapes over one peer's
  `messaginggrpc.EventStreamConsumerAPIClient`, paired as a `Peer`. `Catalog`
  and `Reader` become composites: `Catalog.StreamIDs()` is the union over the
  local catalog and the current peers, and `Reader.Subscribe()` asks the
  `Catalog` which member offers the stream and delegates to that member's
  reader. `projection.Consumer` and `streamConsumer` keep their
  `EventStreams *Catalog` and `Events *Reader` fields.
- **Peers come and go.** The composite `Catalog` owns the set of peers. Adding
  a peer returns a function that removes it, in the same style as
  `cluster.Node.ObserveResponsibility()`; the set is owned by the catalog's
  `Run` goroutine and changed through messaging, not a mutex. When a peer is
  removed, its streams leave `StreamIDs()` and the composite `Reader` cancels
  any subscriptions it had routed to that peer. Re-adding a peer makes its
  streams reappear, and consumption resumes from the handler's checkpoint.
  Configuration is static for now (`WithPeer()` at construction, added to the
  catalog at startup); a runtime add/remove API is deferred and must need no
  redesign.
- **Only local streams are permanent.** `LocalCatalog` keeps its guarantee that
  streams are never removed (its change detection relies on it). The composite
  makes no such guarantee: a peer's streams disappear with the peer. Within a
  peer's lifetime its known stream set only grows and is retained across
  reconnects, since `RemoteCatalog` publishes IDs through an `xsync.Observable`
  as `LocalCatalog` does.
- **`projection.Consumer` follows the catalog both ways.** It starts a stream
  consumer for each stream that appears and stops the stream consumer for each
  stream that disappears, so a removed peer's streams stop being consumed. The
  handler keeps its checkpoint for the stream. A stream ID that is not offered
  by any member when subscribed gets a no-op subscription; the consumer stops
  it when it notices the stream is absent.
- **`ConsumerAPI` serves only this application's streams.** It depends on
  `LocalCatalog` and `LocalReader`, never on the composites, so it never relays
  a peer's streams. (Relay topologies are out of scope.)
- **Remote consumption presents the same shape as local.** A `RemoteReader`
  subscription delivers `Batch` values (one per `ConsumeEventsResponse`, its
  `MultiEnvelope`s flattened with `envelopepb.MultiEnvelope.All()`, the
  response's `CheckpointOffset` as `Batch.Checkpoint`). The server already
  applies the type filter and moves slow clients to history, so the remote
  reader needs no cache; gRPC flow control is its backpressure.
- **Remote failures retry through the supervisor.** Each subscription and each
  discovery stream is a supervised function that reconnects from the last
  delivered checkpoint. `NOT_FOUND` and `OUT_OF_RANGE` from a peer indicate
  that the peer's data no longer matches the handler's checkpoint; they are
  returned via `supervisor.Persistent` (retried slowly), not treated as fatal,
  since one bad peer must not stop the engine. A server-closed stream (no
  requested type recordable by the peer) ends that subscription quietly.
- **Peers are configured statically, for now.** `WithPeer(target string,
  options ...grpc.DialOption)` mirrors `WithListenAddress()`. Homogeneous
  connectivity; no self-peering.
- **Processes are unchanged.** They keep `AcquireEventDelivery`, SKIP LOCKED and
  engine-owned checkpoints in `eventstream.handler_checkpoints`.

---

## Phase A — Catalog and Reader across peers

### Task A.0: Tidy-ups left from the local work

- [x] `internal/schema/ddl/00-eventstream/00-tables.sql`: the comment on
      `handler_checkpoints` still names projection handlers; it is now used by
      processes only. Commit.

### Task A.1: Local types

**Files:** in `internal/eventstream/`, `git mv catalog.go localcatalog.go`,
`reader.go localreader.go` (and their `_test.go` files likewise).

- [x] **Step 1:** Rename `Catalog` → `LocalCatalog` and `Reader` →
      `LocalReader` (types, tests, engine wiring, `ConsumerAPI` and
      `projection.Consumer` fields), with the files moved as above so the local
      implementations live in `localcatalog.go` and `localreader.go`. No
      behaviour changes; the whole suite must pass unchanged.
- [x] **Step 2:** Commit.

### Task A.2: Composites

**Files:** create `internal/eventstream/catalog.go`, `reader.go` + tests;
modify `internal/projection/consumer.go`.

- [ ] **Step 1: Composite `Catalog`.** Holds a `*LocalCatalog` and a set of
      `*Peer` owned by its `Run` goroutine. `AddPeer(*Peer) (remove func())`
      sends to `Run` through channels (blocks until `Run` is running, no-op
      once it has stopped, as `Node.ObserveResponsibility` does). `Run` watches
      the local catalog's and every peer's `changed` channels and the add/remove
      requests, and republishes the union through an `xsync.Observable`,
      storing only when it differs. It also answers which member offers a given
      stream, for the composite `Reader`. `Peer` is introduced here as the
      pairing of a catalog and a reader; A.3 gives it its remote
      implementation.
- [ ] **Step 2: Composite `Reader`.** Holds `Local *LocalReader` and
      `EventStreams *Catalog`. `Subscribe()` asks the catalog for the member
      that offers the stream and delegates; a stream no member offers gets a
      no-op unsubscribe. A subscription routed to a peer is cancelled when that
      peer is removed. A stream offered by more than one member is not expected
      (stream IDs are UUIDs); the first member listed wins.
- [ ] **Step 3: `projection.Consumer` stops as well as starts.** Each stream
      consumer gets its own cancellable context (`supervisor.Go` or
      `context.WithCancel` per stream) so that a stream missing from
      `StreamIDs()` has its consumer cancelled and removed from
      `streamConsumers`. Update the comment that says a stream consumer lives
      for the life of the consumer.
- [ ] **Step 4:** Point `projection.Consumer` at the composites; `ConsumerAPI`
      stays on the local types. With no peers the composites must be a
      pass-through; the whole existing suite must pass unchanged.
- [ ] **Step 5: Tests + commit.** Composite tests use a stub peer (a `Peer`
      whose catalog/reader are driven by the test): union of stream IDs; change
      notification when a member changes; adding a peer adds its streams;
      removing one removes them and ends its subscriptions; `Consumer` stops a
      stream consumer when its stream leaves the catalog.

### Task A.3: Remote catalog, reader and peer

**Files:** create `internal/eventstream/remotecatalog.go`, `remotereader.go`
(and extend `peer.go`) + tests against a real `ConsumerAPI` served by an
engine, as `grpc_test.go` already does (`newConsumerClient`).

- [ ] **Step 1: `RemoteCatalog`.** `Run()` holds `ListEventStreams` open under
      `supervisor.Run`, adding each `event_stream_id` to an observable set.
- [ ] **Step 2: `RemoteReader`.** `Subscribe()` starts a supervised function:
      `ConsumeEvents(streamID, checkpoint, types)`, each response → `Batch`
      (set `StreamID`; flatten `MultiEnvelope`s; validate each envelope carries
      `envelopepb.EventStreamPosition`), blocking send on `ch`, then advance the
      checkpoint it will reconnect from. Unsubscribe cancels it.
- [ ] **Step 3: Honor the server's contract** as decided above: EOF ends the
      subscription; `NOT_FOUND`/`OUT_OF_RANGE` → `supervisor.Persistent`.
- [ ] **Step 4: Remote `Peer`.** One `RemoteCatalog` and `RemoteReader` over
      one `grpc.ClientConn`; `Run()` runs both.
- [ ] **Step 5:** Test + commit.

### Task A.4: Peer configuration

**Files:** modify `engineoption.go`, `engine.go`, `CHANGELOG.md`.

- [ ] **Step 1:** Add `WithPeer(target string, options ...grpc.DialOption)`.
      In `Run()`, one `grpc.ClientConn` and one `Peer` per option, each run as
      a component and added to the composite `Catalog` at startup (remove on
      shutdown is implicit in stopping).
- [ ] **Step 2:** Changelog: add the peer-consumption entry under Added, and
      drop "This engine does not yet consume events from other engines." from
      the gRPC server entry. Commit.

---

## Phase B — End-to-end and hardening

### Task B.0: Test harness

`xtesting.RunEngines*` runs a fixed three replicas of one application (fixed
`appKey`) on one database, exposes only the first engine, and cannot start or
stop engines during a test. Phase B needs:

- [ ] A way to run a second application, with its own identity and database,
      peered to the first (the upstream must be running and have a
      `ListenAddr()` before the downstream is built).
- [ ] A way to add and remove individual engines while a test runs, and to
      reach each engine's `*cluster.Node` (the `contexthook.Node` hook already
      exposes it).

### Task B.1: Foreign events

- [ ] `internal/projection/foreignevents_test.go`: upstream app records an
      event via a command; the downstream app's projection handles it.
      Historical (recorded before the downstream starts) and contemporary.
- [ ] Debug systematically if red: dial → `RemoteCatalog` discovery → node
      responsibility → `RemoteReader` → type filter → delivery. Commit.

### Task B.2: Distribution, failover, catch-up

- [ ] Three replicas over one database: after the cluster converges, every
      event is handled and `HandleEvent` is called exactly once per event
      (duplicates are allowed only while membership changes).
- [ ] Add a replica: streams move, nothing dropped or duplicated after
      convergence. Remove one: its streams are consumed elsewhere within about
      the heartbeat timeout.
- [ ] A slow gRPC client of `ConsumerAPI` is returned to history and still
      receives every event in order (the local equivalent is already covered in
      `reader_test.go`).

### Task B.3: Final validation

- [ ] `make precommit` green. Commit.

---

## Out of scope (explicitly deferred)

- **A runtime API for adding and removing peers.** The composites support it;
  no engine-level API is exposed yet.
- **Process handlers** consumed by a node chosen by rendezvous hashing.
- **Re-subscribing after a peer closes a stream** because it could not record
  any requested type; if the peer later adds that type, the subscription is
  only re-established when this engine restarts or the peer is re-added.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous peer connectivity**, dynamic peer discovery, per-replica
  reachability.
- **Relay topologies**; consumers peer directly with producers.
- **Instant failover** for local projections, given up for a single model.
