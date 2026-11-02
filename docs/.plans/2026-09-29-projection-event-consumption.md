# Projection Event Consumption from Foreign Applications — Remaining Work

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events recorded by _foreign_ applications, running
on other engines, through the same path they already use for this
application's streams. `eventstream.Catalog` is that path; this work makes it
span foreign applications. Foreign events are never
persisted locally.

"Peer" is reserved for another node in the same cluster. An application that
this engine consumes events from is a "foreign application".

**Done already (on `main`):** local consumption through `LocalCatalog`,
handler-owned checkpoints, `cluster.Node` responsibility by rendezvous hashing,
retry through the supervisor, the gRPC server side (`ConsumerAPI`,
`grpcserver.Server`, `WithListenAddress()`).

---

## Decisions that shape the remaining work

- **`Catalog` is the abstraction, for discovery and reading alike.** The
  `Catalog` interface has `StreamIDs()` and `Subscribe()`. `LocalCatalog`
  implements it for this application's streams, discovering them and reading
  their events in one type over the existing `scanner` package. `RemoteCatalog`
  implements it over one foreign application's
  `messaginggrpc.EventStreamConsumerAPIClient`. `CompositeCatalog` implements
  it over a set of catalogs added and removed at runtime: `StreamIDs()` is the
  union, and `Subscribe()` is routed to the member that offers the stream.
  There is no separate reader abstraction. `projection.Consumer` and
  `streamConsumer` depend on the interface through one `EventStreams` field.
- **Catalogs come and go.** `CompositeCatalog.Add(Catalog)` returns a
  function that removes the member, in the same style as
  `cluster.Node.ObserveResponsibility()`; the set is owned by the composite's
  `Run` goroutine and changed through messaging, not a mutex. The local
  catalog is added first, then one `RemoteCatalog` per foreign application.
  A subscription can be lost: `Subscribe()` returns a `lost` channel that the
  catalog closes when it can no longer deliver. `LocalCatalog` closes it when
  it stops; `RemoteCatalog` when it stops or the server closes the gRPC
  stream; the composite passes the offering member's channel through, or an
  already-closed one if no member offers the stream. A catalog removed from
  the composite is stopped by whatever removed it, which is what loses its
  subscriptions. A subscriber treats a lost subscription as an error, so its
  supervisor restarts it from its own checkpoint, which routes it to whichever
  member now offers the stream. No subscriber is ever left bound to a member
  that is gone, even if the member is removed and re-added between two reads
  of `StreamIDs()`.
  Configuration is static for now (`WithForeignApplication()` at
  construction, added to the composite at startup); a runtime add/remove API
  is deferred and must need no redesign.
- **Only local streams are permanent.** `LocalCatalog` keeps its guarantee that
  streams are never removed (its change detection relies on it). The composite
  makes no such guarantee: a member's streams disappear with the member. While
  a `RemoteCatalog` is present, its known stream set only grows and is retained
  across reconnects, since it publishes IDs through an `xsync.Observable` as
  the local catalog does.
- **`projection.Consumer` follows the catalog both ways.** It starts a stream
  consumer for each stream that appears and stops the stream consumer for each
  stream that disappears, so a removed member's streams stop being consumed.
  The handler keeps its checkpoint for the stream. A stream ID that no member
  offers when subscribed gets a no-op subscription; the consumer stops it when
  it notices the stream is absent.
- **`ConsumerAPI` serves only this application's streams.** It depends on
  `*LocalCatalog`, never on the composite, so it never relays a foreign
  application's streams. (Relay topologies are out of scope.)
- **Remote consumption presents the same shape as local.** A `RemoteCatalog`
  subscription delivers `Batch` values (one per `ConsumeEventsResponse`, its
  `MultiEnvelope`s flattened with `envelopepb.MultiEnvelope.All()`, the
  response's `CheckpointOffset` as `Batch.Checkpoint`). The server already
  applies the type filter and moves slow clients to history, so the remote
  catalog needs no cache; gRPC flow control is its backpressure.
- **Remote failures retry through the supervisor.** Each subscription and each
  discovery stream is a supervised function that reconnects from the last
  delivered checkpoint. `NOT_FOUND` and `OUT_OF_RANGE` from a foreign
  application indicate that its data no longer matches the handler's
  checkpoint; they are returned via `supervisor.Persistent` (retried slowly),
  not treated as fatal, since one bad foreign application must not stop the
  engine. A server-closed stream (no requested type recordable by the
  application) reports the subscription lost.
- **Foreign applications are configured statically, for now.**
  `WithForeignApplication(target string, options ...grpc.DialOption)` mirrors
  `WithListenAddress()`. Homogeneous connectivity; an engine never consumes
  from itself.
- **Processes are unchanged.** They keep `AcquireEventDelivery`, SKIP LOCKED and
  engine-owned checkpoints in `eventstream.handler_checkpoints`.

---

## Phase A — Catalogs across foreign applications

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

### Task A.2: One `Catalog` abstraction

**Files:** `internal/eventstream/catalog.go` (interface and `Batch`),
`localcatalog.go` (the former catalog and reader, folded together), `grpc.go`,
`engine.go`, `internal/projection/consumer.go`, `streamconsumer.go`. The
`scanner` package is unchanged.

- [x] **Step 1:** Define the `Catalog` interface. Fold `LocalReader` into
      `LocalCatalog`, whose `Run` discovers streams and reads events. Drop the
      `Events` fields from `ConsumerAPI`, `projection.Consumer` and
      `streamConsumer`; the projection types depend on the interface. No
      behaviour changes; the whole suite must pass unchanged.
- [x] **Step 2:** Commit.

### Task A.3: `CompositeCatalog`

**Files:** create `internal/eventstream/compositecatalog.go` + test; modify
`internal/projection/consumer.go`.

- [x] **Step 1: `CompositeCatalog`.** `Add(Catalog) (remove func())` sends to
      `Run` through channels (blocks until `Run` is running, no-op once it has
      stopped, as `Node.ObserveResponsibility` does). `Run` watches every
      member's `changed` channel and the add/remove requests, and republishes
      the union through an `xsync.Observable`, storing only when it differs.
      A stream offered by more than one member is a misconfiguration (the same
      application listed twice, or this engine pointed at its own cluster):
      `Run` returns an error naming the stream, which stops the engine.
- [x] **Step 2: Routing.** `Subscribe()` delegates to the member that offers
      the stream, read from a map that `publish` keeps alongside the stream
      set, passing the member's `lost` channel through. A stream no member
      offers is lost immediately. `streamConsumer` and `ConsumerAPI` treat a
      lost subscription as an error.
- [x] **Step 3: `projection.Consumer` stops as well as starts.** Each stream
      consumer gets its own cancellable context (`supervisor.Go` or
      `context.WithCancel` per stream) so that a stream missing from
      `StreamIDs()` has its consumer cancelled and removed from
      `streamConsumers`. Update the comment that says a stream consumer lives
      for the life of the consumer. Without this, a stream consumer for a
      vanished stream would keep restarting with backoff, its subscription
      lost each time.
- [x] **Step 4:** The engine builds a `CompositeCatalog`, adds the local
      catalog to it, and gives it to each `projection.Consumer`; `ConsumerAPI`
      stays on `*LocalCatalog`. With one member the composite must be a
      pass-through; the whole existing suite must pass unchanged.
- [x] **Step 5: Tests + commit.** With a stub `Catalog`: union of stream IDs;
      change notification when a member changes; adding a member adds its
      streams; removing one removes them and ends its subscriptions;
      subscriptions are routed to the right member; `Consumer` stops a stream
      consumer when its stream leaves the catalog.

### Task A.4: `RemoteCatalog`

**Files:** create `internal/eventstream/remotecatalog.go` + tests against a
real `ConsumerAPI` served by an engine, as `grpc_test.go` already does
(`newConsumerClient`).

- [ ] **Step 1: Discovery.** `Run()` holds `ListEventStreams` open under
      `supervisor.Run`, adding each `event_stream_id` to an observable set.
- [ ] **Step 2: Reading.** `Subscribe()` starts a supervised function:
      `ConsumeEvents(streamID, checkpoint, types)`, each response → `Batch`
      (set `StreamID`; flatten `MultiEnvelope`s; validate each envelope carries
      `envelopepb.EventStreamPosition`), blocking send on `ch`, then advance the
      checkpoint it will reconnect from. Unsubscribe cancels it.
- [ ] **Step 3: Honor the server's contract** as decided above: EOF closes the
      subscription's `lost` channel; `NOT_FOUND`/`OUT_OF_RANGE` →
      `supervisor.Persistent`.
- [ ] **Step 4:** Test + commit.

### Task A.5: Foreign application configuration

**Files:** modify `engineoption.go`, `engine.go`, `CHANGELOG.md`.

- [ ] **Step 1:** Add `WithForeignApplication(target string, options
      ...grpc.DialOption)`. In `Run()`, one `grpc.ClientConn` and one
      `RemoteCatalog` per option, run as a component and added to the
      `CompositeCatalog` at startup.
- [ ] **Step 2:** Changelog: add an entry for consuming events from foreign
      applications under Added, and drop "This engine does not yet consume
      events from other engines." from the gRPC server entry. Commit.

---

## Phase B — End-to-end and hardening

### Task B.0: Test harness

`xtesting.RunEngines*` runs a fixed three replicas of one application (fixed
`appKey`) on one database, exposes only the first engine, and cannot start or
stop engines during a test. Phase B needs:

- [ ] A way to run a second application, with its own identity and database,
      consuming from the first (the upstream must be running and have a
      `ListenAddr()` before the downstream is built).
- [ ] A way to add and remove individual engines while a test runs, and to
      reach each engine's `*cluster.Node` (the `contexthook.Node` hook already
      exposes it).

### Task B.1: Foreign events

- [ ] `internal/projection/foreignevents_test.go`: upstream app records an
      event via a command; the downstream app's projection handles it.
      Historical (recorded before the downstream starts) and contemporary.
- [ ] Debug systematically if red: dial → `RemoteCatalog` discovery → node
      responsibility → subscription → type filter → delivery. Commit.

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

- **A runtime API for adding and removing foreign applications.** The
  composite supports it; no engine-level API is exposed yet.
- **Process handlers** consumed by a node chosen by rendezvous hashing.
- **Re-subscribing after a foreign application closes a stream** because it
  could not record any requested type; if it later adds that type, the
  subscription is only re-established when this engine restarts or the
  application is re-added.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous connectivity**, dynamic discovery of foreign applications,
  per-replica reachability.
- **Relay topologies**; this engine consumes directly from the application
  that recorded the events.
- **Instant failover** for local projections, given up for a single model.
