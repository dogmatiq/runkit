# Projection Event Consumption from Foreign Applications — Remaining Work

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events recorded by _foreign_ applications, running
on other engines, through the same path they already use for this
application's streams. `eventstream.Catalog` is that path; this work makes it
span foreign applications. Foreign events are never persisted locally.

"Peer" is reserved for another node in the same cluster. An application that
this engine consumes events from is a "foreign application", and the engine
it is consumed from is a "remote engine".

**Done already (on `main`):** local consumption through `LocalCatalog`,
handler-owned checkpoints, `cluster.Node` responsibility by rendezvous hashing,
retry through the supervisor, the gRPC server side (`ConsumerAPI`,
`grpcserver.Server`, `WithListenAddress()`).

> **Superseded from Task A.6.** The catalog model described below — the
> `Catalog` interface and its `Subscribe` subscriptions, `CompositeCatalog`,
> `RemoteCatalog`, lost subscriptions and the recheck interval — is replaced by
> [the event feed plan], which rebuilds consumption on `WatchEvents` and
> `ReadEvents`. Tasks A.0 to A.5 and Phase B are an accurate record of what
> this branch delivered before that; the decisions and Task A.6 are kept as
> history only.

---

## Decisions that shape the remaining work

- **One `Catalog` interface, whose subscriptions may end.** `Catalog` has
  `StreamIDs()`, an `xsync.ObservableR` of the stream IDs it currently offers,
  and `Subscribe()`, which returns `unsubscribe` and an
  `xsync.FutureR[error]`, treated like any error result: resolved if the
  subscription ends, nil if it succeeded. A subscription delivers batches in
  order until it ends or is unsubscribed, and one that has ended is not
  resumed. It succeeds when it has delivered every event it can: the
  application that records the stream, as currently deployed, will not append
  events of the requested types to it, though a later deployment may. The
  stream must be in the current set. A subscription to a stream that is not
  offered, or that stops being offered, ends with `ErrStreamNotFound`, and
  only after the set has been published without the stream. The name matches
  the `NOT_FOUND` that `ConsumerAPI` answers with.
  `LocalCatalog` implements the interface for this application's streams,
  discovering them and reading their events in one type over the existing
  `scanner` package; its subscriptions never end otherwise. `RemoteCatalog`
  implements it over one foreign application's
  `messaginggrpc.EventStreamConsumerAPIClient`; each subscription is one
  `ConsumeEvents` call, which ends when the call does. `CompositeCatalog`
  implements it over a set of members added and removed at runtime; its
  subscriptions end only with `ErrStreamNotFound`, since it starts a new
  member subscription whenever one ends any other way. `projection.Consumer`
  and `streamConsumer` depend on the interface through one `EventStreams`
  field, and `ConsumerAPI` on `*LocalCatalog` directly. Members know nothing
  about retrying, and the composite knows nothing about gRPC. The receive-only
  `xsync` views are from enginekit v0.30.0. There is no separate reader
  abstraction.
- **Catalogs come and go, and the composite keeps subscriptions alive across
  them.** `CompositeCatalog.Add(Catalog)` returns a function that removes the
  member, in the same style as `cluster.Node.ObserveResponsibility()`. The
  members, the route for each stream, and the subscriptions are owned by the
  composite's `Run` goroutine and changed through messaging, not a mutex or
  atomics. The local catalog is added first, then one `RemoteCatalog` per
  available remote engine.
  Each subscription has a goroutine that forwards batches from an attempt on
  one member to the subscriber's channel, recording each batch's checkpoint
  after forwarding it. When the attempt ends, or the member is removed or
  stops offering the stream, the goroutine starts the next attempt from the
  recorded checkpoint on the next member that offers the stream, in rotation.
  A member that advertises a stream but can not deliver it — an instance that
  died behind a per-instance discoverer, or a load balancer routed to the
  wrong application — therefore does not hold the subscription while another
  member can serve it. A nil ending means the member delivered everything it
  can for now; the next attempt waits the backstop interval described below.
  A non-nil ending is a failure, retried through `supervisor.Run` with its
  backoff, honoring `supervisor.Persistent`. If no member offers the stream,
  the composite publishes its set without it and then ends the subscription
  with `ErrStreamNotFound`. The subscriber sees none of the rest: it
  subscribes once, from its own checkpoint, and reads batches until it
  unsubscribes or the stream goes away.
  Resuming from the last forwarded batch is correct here even though the
  stream consumer resumes from the handler's checkpoint (dogmatiq/dogma#221).
  That rule covers the handler failing, where its checkpoint may be behind;
  moving a subscription is continuity for a subscriber that has not failed.
  Batches the old member delivered that were not forwarded are dropped and
  delivered again by the next attempt.
- **A stream is offered while a connected server advertises it.**
  `LocalCatalog` never removes a stream; its change detection relies on that.
  `RemoteCatalog` offers the streams listed by its current `ListEventStreams`
  call: when the call ends, for any reason, it stops offering all of them, and
  offers each again as the re-issued call lists it. A remote engine that
  restarts therefore revokes and re-advertises its streams, which ends every
  subscription to them and brings the subscribers back against the new
  deployment. That is how a subscription that succeeded is reopened without a
  signal from the server (dogmatiq/protobuf#8): recordable types come from the
  application's configuration, so they change only with a deployment, and a
  server-streaming call can not outlive the process serving it, even behind a
  proxy. A network blip does the same at the cost of restarting the
  subscribers, whose `ConsumeEvents` calls broke on the same blip. A stream
  offered by two members stays offered while either offers it, and its
  subscriptions move rather than end.
- **`projection.Consumer` follows the catalog both ways.** It starts a stream
  consumer for each stream that appears and stops the stream consumer for each
  stream that disappears, so a removed member's streams stop being consumed.
  The handler keeps its checkpoint for the stream. A stream consumer whose
  subscription ends with `ErrStreamNotFound` waits until the catalog offers
  the stream again, then subscribes again from the handler's checkpoint,
  unless the consumer stops it first because the stream stayed away. The
  stream may already be back by the time the stream consumer sees the ending;
  it then subscribes again at once, which is correct, since a member now
  offers it. It can not spin on a stale set, because the set is updated before
  the subscription ends.
- **`ConsumerAPI` serves only this application's streams.** It depends on
  `*LocalCatalog`, never on the composite, so it never relays a foreign
  application's streams. (Relay topologies are out of scope.)
- **Remote consumption presents the same shape as local.** A `RemoteCatalog`
  subscription delivers `Batch` values (one per `ConsumeEventsResponse`, its
  `MultiEnvelope`s flattened with `envelopepb.MultiEnvelope.All()`, the
  response's `CheckpointOffset` as `Batch.Checkpoint`). The server already
  applies the type filter and moves slow clients to history, so the remote
  catalog needs no cache; gRPC flow control is its backpressure.
- **Remote failures are reported; the composite retries.** A `RemoteCatalog`
  subscription is one `ConsumeEvents` call. Each response becomes a `Batch`
  sent on the subscriber's channel, and the attempt ends when the call does.
  EOF ends it with nil: the application closed the stream because it can not
  record any of the requested types. `NOT_FOUND` and `OUT_OF_RANGE` mean the
  application's data no longer matches the checkpoint; they end the attempt
  with `supervisor.Persistent`, so the composite retries slowly and never
  fatally, since one bad foreign application must not stop the engine. Any
  other error ends it as a transient failure. Discovery stays supervised
  inside `RemoteCatalog`: `ListEventStreams` is held open under
  `supervisor.Run`, and an engine that does not implement the service is
  asked again after a recheck interval of three minutes.
  A subscription that succeeded is reopened when the remote engine's streams
  are re-advertised, as above. As a backstop, the composite also reopens it
  after a long interval, in the order of tens of minutes, which covers two
  cases the reconnect does not. A server that is not runkit may change what it
  can record without restarting. And a rolling deploy that removes a type has
  the new replicas close the stream while the old ones still append events of
  that type; the client is then connected to a replica that will not restart,
  and the backstop poll delivers the straggling events up to the head before
  the stream closes again. In both cases events are delayed, never lost. The
  engine passes the backstop interval to the composite.
- **Remote engines are discovered, even when the set is static.** Foreign
  applications' events are consumed from _remote engines_. The engine is
  given any number of discoverers, one per use of
  `WithRemoteEngineDiscoverer()`. A `RemoteEngineDiscoverer` is a function,
  `func(ctx, add func(conn) (remove func())) error`; it calls `add` as each
  remote engine becomes available, and calls the returned `remove` function
  when it becomes unavailable. A discoverer with configuration is a
  type whose method is passed as the function. A fixed set is declared with the
  `StaticRemoteEngines(conns ...grpc.ClientConnInterface)` helper, which
  returns a discoverer that reports each connection once:
  `WithRemoteEngineDiscoverer(StaticRemoteEngines(orders, billing))`. There
  is no option that configures a remote engine directly.
- **A remote engine is the logical engine, not an instance.** As with
  `runkit.Engine`, an engine is every instance sharing its storage; each
  instance is a node of its cluster, and an engine that does not cluster has
  one instance. A connection reaches any of a remote engine's instances
  (typically one gRPC target covering them all), so a discoverer reports one
  connection per engine, not per instance. "Remote engine" makes no claim
  about how many applications the engine hosts, or which APIs it offers
  beyond `EventStreamConsumerAPI` today. `messaginggrpc` does not tie a
  server's streams to an application (only each envelope's
  `Source.Application` does); identifying the application per stream is
  proposed in dogmatiq/protobuf#6.
- **A discovered remote engine is a gRPC connection.** The discoverer reports
  a `grpc.ClientConnInterface`; there is no value type describing a remote
  engine. The discoverer creates and owns the connection: users control
  everything about the client (credentials, resolvers, interceptors), client
  errors surface where they create it, and tests can pass in-process
  connections. The engine uses a connection until its `remove` function
  returns, and never closes it. Each addition is identified by its `remove`
  function, so connections are never compared and need not be comparable. To
  change a connection (for example, new credentials) a discoverer removes the
  old one, then adds the new one. `add` must not be called after the
  discoverer returns, including from goroutines it started.
- **Each added connection has its own catalog.** When a connection is added
  the engine creates a `RemoteCatalog` over it, runs it, and adds it to the
  composite. When it is removed the engine removes the catalog from the
  composite and stops it; the composite moves its subscriptions to other
  members that offer the streams, or ends them. A discoverer
  runs under the supervisor for the life of the engine; when it returns, every
  connection it added is removed. Two connections
  to the same remote engine (including one per instance) are two members
  offering the same streams, as is a connection to this engine; each such
  stream is served by one of them. Homogeneous connectivity.
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
      A stream offered by more than one member (the same application listed
      twice, or this engine pointed at its own cluster) is served by one of
      them.
- [x] **Step 2: Routing.** `Subscribe()` delegates to the member that offers
      the stream, read from a map that `publish` keeps alongside the stream
      set, passing the member's `lost` channel through. A stream no member
      offers is lost immediately. `streamConsumer` subscribes again once the
      stream is offered; `ConsumerAPI` ends the gRPC stream as unavailable.
- [x] **Step 3: `projection.Consumer` stops as well as starts.** Each stream
      consumer gets its own cancellable context (`supervisor.Go` or
      `context.WithCancel` per stream) so that a stream missing from
      `StreamIDs()` has its consumer cancelled and forgotten. Update the
      comment that says a stream consumer lives for the life of the
      consumer. Without this, a stream consumer for a
      vanished stream would wait for it to be offered again for as long as the
      consumer runs.
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

- [x] **Step 1: Discovery.** `Run()` holds `ListEventStreams` open under
      `supervisor.Run`, adding each `event_stream_id` to an observable set. A
      remote engine that does not implement the service (`UNIMPLEMENTED`) is
      logged once and offers no streams; it is asked again at the same
      interval as a server-closed stream.
- [x] **Step 2: Reading.** `Subscribe()` starts a supervised function:
      `ConsumeEvents(streamID, checkpoint, types)`, each response → `Batch`
      (set `StreamID`; flatten `MultiEnvelope`s; validate each envelope carries
      `envelopepb.EventStreamPosition`), blocking send on `ch`, then advance the
      checkpoint it will reconnect from. Unsubscribe cancels it.
- [x] **Step 3: Honor the server's contract** as decided above: EOF leaves the
      subscription in place, and it is requested again after an interval;
      `NOT_FOUND`/`OUT_OF_RANGE` → `supervisor.Persistent`.
- [x] **Step 4:** Test + commit.

### Task A.5: Remote engine discovery

**Files:** create `remoteengine.go` (public types) and
`internal/remoteengine/connector.go`; modify `engineoption.go`, `engine.go`,
`CHANGELOG.md`.

- [x] **Step 1: Public API.** The `RemoteEngineDiscoverer` function type,
      `StaticRemoteEngines(conns ...grpc.ClientConnInterface)
      RemoteEngineDiscoverer`, and `WithRemoteEngineDiscoverer()`, which may
      be given more than once, adding a discoverer each time. Docs cover what
      the connection must serve, one connection per engine (not per
      instance), ownership (the discoverer closes it, after it is
      removed), removal through the returned function, and what stops the
      engine.
- [x] **Step 2: Running discoverers.** `remoteengine.Connector`, run as a
      component, one per discoverer, takes the composite as its
      `EventStreams` dependency; the event stream subsystem never sees a
      discoverer. It runs the discoverer under `supervisor.Run`; for each
      added connection it adds a `RemoteCatalog` to the composite and runs it;
      `remove` removes the catalog and stops it, returning once it has stopped
      using the connection. When the discoverer returns, its connections are
      removed; a discoverer that returns before its context is canceled, even
      without an error, is logged and called again after a delay. Other
      subsystems that use remote engines later become further dependencies of
      this type.
- [x] **Step 3: Tests + commit.** Against a real `ConsumerAPI`: an added
      connection's streams appear in the composite; removing it removes them
      and loses their subscriptions; a returning discoverer's connections are
      removed; `StaticRemoteEngines` adds each connection; two discoverers run
      side by side.
- [x] **Step 4:** Changelog: add an entry for consuming events from remote
      engines under Added, and drop "This engine does not yet consume events
      from other engines." from the gRPC server entry. Commit.

### Task A.6: The composite owns retry and rotation

Not done. It was started and then abandoned in favor of
[the event feed plan], which removes the composite, the remote catalog and the
`Subscribe` subscription model altogether. The unfinished work is kept on the
`remote-engines-a6-wip` branch for reference.

---

## Phase B — End-to-end and hardening

### Task B.0: Test harness

`xtesting.RunEngines*` runs a fixed three replicas of one application (fixed
`appKey`) on one database, exposes only the first engine, and cannot start or
stop engines during a test. Phase B needs:

- [x] A way to run a second application, with its own identity and database,
      consuming from the first. `xtesting.StartCluster(t, db, app, n,
      options...)` (in `xtesting/cluster.go`) runs `app` on `n` nodes and
      waits for every node to be ready and to see the others, returning a
      `*Cluster` (one logical engine) with `Nodes()`, `Stop()` and
      `Failed()`; `RunEnginesWithDB` is built on it. A `Cluster` is used only
      from the test's goroutine, so it needs no locking.
      `xtesting.Dial(t, engines...)` returns one connection to the given
      engines (a manual resolver over their listen addresses, `pick_first`),
      using the first and failing over to the others in the order given, for
      `StaticRemoteEngines`.
- [x] A way to add and remove individual nodes while a test runs:
      `Cluster.AddNode(t)` and `Cluster.StopNode(e)`, each node running with
      its own context. `Cluster` implements `dogma.CommandExecutor`,
      executing each command on a running node chosen at random, and the
      `xtesting.ExecuteCommand*` helpers accept any executor.

### Task B.1: Foreign events

- [x] `internal/projection/foreignevents_test.go`: upstream app records an
      event via a command; the downstream app's projection handles it.
      Historical (recorded before the downstream starts) and contemporary.
- [x] Debug systematically if red: dial → `RemoteCatalog` discovery → node
      responsibility → subscription → type filter → delivery. Commit.

### Task B.2: Distribution, failover, catch-up

- [x] Three nodes over one database: after the cluster converges, every
      event is handled and `HandleEvent` is called exactly once per event
      (`internal/projection/distribution_test.go`).
- [x] Add a node: streams move, nothing dropped or duplicated. Remove one:
      its streams are consumed elsewhere once it misses its heartbeats
      (about the heartbeat timeout plus one interval; the test waits for
      it, as heartbeat timing is not configurable).
- [x] A slow gRPC client of `ConsumerAPI` is returned to history and still
      receives every event in order (`grpc_test.go`).
- [x] Remote failover: a downstream projection keeps consuming when the
      upstream node it is connected to stops; the test dials the two upstream
      nodes in order and stops the first (`foreignevents_test.go`).
- [x] A projection consumes local and foreign streams at once, and a
      projection that handles none of a remote engine's event types does not
      retry against it (`foreignevents_test.go`).

### Task B.3: Final validation

- [x] `RunEngines*` handed only the first engine to tests, so accepting
      commands and serving `ConsumerAPI` on the other nodes was never
      exercised. Resolved for commands: `fn` receives the `*xtesting.Cluster`,
      which executes each command on a node chosen at random. gRPC tests
      dial the first node; every node runs the same `ConsumerAPI` over the
      same database.
- [x] `make precommit` green. Commit.

---

## Out of scope (explicitly deferred)

- **Discoverers other than `StaticRemoteEngines`** (DNS, Kubernetes,
  registries). The interface allows them; runkit provides none yet.
- **Process handlers** consumed by a node chosen by rendezvous hashing.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous connectivity**, per-replica reachability.
- **Relay topologies**; this engine consumes directly from the application
  that recorded the events.
- **Instant failover** for local projections, given up for a single model.
- **Closing a stream at head only when no live replica can record the
  requested types**, the union rule proposed in dogmatiq/protobuf#8. Without
  it, a rolling deploy that removes a type delays that type's straggling
  events by up to the backstop interval. With it, `ConsumerAPI` would depend
  on the cluster and on a table of each node's recordable types, since a
  heartbeat carries only the node's ID. Not worth that coupling for a bounded
  delay in a rare case.

<!-- references -->

[the event feed plan]: 2026-11-04-event-feed-consumption.md
