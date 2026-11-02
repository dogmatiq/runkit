# Unified Projection Event Consumption — Remaining Work

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events from **all** streams — those recorded by
_this_ application and those recorded by applications on _peer_ engines —
through a **single** path: rendezvous hashing to choose which node consumes
each stream, a stream source abstraction (local + remote gRPC), handler-owned
checkpoints, and a per-(handler, stream) catch-up subscription. Foreign events
are never persisted locally.

**Tech Stack:** Go, PostgreSQL (`database/sql` + pgx stdlib), gRPC
(`messaginggrpc`), `enginekit` stubs, testcontainers.

---

## Decisions

- **One projection consumption path.** Local and foreign streams are consumed
  identically; only the source differs.
- **Local streams are consumed directly.** `projection.Consumer` and its
  stream consumers use `*eventstream.Reader` and `*eventstream.Catalog`
  directly. The `StreamSource` interface is extracted when the remote source is
  written, so that a real second implementation shapes it.
- **Handler-owned checkpoints.** Projections resume from
  `handler.CheckpointOffset(streamID)` and report progress via `HandleEvent`'s
  returned offset (OCC). `handler_checkpoints` becomes process-only.
- **The scope's checkpoint is the handler's, not the scan position.**
  `ProjectionEventScope.CheckpointOffset()` is the value `HandleEvent` last
  returned, or `CheckpointOffset(streamID)` when the stream consumer starts.
  Dogma says it "may be lower than the incoming event's offset" when the
  stream holds types the handler does not consume, so `Batch.Checkpoint`,
  which advances past such events, is not used for it. The stream consumer
  uses `Batch.Checkpoint` for nothing.
- **Retry through the supervisor.** An error from `HandleEvent` fails the
  stream consumer, which its supervisor restarts with backoff from
  `CheckpointOffset(streamID)`. The `failures` and `resume_at` columns of
  `handler_checkpoints` are not used for projections.
- **No gRPC loopback.** The engine consumes its own streams via `Catalog` and
  `Reader`, never via its own `ConsumerAPI`.
- **Rendezvous hashing decides which node is responsible for each workload,
  keyed by concurrency preference.**
  `cluster.Node.ObserveResponsibility(workload)` reports whether this node
  is responsible for a workload among the live nodes, and is updated whenever
  they change. For `MaximizeConcurrency` the workload is
  `uuidpb.Derive(handlerKey, streamID)`, so each (handler, stream) pair is
  placed independently. For `MinimizeConcurrency` the workload is the handler
  key, so one node consumes all of that handler's streams, and its stream
  consumers share a `semaphore.Weighted` around `HandleEvent`, so that a
  canceled context can abandon the wait. No advisory lock is taken. Failover
  is bounded by the heartbeat timeout; responsibility is static.
- **Consumption may briefly overlap.** While nodes join or leave, such as at
  startup when each node sees only itself, two nodes can both consume the same
  stream and dispatch the same event. Dogma's OCC contract makes this
  safe for a correct handler, so no lease or startup delay is used. A
  `MinimizeConcurrency` handler may briefly run on two nodes. Every node
  replies at once to a heartbeat from a node that has just joined, so a new
  node that is already listening learns the cluster within one signal round
  trip. A node that misses heartbeats, at startup or while its signal bus
  reconnects, learns the cluster within one heartbeat interval.
- **A node that can not hear heartbeats consumes nothing.** The set of live
  nodes is built only from signals, so a node whose signal bus is disconnected
  sees its live set empty while its own heartbeats may still reach other
  nodes. While this node's own ID is absent from the live set, its view is
  unknown and it consumes none of its streams.
- **Catch-up subscription semantics** are those `Reader` already provides:
  historical reads are isolated, contemporary delivery is shared and bounded,
  falling behind returns a consumer to history. The remote source must present
  the same shape so the stream consumer is written once.
- **Projections only; processes unchanged.** Processes keep
  `AcquireEventDelivery` + SKIP LOCKED + runkit-owned checkpoints.
- **Signals, not polling.** Local discovery and tail wakeups are signal-driven.
  Foreign streams use gRPC (`ListEventStreams`, `ConsumeEvents`), since
  notifications are per-database.
- **Homogeneous connectivity; no self-peering.**

---

## Phase A — Remote stream sources

### Task A.1: `StreamSource` interface and local implementation

**Files:** create `internal/eventstream/source.go` + test.

- [ ] **Step 1: Extract the interface** from the consumer's and stream
      consumers' use of `Catalog` and `Reader`:

  ```go
  type StreamSource interface {
      // StreamIDs returns the IDs of the streams available from the source,
      // and a channel that is closed when more become available.
      StreamIDs() (*uuidpb.Set, <-chan struct{})

      // SubscribeToEvents registers ch to receive batches of events of the
      // given types from the given stream, starting at checkpoint.
      SubscribeToEvents(streamID *uuidpb.UUID, checkpoint uint64, types *uuidpb.Set, ch chan<- Batch) func()
  }
  ```

- [ ] **Step 2: Local implementation** delegating to `Catalog.StreamIDs` and
      `Reader.Subscribe`; the consumer takes sources instead of the catalog
      and reader.
- [ ] **Step 3: Test + commit.**

### Task A.2: Remote (gRPC) stream source

**Files:** create `internal/eventstream/remote.go` + test (against a real
`ConsumerAPI` server, as the gRPC tests already do).

- [ ] **Step 1: Implement `StreamSource`** over
      `messaginggrpc.EventStreamConsumerAPIClient`: `ListEventStreams` for
      discovery, `ConsumeEvents` per subscription, each `ConsumeEventsResponse`
      becoming a `Batch` (its envelopes and `CheckpointOffset`). Discovery
      publishes the stream IDs through an `xsync.Observable`, as `Catalog`
      does. Each subscription is a supervised function that reconnects from
      the last checkpoint on error.
- [ ] **Step 2: Honor the server's contract:** a closed stream (no requested
      type recordable) ends the subscription; `NOT_FOUND` and `OUT_OF_RANGE` are
      failures, not retries.
- [ ] **Step 3: Test + commit.**

### Task A.3: Peer configuration

**Files:** modify `engineoption.go`, `engine.go`.

- [ ] **Step 1: Add `WithPeer(target string)`** (or a dynamic equivalent) and
      build one remote `StreamSource` per peer with a gRPC client connection.
- [ ] **Step 2: Commit.**

---

## Phase B — End-to-end and hardening

### Task B.1: Foreign events

- [ ] **Step 1: Write** `internal/projection/foreignevents_test.go`: two engines
      with distinct applications and databases, the downstream peered to the
      upstream; a command on the upstream records an event; the downstream's
      projection handles it.
- [ ] **Step 2: Debug systematically** if red: peer connection → remote source →
      discovery → node choice → type filter → delivery. Commit.

### Task B.2: Distribution, failover, catch-up

- [ ] Three replicas over one database: every event handled, total `HandleEvent`
      invocations bounded (no N× redundancy).
- [ ] Add replicas: streams move between nodes, nothing dropped or
      duplicated. Remove one: its streams are consumed elsewhere within about
      the heartbeat timeout.
- [ ] A new projection catching up from zero does not stall caught-up ones; a
      deliberately slow one drops to history and recovers.

### Task B.3: Final validation

- [ ] `make precommit` green; `CHANGELOG.md` `[Unreleased]` entry. Commit.

---

## Out of scope (explicitly deferred)

- **Process handlers** consumed by a node chosen by rendezvous hashing; they
  keep SKIP LOCKED and runkit-owned checkpoints.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous peer connectivity** and per-replica reachability.
- **Relay topologies**; consumers peer directly with producers.
- **Instant failover** for local projections, given up for a single model.
