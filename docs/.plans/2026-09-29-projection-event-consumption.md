# Unified Projection Event Consumption — Remaining Work

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events from **all** streams — those recorded by
_this_ application and those recorded by applications on _peer_ engines —
through a **single** path: rendezvous-assigned per-stream ownership, a stream
source abstraction (local + remote gRPC), handler-owned checkpoints, and a
per-(handler, stream) catch-up subscription. Foreign events are never persisted
locally.

**Tech Stack:** Go, PostgreSQL (`database/sql` + pgx stdlib), gRPC
(`messaginggrpc`), `enginekit` stubs, testcontainers.

---

## Where things stand (2026-10-28)

The infrastructure the unified path needs is built, on the `eventstream-grpc`
branch. The consumer that uses it is not.

**Built:**

- **Cluster membership** (`internal/cluster`): `Heartbeater` sends a signal on
  `heartbeat.<node-id>` every `HeartbeatInterval` (1s); `MembershipObserver`
  subscribes to `heartbeat`, expires nodes after `HeartbeatTimeout` (5s), and
  publishes the live set through `LiveNodeIDs`, an `xsync.Observable` that
  never blocks on slow consumers. `cluster/rendezvous` provides HRW
  hashing over `*uuidpb.UUID` (`Winner`, `Wins`, `Rank`, `RankAbove`).
- **Signals** (`internal/signal`): `Listener` receives PostgreSQL notifications
  on a single channel whose payload is a hierarchical, dot-separated topic, and
  delivers the subtopic to subscribers of that topic or any topic above it.
  Delivery is lossy: a signal is a hint to re-read state, never a payload. When
  the listener starts listening it signals every subscriber, so subscribers
  that registered before it, and signals missed during a reconnect, are
  covered. `signal.Notify` sends from Go; triggers on `eventstream.streams`
  and `eventstream.events` send `eventstream.create.<stream-id>` and
  `eventstream.append.<stream-id>` for every write path.
- **Local stream reading** (`internal/eventstream`): `Catalog` discovers streams
  and sends new IDs to subscribers. `Reader` is the per-consumer catch-up
  subscription for local streams: `Subscribe(streamID, checkpoint, types, ch)`
  delivers `Batch`es (envelopes plus the checkpoint to resume from, so a
  consumer advances past types it does not handle). Each stream has a cache of
  recent events fed by a `scanner.Contemporary`; consumers behind the cache read
  history through their own `scanner.Historical` and rejoin the cache when they
  catch up; a consumer that stops keeping up is returned to history rather than
  blocking anyone. `ConsumerAPI` serves `ListEventStreams` and `ConsumeEvents`
  over gRPC from `Catalog` and `Reader`, signal-driven, with the service
  definition's error contract.
- **Supervision** (`internal/supervisor`): `Run` and `Group` retry a fallible
  function with backoff, unless it fails with `supervisor.Fatal`. Every engine
  component supervises its own fallible parts and returns an error only when it
  can not continue, which stops the engine.
- **Schema:** `is_foreign` is gone; foreign events are never stored.

**Not built** (the discarded foreign-only prototype's versions no longer exist):

- Peer configuration on the engine (`WithPeer` or equivalent) and remote gRPC
  stream sources.
- The projection applier and per-(handler, stream) worker.
- The engine-level consumer that owns streams by rendezvous and runs workers.
- The cut-over: projections are still served by `projection.EventPump` with
  runkit-owned checkpoints in `handler_checkpoints`.
- An end-to-end foreign-events test.

---

## Decisions still binding

- **One projection consumption path.** Local and foreign streams are consumed
  identically; only the source differs.
- **Handler-owned checkpoints.** Projections resume from
  `handler.CheckpointOffset(streamID)` and report progress via `HandleEvent`'s
  returned offset (OCC). `handler_checkpoints` becomes process-only.
- **No gRPC loopback.** The engine consumes its own streams via `Catalog` and
  `Reader`, never via its own `ConsumerAPI`.
- **Rendezvous per-stream ownership.** Workload = stream ID, candidates = the
  live node IDs from `MembershipObserver`. Failover is bounded by
  `HeartbeatTimeout`; assignment is static.
- **Catch-up subscription semantics** are those `Reader` already provides:
  historical reads are isolated, contemporary delivery is shared and bounded,
  falling behind returns a consumer to history. The remote source must present
  the same shape so the worker is written once.
- **Scan position vs checkpoint.** A `Batch.Checkpoint` is the scan position
  (advances past irrelevant types); the handler checkpoint advances only on
  `HandleEvent`. Resume after restart uses the handler checkpoint.
- **Concurrency preference.** `MinimizeConcurrency` is honored by acquiring the
  handler-keyed advisory lock via `concurrency.EnforceConcurrencyPreference`;
  `MaximizeConcurrency` takes no lock.
- **Projections only; processes unchanged.** Processes keep
  `AcquireEventDelivery` + SKIP LOCKED + runkit-owned checkpoints.
- **Signals, not polling.** Local discovery and tail wakeups are signal-driven.
  Foreign streams use gRPC (`ListEventStreams`, `ConsumeEvents`), since
  notifications are per-database.
- **Homogeneous connectivity; no self-peering.**

---

## Phase A — Stream sources

Make local and foreign streams interchangeable to the consumer. The local
implementation is an adapter over what exists; no new reading code.

### Task A.1: `StreamSource` interface and local implementation

**Files:** create `internal/eventstream/source.go` + test.

- [ ] **Step 1: Define the interface** in terms of the existing shapes:

  ```go
  type StreamSource interface {
      // SubscribeToStreams registers ch to receive the ID of each stream as it
      // becomes available, beginning with those that already exist.
      SubscribeToStreams(ch chan<- *uuidpb.UUID) func()

      // SubscribeToEvents registers ch to receive batches of events of the
      // given types from the given stream, starting at checkpoint.
      SubscribeToEvents(streamID *uuidpb.UUID, checkpoint uint64, types *uuidpb.Set, ch chan<- Batch) func()
  }
  ```

- [ ] **Step 2: Local implementation** delegating to `Catalog.StreamIDs` and
      `Reader.Subscribe`.
- [ ] **Step 3: Test + commit.**

### Task A.2: Remote (gRPC) stream source

**Files:** create `internal/eventstream/remote.go` + test (against a real
`ConsumerAPI` server, as the gRPC tests already do).

- [ ] **Step 1: Implement `StreamSource`** over
      `messaginggrpc.EventStreamConsumerAPIClient`: `ListEventStreams` for
      discovery, `ConsumeEvents` per subscription, each `ConsumeEventsResponse`
      becoming a `Batch` (its envelopes and `CheckpointOffset`). Each
      subscription is a supervised function that reconnects from the last
      checkpoint on error.
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

## Phase B — Applier and stream worker

### Task B.1: Projection applier

**Files:** create `internal/projection/applier.go` + test.

- [ ] **Step 1: Define** the per-handler interface:

  ```go
  type Applier interface {
      HandlerIdentity() *identitypb.Identity
      EventTypeIDs() *uuidpb.Set
      ResumeOffset(ctx context.Context, streamID *uuidpb.UUID) (uint64, error)
      Apply(ctx context.Context, streamID *uuidpb.UUID, checkpoint, offset uint64, env *envelopepb.Envelope) (next uint64, err error)
  }
  ```

- [ ] **Step 2: Implement** for projections: `ResumeOffset` →
      `handler.CheckpointOffset`; `Apply` → `HandleEvent` under
      `concurrency.EnforceConcurrencyPreference`, building the event scope as
      `projection/eventscope.go` does.
- [ ] **Step 3: Test + commit.**

### Task B.2: Stream worker

**Files:** create `internal/projection/streamworker.go` + test with a fake
source and fake applier.

- [ ] **Step 1: Implement** the per-(handler, stream) loop. `Reader` already
      provides the historical ↔ contemporary transitions, so the worker is a
      loop, not a state machine:
  - `checkpoint := applier.ResumeOffset(streamID)`; subscribe to the source at
    `checkpoint` with the applier's types, with a small buffered channel.
  - For each batch, `Apply` each envelope with the current checkpoint; on
    success the checkpoint becomes the applier's returned `next`; after the
    batch, the checkpoint becomes `Batch.Checkpoint`.
  - On OCC conflict (`next` differs from the expected offset), unsubscribe and
    resubscribe at `next`.
  - Run under supervision; a restart re-derives the start from `ResumeOffset`.
- [ ] **Step 2: Test** catch-up from zero, delivery at the tail, OCC rewind,
      and resume after restart. Commit.

---

## Phase C — Consumer

### Task C.1: Ownership and reconciliation

**Files:** create `internal/projection/consumer.go` + test.

- [ ] **Step 1: Implement** the engine component. Inputs: this node's ID, the
      `MembershipObserver`, the set of appliers, the local source and the remote
      sources, and a logger. It:
  - observes `LiveNodeIDs` for the set of live nodes;
  - subscribes to streams from every source, maintaining stream → source;
  - for each stream, when `rendezvous.Wins(streamID, nodeID, live)`, runs one
    worker per applier under a `supervisor.Group`; stops them when ownership is
    lost or the stream's source disappears.
  - Like every other component, its `Run(ctx)` returns an error only when it
    can not continue.
- [ ] **Step 2: Test** with a fake source and a single live node: every event
      applied in order exactly once; ownership changes start and stop workers.
      Commit.

---

## Phase D — Engine cut-over

### Task D.1: Wire the consumer

**Files:** modify `engine.go`.

- [ ] **Step 1: Build an applier** per non-disabled `*config.Projection`.
- [ ] **Step 2: Construct and run** the consumer with the local source and the
      peer sources; remove the projection `EventPump` from
      `newComponentsForHandler` (keep the `Compactor` and the process pump).
- [ ] **Step 3: Build + commit.**

### Task D.2: `handler_checkpoints` becomes process-only

**Files:** modify `internal/projection/*`.

- [ ] **Step 1: Delete** `projection.EventPump` and its use of
      `AcquireEventDelivery`, `AdvanceStreamCheckpoint` and
      `SetCheckpointOffset`. Leave the `messagepump` helpers for processes.
- [ ] **Step 2: Confirm** `projection.InitializeHandler` and `Compactor` are
      untouched. Commit.

### Task D.3: Migrate projection tests

- [ ] **Step 1: Re-express** assertions on `handler_checkpoints` in terms of the
      handler's own checkpoint (`CheckpointOffsetFunc` on the stubs).
- [ ] **Step 2: Run** `make test`. Commit.

---

## Phase E — End-to-end and hardening

### Task E.1: Foreign events

- [ ] **Step 1: Write** `internal/projection/foreignevents_test.go`: two engines
      with distinct applications and databases, the downstream peered to the
      upstream; a command on the upstream records an event; the downstream's
      projection handles it.
- [ ] **Step 2: Debug systematically** if red: peer connection → remote source →
      discovery → ownership → type filter → delivery. Commit.

### Task E.2: Local projections through the unified path

- [ ] **Step 1: Run** the full suite; the existing projection tests (routing,
      OCC, ordering, event stream) must pass unchanged in behavior. Commit.

### Task E.3: Distribution, failover, catch-up

- [ ] Three replicas over one database: every event applied, total `HandleEvent`
      invocations bounded (no N× redundancy).
- [ ] Add replicas: ownership redistributes, nothing dropped or duplicated.
      Remove one: its streams are re-owned within ~`HeartbeatTimeout`.
- [ ] A new projection catching up from zero does not stall caught-up ones; a
      deliberately slow one drops to history and recovers.

### Task E.4: Final validation

- [ ] `make precommit` green; `CHANGELOG.md` `[Unreleased]` entry. Commit.

---

## Out of scope (explicitly deferred)

- **Process handlers** under rendezvous ownership; they keep SKIP LOCKED and
  runkit-owned checkpoints.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous peer connectivity** and per-replica reachability.
- **Relay topologies**; consumers peer directly with producers.
- **Instant failover** for local projections, given up for a single model.
