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

## Decisions

- **One projection consumption path.** Local and foreign streams are consumed
  identically; only the source differs.
- **Local streams first.** Projections consume local streams through `Reader`
  and `Catalog` before any remote source exists. The worker and consumer use
  `*eventstream.Reader` and `*eventstream.Catalog` directly; the
  `StreamSource` interface is extracted when the remote source is written, so
  that a real second implementation shapes it.
- **Handler-owned checkpoints.** Projections resume from
  `handler.CheckpointOffset(streamID)` and report progress via `HandleEvent`'s
  returned offset (OCC). `handler_checkpoints` becomes process-only.
- **The scope's checkpoint is the handler's, not the scan position.**
  `ProjectionEventScope.CheckpointOffset()` is the value `HandleEvent` last
  returned, or `CheckpointOffset(streamID)` when the worker starts. Dogma says
  it "may be lower than the incoming event's offset" when the stream holds
  types the handler does not consume, so `Batch.Checkpoint`, which advances
  past such events, is not used for it. The worker uses `Batch.Checkpoint` for
  nothing.
- **Retry through the supervisor.** An error from `HandleEvent` fails the
  worker, which its supervisor restarts with backoff from
  `CheckpointOffset(streamID)`. The `failures` and `resume_at` columns of
  `handler_checkpoints` are not used for projections.
- **No gRPC loopback.** The engine consumes its own streams via `Catalog` and
  `Reader`, never via its own `ConsumerAPI`.
- **Rendezvous ownership, keyed by concurrency preference.** Candidates are
  the live node IDs from `MembershipObserver`. For `MaximizeConcurrency` the
  workload is `uuidpb.Derive(handlerKey, streamID)`, so each (handler, stream)
  pair is placed independently. For `MinimizeConcurrency` the workload is the
  handler key, so one node owns all of that handler's streams and a mutex in
  the applier serializes `HandleEvent`. No advisory lock is taken. Failover is
  bounded by `HeartbeatTimeout`; assignment is static.
- **Ownership may briefly overlap.** While membership changes, such as at
  startup when each node sees only itself, two nodes can both own a workload
  and deliver the same event. Dogma's OCC contract makes this safe for a
  correct handler, so no lease or startup delay is used. A
  `MinimizeConcurrency` handler may briefly run on two nodes.
- **A node that can not hear heartbeats owns nothing.** Membership is built
  only from signals, so a node whose signal listener is disconnected sees its
  live set empty while its own heartbeats may still reach other nodes. While
  this node's own ID is absent from the live set, its view is unknown and it
  runs no workers.
- **Catch-up subscription semantics** are those `Reader` already provides:
  historical reads are isolated, contemporary delivery is shared and bounded,
  falling behind returns a consumer to history. The remote source must present
  the same shape so the worker is written once.
- **Projections only; processes unchanged.** Processes keep
  `AcquireEventDelivery` + SKIP LOCKED + runkit-owned checkpoints.
- **Signals, not polling.** Local discovery and tail wakeups are signal-driven.
  Foreign streams use gRPC (`ListEventStreams`, `ConsumeEvents`), since
  notifications are per-database.
- **Homogeneous connectivity; no self-peering.**

---

## Phase A — Applier and stream worker

### Task A.1: Projection applier

**Files:** create `internal/projection/applier.go` + test.

- [ ] **Step 1: Define** the per-handler interface:

  ```go
  type Applier interface {
      HandlerIdentity() *identitypb.Identity
      Concurrency() dogma.ConcurrencyPreference
      EventTypeIDs() *uuidpb.Set
      ResumeOffset(ctx context.Context, streamID *uuidpb.UUID) (uint64, error)
      Apply(ctx context.Context, streamID *uuidpb.UUID, checkpoint uint64, env *envelopepb.Envelope) (next uint64, err error)
  }
  ```

- [ ] **Step 2: Implement** for projections: `ResumeOffset` →
      `handler.CheckpointOffset`; `Apply` → `HandleEvent` under
      `xerrors.RecoverT`, building the event scope as
      `projection/eventscope.go` does, with the event's offset taken from the
      envelope's `EventStreamPosition` extension and `checkpoint` as the
      scope's checkpoint offset. For `MinimizeConcurrency`, a mutex in the
      applier serializes `HandleEvent`.
- [ ] **Step 3: Test + commit.**

### Task A.2: Stream worker

**Files:** create `internal/projection/streamworker.go` + test with a fake
applier against a real `Reader`.

- [ ] **Step 1: Implement** the per-(handler, stream) loop. `Reader` already
      provides the historical ↔ contemporary transitions, so the worker is a
      loop, not a state machine:
  - `checkpoint := applier.ResumeOffset(streamID)`; subscribe to the reader at
    `checkpoint` with the applier's types, with a small buffered channel.
  - For each envelope of each batch, `Apply` it with `checkpoint`. If `next`
    is the event's offset plus one, `checkpoint = next`. Otherwise an OCC
    conflict occurred: log it, discard the rest of the batch, unsubscribe,
    set `checkpoint = next` and resubscribe there.
  - `Batch.Checkpoint` is ignored.
  - Run under supervision; an `Apply` error fails the worker, and a restart
    re-derives the start from `ResumeOffset`.
- [ ] **Step 2: Test** catch-up from zero, delivery at the tail, OCC rewind,
      resume after restart, retry after an error, and that the scope's
      checkpoint stays at the handler's value while irrelevant events are
      skipped. Commit.

---

## Phase B — Consumer

### Task B.1: Ownership and reconciliation

**Files:** create `internal/projection/consumer.go` + test.

- [ ] **Step 1: Implement** the engine component. Inputs: this node's ID, the
      `MembershipObserver`, the set of appliers, the `Catalog`, the `Reader`,
      and a logger. It:
  - observes `MembershipObserver.LiveNodeIDs` for the set of live nodes, and
    runs no workers while this node's own ID is absent from it;
  - observes `Catalog.StreamIDs` for the set of streams;
  - for each applier and stream, computes the workload from the applier's
    concurrency preference, and when `rendezvous.Wins(workload, nodeID, live)`
    runs a worker under a `supervisor.Group`; stops it when ownership is lost.
  - Like every other component, its `Run(ctx)` returns an error only when it
    can not continue.
- [ ] **Step 2: Test** with a single live node: every event applied in order
      exactly once; ownership changes start and stop workers; a
      `MinimizeConcurrency` handler's streams are all owned by one node.
      Commit.

---

## Phase C — Engine cut-over

### Task C.1: Wire the consumer

**Files:** modify `engine.go`.

- [ ] **Step 1: Build an applier** per non-disabled `*config.Projection`.
- [ ] **Step 2: Construct and run** the consumer; remove the projection
      `EventPump` from `newComponentsForHandler` (keep the `Compactor` and the
      process pump).
- [ ] **Step 3: Build + commit.**

### Task C.2: `handler_checkpoints` becomes process-only

**Files:** modify `internal/projection/*`.

- [ ] **Step 1: Delete** `projection.EventPump` and its use of
      `AcquireEventDelivery`, `AdvanceStreamCheckpoint` and
      `SetCheckpointOffset`. Leave the `messagepump` helpers for processes.
      Rows written for projections by earlier versions are left in place; the
      DDL can not tell them from process rows.
- [ ] **Step 2: Confirm** `projection.InitializeHandler` and `Compactor` are
      untouched. Commit.

### Task C.3: Migrate projection tests

- [ ] **Step 1: Delete** the four tests in
      `internal/projection/eventstream_test.go` that assert on `failures` and
      `resume_at`: `handlerFailuresCauseStreamConsumptionToBePostponed`,
      `failureCounterIsResetOnSuccess`, `failureCounterGrowsAcrossFailures`
      and `postponedStreamsAreNotConsumed`. The process package keeps its
      copies.
- [ ] **Step 2: Make the test stubs behave like real OCC handlers.** Stubs
      that track a checkpoint compare `s.CheckpointOffset()` with their stored
      value and, on a mismatch, return the stored value without applying the
      event, so a duplicate delivery during an ownership overlap is not
      reported as an error.
- [ ] **Step 3: Re-express** the remaining uses of
      `WaitForHandlerToConsumeAllEvents` in projection tests in terms of the
      handler's own checkpoint (`CheckpointOffsetFunc` on the stubs).
- [ ] **Step 4: Add** a test that a failing projection handler is retried from
      its own checkpoint.
- [ ] **Step 5: Run** the full suite; the existing projection tests (routing,
      OCC, ordering, concurrency) must pass unchanged in behavior. Add the
      `CHANGELOG.md` `[Unreleased]` entry. Commit.

---

## Phase D — Remote stream sources

### Task D.1: `StreamSource` interface and local implementation

**Files:** create `internal/eventstream/source.go` + test.

- [ ] **Step 1: Extract the interface** from the consumer's and worker's use of
      `Catalog` and `Reader`:

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

### Task D.2: Remote (gRPC) stream source

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

### Task D.3: Peer configuration

**Files:** modify `engineoption.go`, `engine.go`.

- [ ] **Step 1: Add `WithPeer(target string)`** (or a dynamic equivalent) and
      build one remote `StreamSource` per peer with a gRPC client connection.
- [ ] **Step 2: Commit.**

---

## Phase E — End-to-end and hardening

### Task E.1: Foreign events

- [ ] **Step 1: Write** `internal/projection/foreignevents_test.go`: two engines
      with distinct applications and databases, the downstream peered to the
      upstream; a command on the upstream records an event; the downstream's
      projection handles it.
- [ ] **Step 2: Debug systematically** if red: peer connection → remote source →
      discovery → ownership → type filter → delivery. Commit.

### Task E.2: Distribution, failover, catch-up

- [ ] Three replicas over one database: every event applied, total `HandleEvent`
      invocations bounded (no N× redundancy).
- [ ] Add replicas: ownership redistributes, nothing dropped or duplicated.
      Remove one: its streams are re-owned within ~`HeartbeatTimeout`.
- [ ] A new projection catching up from zero does not stall caught-up ones; a
      deliberately slow one drops to history and recovers.

### Task E.3: Final validation

- [ ] `make precommit` green; `CHANGELOG.md` `[Unreleased]` entry. Commit.

---

## Out of scope (explicitly deferred)

- **Process handlers** under rendezvous ownership; they keep SKIP LOCKED and
  runkit-owned checkpoints.
- **Hot-spot mitigation** (weighted rendezvous, work stealing).
- **Heterogeneous peer connectivity** and per-replica reachability.
- **Relay topologies**; consumers peer directly with producers.
- **Instant failover** for local projections, given up for a single model.
