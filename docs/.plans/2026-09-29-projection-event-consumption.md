# Unified Projection Event Consumption Implementation Plan

> **For agentic workers:** Implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax for tracking. Build and test exclusively through `make`
> (`make test`, `make precommit`) per the repository conventions — never invoke
> language toolchains directly. Tests spin up PostgreSQL via testcontainers, so a
> working Docker environment is required.

**Goal:** Projections consume events from **all** streams — those recorded by
_this_ application and those recorded by applications on _peer_ engines
(discovered via `WithPeer`) — through a **single** path: rendezvous-assigned
per-stream ownership, a stream-reader abstraction (local direct-SQL + remote
gRPC), handler-owned checkpoints, and a per-(handler, stream) **catch-up
subscription**. Foreign events are never persisted locally.

**Architecture:** Each engine replica joins a database-scoped **cluster** whose
membership rides PostgreSQL `LISTEN`/`NOTIFY`: a `cluster.Heartbeater` emits a
`heartbeat` notification every interval, a shared `cluster.NotificationListener`
owns the listening connection, and a `cluster.MembershipObserver` turns
heartbeats and heartbeat timeouts into `MembershipChange{Added, Removed, Live}`
events. Every event stream — local or foreign — is a **stream source** that can
list its streams and read a stream's events.
Ownership of each **stream** is assigned by **rendezvous (highest-random-weight)
hashing** over the live node set, so exactly one replica consumes a given stream
at a time, rebalancing automatically as nodes join and leave. The owning replica
runs, per projection handler on that stream, a **catch-up subscription**: it
reads history independently from the handler's own `CheckpointOffset()` until it
reaches the tail, then "locks on" to a shared per-stream contemporary feed. It
applies events via `HandleEvent` with the handler's optimistic concurrency
control (OCC) as the correctness backstop. `handler_checkpoints` becomes
**process-only**; projection checkpoints are owned by the handler.

**Tech Stack:** Go, PostgreSQL (`database/sql` + pgx stdlib), gRPC
(`messaginggrpc`), `enginekit` stubs, testcontainers.

---

## Background & Key Decisions

Settled during design (full log in session memory). Binding constraints.

- **U1 — One projection consumption path.** Local and foreign projection streams
  are consumed identically: rendezvous ownership → stream-reader → handler-owned
  checkpoint. The only difference is the leaf reader (local SQL vs remote gRPC).
- **U2 — Handler-owned checkpoints.** Projections resume from
  `handler.CheckpointOffset(streamID)` and report progress via `HandleEvent`'s
  returned offset (OCC). runkit no longer stores projection checkpoints;
  `handler_checkpoints` survives **only for processes**. This matches the Dogma
  contract (the handler owns its checkpoint) and removes runkit's redundant copy.
- **U3 — Stream-reader abstraction (no gRPC loopback).** A `StreamSource` lists
  streams and opens readers; a `StreamReader` yields `(envelope, offset)`. Local
  impl reads the `events` table directly; remote impl uses the gRPC client. The
  existing `ConsumerAPI` poll logic is refactored into a shared reader used by
  the gRPC _server_, the _local_ reader, and kept consistent between them. The
  engine never consumes its own streams over gRPC.
- **U4 — Rendezvous per-stream ownership.** Workload = **stream ID**, candidates
  = the live node IDs from `cluster.MembershipObserver`. Failover is TTL-bounded
  (~300ms via `HeartbeatTimeout`) and assignment is static (not backlog-aware);
  hot-spot mitigation (weighted rendezvous / work-stealing) is **deferred**.
- **U5 — Delivery is a per-(handler, stream) catch-up subscription.**
  - **Historical** (behind the tail): isolated, independent read from the
    handler's checkpoint. A new/reset projection catches up alone and never
    blocks co-located consumers.
  - **Contemporary** (at the tail): the consumer **locks on** to a single shared
    per-stream feed. New appends are read **once** and broadcast into small
    **bounded per-consumer buffers**; each consumer applies independently (own
    checkpoint/OCC/goroutine).
  - **Fall-behind / OCC conflict** → the consumer **drops back to historical**
    from the relevant offset and re-locks-on when it reaches the tail again. A
    slow consumer never gates the shared feed.
- **U6 — Scan position vs checkpoint.** Each consumer tracks an in-memory _scan
  position_ (advances past events of types it doesn't handle) distinct from the
  handler's persistent _checkpoint_ (advances only on `HandleEvent`). Lock-on is
  judged by scan position reaching `next_offset`; **resume after restart** uses
  the handler checkpoint, re-scanning irrelevant events via the type index.
- **U7 — Concurrency preference.** `MinimizeConcurrency` is honored by acquiring
  the **same** handler-keyed advisory lock the local path uses
  (`concurrency.EnforceConcurrencyPreference`), via a short-lived transaction on
  the engine DB. `MaximizeConcurrency` takes no lock.
- **U8 — Projections only; processes unchanged.** Processes keep
  `AcquireEventDelivery` + SKIP LOCKED + `handler_checkpoints` (runkit-owned).
  The two models coexist over the same local streams, split cleanly by handler
  kind.
- **U9 — Homogeneous connectivity assumed (foreign only).** Local streams are
  reachable by every replica via the shared DB; only foreign streams depend on
  peer connectivity, where homogeneous connectivity is assumed and heterogeneous
  connectivity is an out-of-scope robustness concern.
- **U10 — No self-peering.** The engine consumes its own streams via the local
  source, never by adding itself as a peer. The `ConsumerAPI` gRPC server
  continues to serve _other_ engines only.
- **U11 — Catch-up subscription applies to foreign too.** Caught-up foreign
  consumers for a stream share one gRPC live tail; laggards do independent
  per-handler historical gRPC reads. This collapses the H× fan-out at the tail.
- **U12 — Notification-driven signaling (no polling).** Membership and local
  stream activity both ride PostgreSQL `LISTEN`/`NOTIFY` through a shared
  `cluster.NotificationListener`. `Heartbeater` sends `heartbeat`;
  `acquire_for_write` sends `eventstream.create` on new-stream creation; `append`
  sends `eventstream.append.<stream_id>` carrying the new `next_offset`. Local
  stream **discovery** is driven by `eventstream.create` and contemporary **tail
  wakeups** by `eventstream.append.<stream_id>` — no `next_offset` polling. The
  SQL notifications and the cluster listener/observer **already exist**. Foreign
  streams use gRPC discovery/long-poll instead, since `NOTIFY` is per-database.

**Target test:** [`internal/projection/foreignevents_test.go`](../../internal/projection/foreignevents_test.go)
(`TestForeignEvents`) must pass — a downstream projection consumes an upstream
peer's event. It is the first end-to-end milestone of the unified path.

---

## Status summary

- **Phase 1 — Remove `is_foreign`** — **DONE**.
- **Phase 2 — Cluster membership + rendezvous** — **DONE** (notification-based:
  `cluster.Heartbeater`, `cluster.NotificationListener`,
  `cluster.MembershipObserver`, `cluster/rendezvous`).
- **PG notification plumbing** — **DONE** (`eventstream.create` and
  `eventstream.append.<stream_id>` emitted by the SQL functions).
- A **foreign-only prototype** (per-(handler, stream) pull consumers, pull-based
  membership) was built and briefly passed `TestForeignEvents`, then superseded
  by the notification-based `cluster.MembershipObserver` and this unified design. Its reusable parts (the
  projection apply logic, gRPC client usage, engine fan-in of peers) are absorbed
  below; its pull reconciliation loop and pull membership access are **replaced**.

---

## File Structure

**`internal/cluster`** (notification-based membership) — **DONE**:

- `heartbeat.go` — `Heartbeater`: every `HeartbeatInterval` (100ms) runs `SELECT
  pg_notify('heartbeat', node_id)`. No table. `HeartbeatTimeout =
  3 × HeartbeatInterval` = 300ms.
- `notification.go` — `NotificationListener`: a generic `LISTEN`/`NOTIFY` hub.
  `Subscribe(topic, ch chan<- string) func()` manages `LISTEN`/`UNLISTEN` on a
  dedicated pgx connection (held out of the pool), dispatches payloads, and
  reconnects on error.
- `membership.go` — `MembershipObserver`: subscribes to `heartbeat`, tracks
  node→last-seen in memory, expires nodes past `HeartbeatTimeout`, and pushes
  `MembershipChange{Added, Removed, Live}` via `Subscribe(ch chan<-
  MembershipChange) func()` (new subscribers get the current live set).

**`internal/cluster/rendezvous`** (pure HRW hashing) — **DONE**:

- `rendezvous.go` — `Winner`/`Wins`/`Rank`/`RankAbove` over `*uuidpb.UUID`.

**`internal/eventstream`** (stream sources + readers):

- Refactor: extract the stream-listing and event-reading SQL currently in
  `grpc.go` (`pollEventStreams`, `pollEvents`) into a reusable `Reader` type used
  by both the gRPC server and the local source.
- Create: `source.go` — `StreamSource` + `StreamReader` interfaces, and the
  `LocalSource` (direct SQL) implementation. `LocalSource` discovers streams via
  the `eventstream.create` notification (plus an initial listing) and drives
  contemporary wakeups via `eventstream.append.<stream_id>` notifications —
  **already emitted** by the PG functions. It uses the shared
  `NotificationListener`.
- Create: `remote.go` — `RemoteSource`/`RemoteReader` (gRPC client) wrapping
  `messaginggrpc.EventStreamConsumerAPIClient` (discovery via `ListEventStreams`,
  tail via `ConsumeEvents` long-poll).
- **DONE (SQL):** `acquire_for_write` emits `eventstream.create`; `append` emits
  `eventstream.append.<stream_id>` with the new `next_offset`. No polling
  `ActivityWatcher` component is needed.
- Consider relocating `NotificationListener` to a neutral package — it is generic
  infra used by both `cluster` and `eventstream`; it currently lives in
  `cluster`.

**`internal/projectionstream`** (the unified consumer — absorbs the old
`internal/foreignstream`):

- `doc.go`, `consumer.go` — engine-level `Consumer`: aggregates sources
  (`{local} ∪ {peers}`), discovers streams, subscribes to
  `cluster.MembershipObserver`, computes per-stream ownership, and reconciles
  `streamWorker`s.
- `streamworker.go` — the per-(handler, stream) catch-up state machine
  (historical ↔ contemporary).
- `tailfeed.go` — the shared per-stream contemporary feed (read once, broadcast
  into bounded per-consumer buffers).
- `applier.go` — `Applier` interface (resume + apply for one handler) and the
  projection applier (`CheckpointOffset` resume; `HandleEvent` + OCC +
  concurrency lock).
- Tests alongside each file + a subsystem test with a fake source.

**Schema:**

- **DONE:** membership needs **no table** — heartbeats are `pg_notify` only.
- **DONE:** `acquire_for_write` emits `eventstream.create`; `append` emits
  `eventstream.append.<stream_id>` with the new `next_offset`.
- Later: no projection schema change required — `handler_checkpoints` is retained
  for processes; projection rows simply stop being created.

**Engine wiring:**

- `engine.go` — replace the projection `EventPump` component with the unified
  `projectionstream.Consumer`; keep the process `EventPump` and all other
  handler components unchanged. Fan-in peer connections into the consumer.
- `engineoption.go` — `WithPeer` / `WithPeerChanges` / `PeerChange` (present).

---

## Phase 1 — Remove `is_foreign` (B2 groundwork) — DONE

Foreign streams are never stored, so nothing branches on that column.

- Dropped the `is_foreign` column + `streams_by_source` index.
- Removed `NOT is_foreign` clauses from `acquire_for_write`, the `ConsumerAPI`
  `ListEventStreams` query, and `AcquireEventDelivery`.
- Full suite green except the (then-unimplemented) `TestForeignEvents`.

---

## Phase 2 — Cluster membership + rendezvous — DONE

Membership is tracked entirely over PostgreSQL `LISTEN`/`NOTIFY`; there is **no
heartbeats table**.

- `internal/cluster/rendezvous`: HRW hashing over `*uuidpb.UUID` (xxh3), with
  self-affinity and deterministic tie-breaking.
- `internal/cluster/heartbeat.go`: `Heartbeater` — `SELECT pg_notify('heartbeat',
  node_id)` every `HeartbeatInterval` (100ms); `HeartbeatTimeout = 3 ×` = 300ms.
- `internal/cluster/notification.go`: `NotificationListener` — generic
  `LISTEN`/`NOTIFY` hub on a dedicated pgx connection, with reconnect.
- `internal/cluster/membership.go`: `MembershipObserver` — consumes `heartbeat`
  notifications, expires silent nodes after `HeartbeatTimeout`, and pushes
  `MembershipChange{Added, Removed, Live}` to subscribers (current live set
  delivered on subscribe).

---

## Phase 3 — Stream sources & readers

Build the abstraction that makes local and foreign streams interchangeable, so
the consumer is written once.

### Task 3.1: Extract a reusable event reader

**Files:**

- Refactor: `internal/eventstream/grpc.go`
- Create: `internal/eventstream/reader.go`
- Test: `internal/eventstream/reader_test.go`

- [ ] **Step 1: Define the reader.** Move the SQL from `pollEventStreams` /
      `pollEvents` into a `Reader` with two methods, leaving `ConsumerAPI` a thin
      gRPC adapter over it:

  ```go
  // ListStreams returns stream IDs not already in seen, with their next offsets.
  func (r *Reader) ListStreams(ctx context.Context, seen *uuidpb.Set) ([]StreamInfo, error)

  // ReadEvents returns up to limit events of the given types at or after offset,
  // each paired with its stream offset, plus the next checkpoint offset.
  func (r *Reader) ReadEvents(
      ctx context.Context,
      streamID *uuidpb.UUID,
      offset uint64,
      types *uuidpb.Set,
      limit int,
  ) (events []ReadEvent, next uint64, err error)
  ```

- [ ] **Step 2: Re-point `ConsumerAPI`** at `Reader` so the server behavior is
      unchanged.

- [ ] **Step 3: Test + run.** `go test ./internal/eventstream/` — existing gRPC
      tests must still pass; add a direct `Reader` test.

- [ ] **Step 4: Commit.** `"Extract reusable eventstream.Reader"`.

### Task 3.2: `StreamSource` / `StreamReader` interfaces + `LocalSource`

**Files:**

- Create: `internal/eventstream/source.go`
- Test: `internal/eventstream/source_test.go`

- [ ] **Step 1: Define the interfaces.**

  ```go
  // StreamSource lists streams and opens readers for them.
  type StreamSource interface {
      // ListStreams reports the current stream IDs and notifies of new ones.
      ListStreams(ctx context.Context) ([]*uuidpb.UUID, error)

      // Open returns a StreamReader positioned at offset, filtered to types.
      Open(streamID *uuidpb.UUID, types *uuidpb.Set) StreamReader
  }

  // StreamReader yields events in offset order from where it was opened.
  type StreamReader interface {
      // Next returns the next event, or ok=false when momentarily caught up.
      Next(ctx context.Context) (env *envelopepb.Envelope, offset uint64, ok bool, err error)
      Close() error
  }
  ```

- [ ] **Step 2: Implement `LocalSource`** over `eventstream.Reader` (direct SQL),
      paging via `ReadEvents`.

- [ ] **Step 3: Test + run** against a populated local DB (reuse
      `xtesting.PopulateEventStreams`). Commit `"Add StreamSource with local
implementation"`.

### Task 3.3: `RemoteSource` (gRPC client)

**Files:**

- Create: `internal/eventstream/remote.go`
- Test: `internal/eventstream/remote_test.go` (fake or real `ConsumerAPI` server)

- [ ] **Step 1: Implement `RemoteSource`/`RemoteReader`** wrapping
      `ListEventStreams` and `ConsumeEvents`, extracting each event's offset from
      the `EventStreamPosition` extension.
- [ ] **Step 2: Test + run + commit** `"Add remote (gRPC) StreamSource"`.

### Task 3.4: Notification-driven local discovery & tail wakeups — SQL DONE

The PG functions already emit the needed notifications (`eventstream.create`,
`eventstream.append.<stream_id>`); no polling watcher is required.

**Files:**

- Use: `internal/cluster/notification.go` (`NotificationListener`).
- Wire within: `internal/eventstream/source.go` (`LocalSource`).

- [ ] **Step 1: `LocalSource` discovery.** On start, list existing streams
      (`SELECT id FROM eventstream.streams`), then subscribe to `eventstream.create`
      via the `NotificationListener` to learn new stream IDs.
- [ ] **Step 2: Contemporary wakeups.** The local tail feed for stream S
      subscribes to `eventstream.append.<S>`; each payload is the stream's new
      `next_offset`, which wakes the feed to read and broadcast the new events.
- [ ] **Step 3: Test** that creating a stream and appending events drives
      discovery and tail delivery without polling. Commit.

> Optional follow-up (not required here): make the gRPC `ConsumeEvents` **server**
> notification-driven too (subscribe to `eventstream.append.<S>` instead of its
> 25ms poll) so remote tails are low-latency.

---

## Phase 4 — Catch-up subscription delivery

The per-(handler, stream) state machine and the shared contemporary feed. This is
the core of U5/U6 and the most delicate code — design its internal structure and
tests carefully before implementing.

### Task 4.1: `Applier` + projection applier

**Files:**

- Create: `internal/projectionstream/applier.go`
- Test: `internal/projectionstream/applier_test.go`

- [ ] **Step 1: Define** the per-handler interface (generalizes the prototype's
      `Driver`):

  ```go
  type Applier interface {
      HandlerIdentity() *identitypb.Identity
      EventTypeIDs() *uuidpb.Set
      ResumeOffset(ctx context.Context, streamID *uuidpb.UUID) (uint64, error)
      Apply(ctx context.Context, streamID *uuidpb.UUID,
          checkpointOffset, eventOffset uint64, env *envelopepb.Envelope) (next uint64, err error)
  }
  ```

- [ ] **Step 2: Implement the projection applier:** `ResumeOffset` →
      `handler.CheckpointOffset`; `Apply` → `HandleEvent` wrapped in
      `concurrency.EnforceConcurrencyPreference` (short-lived engine-DB tx for
      `MinimizeConcurrency`; direct call otherwise), building the `eventScope` as
      [`projection.eventpump.go`](../../internal/projection/eventpump.go) does.
      (The prototype's `ForeignDriver` is the starting point.)
- [ ] **Step 3: Test + run + commit.**

### Task 4.2: Contemporary tail feed

**Files:**

- Create: `internal/projectionstream/tailfeed.go`
- Test: `internal/projectionstream/tailfeed_test.go`

- [ ] **Step 1: Implement** a per-stream feed that, when woken by the
      `eventstream.append.<S>` notification (local) or the gRPC live tail
      (remote), reads each new event **once** and broadcasts `(env, offset)` into
      each locked-on subscriber's **bounded** buffer. Subscribers join at a given
      offset and leave on demand; a subscriber whose buffer would overflow is
      signalled to **detach** (drop to historical).
- [ ] **Step 2: Test** fan-out to multiple subscribers, single read per event,
      and overflow → detach signal. Commit.

### Task 4.3: `streamWorker` state machine

**Files:**

- Create: `internal/projectionstream/streamworker.go`
- Test: `internal/projectionstream/streamworker_test.go`

- [ ] **Step 1: Implement** the per-(handler, stream) worker:
  - Start in **historical** mode: `scan := applier.ResumeOffset(streamID)`; open a
    `StreamReader` at `scan`; for each `(env, offset)` call
    `applier.Apply(..., checkpointOffset=scan, eventOffset=offset)`; on success
    advance `scan = max(scan, offset+1)`; on OCC conflict set `scan = next`.
  - When the reader reports caught-up (no more events and `scan == next_offset`),
    transition to **contemporary**: subscribe to the stream's tail feed at `scan`.
  - In **contemporary** mode, apply broadcast events from the bounded buffer; on
    overflow/detach or OCC conflict, drop back to **historical** from the current
    offset.
  - Resume-after-restart always re-derives the start from
    `applier.ResumeOffset` (the handler checkpoint), per U6.
- [ ] **Step 2: Test** the transitions deterministically with a fake source +
      fake applier: historical catch-up, lock-on at tail, live delivery,
      fall-behind → historical, OCC rewind. Commit.

---

## Phase 5 — Unified consumer

### Task 5.1: `Consumer` (ownership + reconciliation)

**Files:**

- Create: `internal/projectionstream/consumer.go`
- Test: `internal/projectionstream/consumer_test.go`

- [ ] **Step 1: Implement** the engine-level component. Inputs: this node's ID,
      the `cluster.MembershipObserver`, the `cluster.NotificationListener`, the
      set of projection `Applier`s, the local `StreamSource`, a channel of remote
      sources derived from peer connections, and a logger. `Run(ctx)`:
  - Subscribe to `cluster.MembershipObserver` for `MembershipChange`; reconcile
    on change using its `Live` set.
  - Discover streams from every source (local via an initial `ListStreams` +
    `eventstream.create` notifications; remote via `ListEventStreams`),
    maintaining `streamID → source`.
  - For each discovered stream, ownership is per-stream:
    `rendezvous.Wins(streamID, nodeID, liveNodes)`. When owned, start one
    `streamWorker` per `Applier` (keyed `handlerKey + "/" + streamID`), sharing
    one tail feed per stream; when not owned or the stream/source disappears,
    stop them.
  - Supervise with an `errgroup`, matching `engine.go` style.
- [ ] **Step 2: Test** with a fake local source (one stream, a few events) and a
      single-node heartbeat: assert the stub projection applies all events, in
      order, exactly once. Commit.

---

## Phase 6 — Engine wiring & local cut-over

### Task 6.1: Build appliers and run the consumer

**Files:**

- Modify: `engine.go`

- [ ] **Step 1: Build a projection `Applier` per non-disabled
      `*config.Projection`** (identity, concurrency, `EventTypeIDs`, `e.db`,
      logger) — the same inputs the local `EventPump` used.
- [ ] **Step 2: Construct the local `StreamSource`, the
      `cluster.NotificationListener`, the `cluster.Heartbeater`, the
      `cluster.MembershipObserver`, and the `projectionstream.Consumer`;** fan-in
      peer connections to remote sources. Run them as components in the errgroup.
- [ ] **Step 3: Remove the projection `EventPump` component** from
      `newComponentsForHandler` (keep the `Compactor` and the process
      `EventPump`). Projections are now served solely by the unified consumer.
- [ ] **Step 4: Build** `go build ./...`. Commit.

### Task 6.2: Make `handler_checkpoints` process-only

**Files:**

- Modify: `internal/projection/*` (remove the local projection pump path)

- [ ] **Step 1: Delete** `projection.EventPump` and its use of
      `AcquireEventDelivery`/`AdvanceStreamCheckpoint`/`SetCheckpointOffset`.
      Leave the `messagepump` helpers for processes.
- [ ] **Step 2: Confirm** `projection.InitializeHandler` (inserts into
      `projection.handlers`) and `Compactor` (uses `projection.handlers`) are
      untouched.
- [ ] **Step 3: Build.** Commit `"Serve projections via the unified consumer;
handler_checkpoints is process-only"`.

### Task 6.3: Migrate projection tests

**Files:**

- Modify: projection tests asserting on `handler_checkpoints`
  (e.g. [`eventstream_test.go`](../../internal/projection/eventstream_test.go))

- [ ] **Step 1: Re-express** checkpoint assertions in terms of the handler's own
      checkpoint (stubs implement `CheckpointOffsetFunc`, as the OCC tests already
      do) instead of querying `handler_checkpoints`.
- [ ] **Step 2: Run** `go test ./internal/projection/` until green. Commit.

---

## Phase 7 — End-to-end milestones

### Task 7.1: `TestForeignEvents` passes via the unified path

- [ ] **Step 1: Run** `go test ./internal/projection/ -run TestForeignEvents -v`.
      Expected: PASS — the downstream projection observes the upstream event
      through the unified consumer + remote source.
- [ ] **Step 2: Debug systematically** if red: peer fan-in → remote source;
      discovery; ownership; type filter; envelope offset extraction; historical →
      contemporary handoff. Commit.

### Task 7.2: Local projections still pass end-to-end

- [ ] **Step 1: Run** the full projection suite and `make test`. The existing
      local projection tests (routing, OCC, ordering, event stream) must pass
      through the unified path. Commit.

---

## Phase 8 — Hardening & verification

### Task 8.1: Multi-replica distribution

- [ ] Run the consumer with 3 replicas sharing one DB (mirroring
      `concurrentEngines`), over a mix of local and foreign streams. Assert every
      event is applied and total `HandleEvent` invocations are bounded (no N×
      redundancy), proving rendezvous distributes streams. Commit.

### Task 8.2: Rebalancing & failover

- [ ] Start 1 replica owning all streams; add 2 — ownership redistributes roughly
      evenly, no dropped/duplicated events. Stop 1 — its streams are re-owned
      within ~TTL. Commit.

### Task 8.3: Catch-up subscription behavior

- [ ] With a stream at the tail being consumed by several caught-up projections,
      add a **new** projection (checkpoint 0): assert it catches up in historical
      mode **without** stalling the caught-up ones (no gap in their live
      delivery). Assert a deliberately-slow consumer drops to historical and
      recovers. Commit.

### Task 8.4: Final validation

- [ ] `make precommit` green. Add a `CHANGELOG.md` `[Unreleased]` entry describing
      unified projection consumption (local + foreign). Commit.

---

## Out of Scope (explicitly deferred)

- **Process handlers** under rendezvous/stream-reader. They keep SQL/SKIP LOCKED
  - runkit-owned `handler_checkpoints`. Unifying _process coordination_ (keeping
    their checkpoint runkit-owned) is a plausible later step using the same
    machinery.
- **Hot-spot mitigation** — backlog-aware weighting or work-stealing on top of
  rendezvous, if static assignment ever imbalances load.
- **Notification-driven gRPC server** — making `ConsumeEvents` subscribe to
  `eventstream.append.<S>` instead of its 25ms poll, for low-latency remote
  tails. (Local activity is already notification-driven; this is the remaining
  polling path.)
- **Heterogeneous peer connectivity** / per-replica reachability in rendezvous.
- **Relay topologies** (A consuming from B, then re-serving to C) — impossible
  under B2 by design; consumers peer directly with producers.
- **Instant failover / backlog-aware catch-up for local projections** — given up
  deliberately in exchange for a single consumption model (U4).
