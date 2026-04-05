# CommandExecutor: Acceptance Path to `nil` (without `WithEventObserver`)

This document records the research into what phases and steps are needed for
`ExecuteCommand` to return `nil` when called without `WithEventObserver`. It
refines the Phase 3 description in [000-big-picture.md](000-big-picture.md)
and is intended as the planning input for the agent that implements Phases 2,
3 (acceptance portion), and the partial Phase 10 wiring needed to support it.

---

## The key insight: acceptance, not completion

The big-picture plan lists "Append to command journal at position 0 --
Acceptance point" as step 3 of 9 in the Phase 3 command lifecycle. Without
`WithEventObserver`, `ExecuteCommand` should return `nil` at that acceptance
point. The remaining six steps -- routing to the instance-owning node,
loading instance state, calling `HandleCommand()`, OCC journal write, event
commit, and finalization -- all happen asynchronously in background goroutines.
The caller never waits for them.

This means the synchronous path from `ExecuteCommand` to `nil` is very short:
write two persistence stores, then return. All subsystem complexity is async.

Contrast with `WithEventObserver`: in that case, the caller additionally waits
for a specific event to be produced by the causal chain. That is a separate
problem (Open Question 8 in the big-picture plan) and is explicitly out of
scope here.

---

## The acceptance sequence (order is load-bearing)

Two nodes are involved (or one, when the producer and handler are the same):

- **Producer node**: the node on which `ExecuteCommand()` was called.
- **Handler node**: the node selected by applying the application-level and
  cluster-level routing rules as understood by the producer. This is the node
  that will write the backlog entry and command journal and, asynchronously,
  run `HandleCommand`. Until Phase 9 (inter-node gRPC) is built, the handler
  node is always the producer itself (fallback-to-self).

**On the producer node (synchronous):**

0. Identify the handler node using ranked iteration with fallback to self
   (see the Instruction routing section of the big-picture plan). This is a
   pure in-memory control-plane operation -- no disk I/O. Until Phase 9 is
   built, self is always selected.

**On the handler node (synchronous, before nil is returned to the producer):**

All of the following happen on the handler node.

1. Add `command_uuid` to command backlog Set keyed `(node, app, command_uuid)`.
   Using the handler node's UUID as part of the key ensures that on restart,
   each node finds its own backlog naturally.

2. Append the command envelope to the command journal keyed `(app, command_uuid)`
   at position 0. A `ConflictError` here means the command was already accepted
   (same UUID, different submission); treat as a no-op success. This is the
   formal acceptance point.

3. Return `nil` to the producer (and on to the original caller).

**Why backlog first?** If the handler node crashes between write 1 and
write 2, the orphaned backlog entry survives in durable storage. Recovery
logic (on restart or from another node) can detect that the backlog entry has
no corresponding command journal and discard it safely. The reverse order --
journal first, then backlog -- creates a window where a durably accepted
command has no backlog entry: it is invisible to all recovery passes and
silently lost.

**What is async.** After returning `nil`, the handler node runs the
remaining command lifecycle steps (load state, call `HandleCommand`, OCC
write, event commit, finalization) entirely in background goroutines. These
do not block the caller.

---

## Phases needed on the synchronous critical path

### Phase 2 (persistence options only)

The persistence store options -- `WithJournals`, `WithKeyspaces`, `WithSets`
-- introduced in Phase 2 are required so the engine can open the backlog Set
and command journal. The rest of Phase 2 (heartbeat keyspace, live node set)
is NOT on the synchronous path and can be deferred.

For a single-node implementation, the live node set can be initialised as
`[self]` inside the Phase 10 wiring without any cross-process heartbeat
plumbing. Phase 2 heartbeat work is only needed when multi-node is in scope.

### Phase 3 -- acceptance portion

Write the backlog entry, write the command journal, return `nil`. The
implementation lives in `internal/subsystem/aggregate` (and later
`internal/subsystem/integration`, which shares the same stores). For the
first working milestone, aggregate-only is sufficient.

### Phase 10 -- partial wiring

Connect the persistence stores to the acceptance logic and replace
`noopExecutor` with a real executor:

1. Resolve node identity (`WithNodeID` / `DOGMA_NODE_ID` / random UUID).
2. Resolve persistence stores from options (or environment fallback).
3. Wire the acceptance path (backlog Set opener + command journal opener).
4. Signal readiness by storing the real executor into `executor.future`
   (currently stores `noopExecutor{}`), which unblocks any callers already
   waiting in `executor.ExecuteCommand`.

The pre-startup blocking behaviour of `executor.future.Wait` is already
implemented and does not change. Only the value stored by `Run()` changes.

---

## Phases NOT needed for this goal

| Phase | Why deferred |
| ----- | ------------ |
| Phase 2 heartbeat / live node set | Not on the synchronous path; single-node needs only `[self]` |
| Phase 3 background execution | Async; does not block return |
| Phase 4 Integration subsystem | Aggregate-only is sufficient for the first milestone |
| Phase 5 Event stream | Async; only needed by background Phase 3 event commit |
| Phase 6 Process subsystem | Async consumer of the event stream |
| Phase 7 Projection subsystem | Async consumer of the event stream |
| Phase 8 Poison backlog | Failure path; deferred |
| Phase 9 Inter-node gRPC | Single-node first |
| `WithEventObserver` V1/V2/V3 | Separate problem; Open Question 8 |

---

## Open questions

### OQ-A: Phase 2 heartbeat -- skip or stub for single-node?

The live node set is the input to rendezvous hashing. For a single-node
engine, the set is always `[self]`, so rendezvous always selects self. This
means Phase 2 heartbeat logic can be replaced with a trivial in-process
initialisation (populate the set with the local node UUID at startup) until
multi-node support is needed.

Decision needed: implement Phase 2 fully upfront, or start with single-node
stub and layer in the heartbeat later?

### OQ-B: Handler invocation interface (OQ6 from engine-as-platform.md)

Phase 3's background execution path must call `HandleCommand()`. The
engine-as-platform doc recommends a thin location-transparent interface
(Option B) rather than a direct call to `dogma.AggregateMessageHandler`.
This decision only affects the background path, not the synchronous return
path, but it must be made before the background execution code is written.

### OQ-C: Aggregate-only or aggregate + integration for the first milestone?

The command backlog and command journal are shared between the aggregate and
integration subsystems. If aggregate-only is the first milestone, the
acceptance code can be scoped to commands routed to aggregate handlers.
Integration can be layered in as Phase 4 without touching the acceptance path.

---

## Relationship to the big-picture plan

The numbered steps in the big-picture Phase 3 description remain correct as
a description of the full command lifecycle. This document only clarifies
**where `nil` is returned** within that lifecycle (after step 3) and what is
therefore on vs. off the synchronous path for the specific goal of supporting
`ExecuteCommand` without `WithEventObserver`.

No changes to the big-picture plan are required as a result of this research.
