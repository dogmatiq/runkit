# Command acceptance path

This document records decisions about the command acceptance path -- the
synchronous sequence from `ExecuteCommand()` to returning `nil` (without
`WithEventObserver`). It refines the Phase 3 description in
[000-big-picture.md](000-big-picture.md).

---

## Acceptance, not completion

Without `WithEventObserver`, `ExecuteCommand` returns `nil` at the acceptance
point. The remaining lifecycle steps -- loading instance state, calling
`HandleCommand()`, OCC journal write, event commit, and finalization -- all
happen asynchronously in background goroutines. The caller never waits for
them.

The synchronous path is short: one persistence write, then return. All
subsystem complexity is async.

`WithEventObserver` is a separate problem (Open Question 8 in the big-picture
plan) and is out of scope here.

---

## Ratified foundations

The command acceptance path depends on three ratified ADRs:

- [0002-rendezvous-hashing-for-workload-assignment.md](../adr/0002-rendezvous-hashing-for-workload-assignment.md)
- [0003-optimistic-conflict-resolution.md](../adr/0003-optimistic-conflict-resolution.md)
- [0004-ranked-instruction-routing.md](../adr/0004-ranked-instruction-routing.md)

This document does not restate those ADRs. It only captures
acceptance-path-specific consequences.

### Routing consequences for command acceptance

Command delivery uses the ranked offering protocol from
[0004-ranked-instruction-routing.md](../adr/0004-ranked-instruction-routing.md),
with workload assignment based on
[0002-rendezvous-hashing-for-workload-assignment.md](../adr/0002-rendezvous-hashing-for-workload-assignment.md).
The primary goal is warm-state affinity so the common path avoids storage
reads and minimizes OCC pressure.

For aggregates, the routing key is derived from the instance ID:

```
owner = rendezvous_hash(uuid5(app_key, instance_id), live_node_uuids)
```

For integrations, the routing key depends on the handler's concurrency
preference. With `MinimizeConcurrency`, all commands for a handler funnel
to one node (routing key = handler key). With `MaximizeConcurrency`,
commands spread across nodes (routing key = command UUID). The concurrency
preference can change across deployments; see the routing validation
section for how this is handled.

---

## Acceptance path: keyed vs unkeyed commands

A **keyed command** carries a caller-supplied idempotency key (via
`WithIdempotencyKey`). An **unkeyed command** does not. All commands are
idempotent (or should be). The question is not whether a command is
idempotent, but how idempotency is enforced: by the caller (via an
idempotency key) or by the engine (via internal mechanisms).

When the caller provides an idempotency key, they are supplying a stable
identifier that the engine uses for cluster-wide duplicate detection. The
caller commits to retrying on failure using the same key.

When no idempotency key is provided (an unkeyed command), the engine
enforces idempotency internally:

1. **UUIDv4 command IDs don't collide.** External commands without an
   idempotency key receive a fresh UUIDv4. Two nodes receiving the same
   UUIDv4 is not a realistic scenario. No cluster-wide dedup is needed.

2. **Process-produced commands have dedup already.** The process journal's
   OCC ensures only one execution wins. No cluster-wide dedup needed.

3. **Factspace OCC.** At execution time, the handler's factspace
   positional OCC prevents duplicate side effects even if the same
   command is dispatched twice (e.g., during recovery), as required by
   [0003-optimistic-conflict-resolution.md](../adr/0003-optimistic-conflict-resolution.md).

The acceptance path differs based on which strategy applies.

### Factspace keying

Each handler type has a factspace that records command processing state.
The factspace key determines the granularity of OCC and idempotency
checks, and is therefore an acceptance-path concern.

- **Aggregates:** keyed by `(app, handler_key, instance_id)`. Multiple
  commands may target the same instance, so the factspace tracks the
  full instance lifecycle. OCC contention is per-instance.

- **Integrations:** keyed by `(app, handler_key, command_uuid)`. Each
  command gets its own factspace. This avoids OCC contention between
  concurrent commands (important for MaximizeConcurrency), and makes
  the idempotency check a simple existence/position check on the
  command's own factspace. The key does not include the routing key or
  the node, so a command accepted under one concurrency preference and
  executed after a change hits the same factspace either way.
  Concurrency preference is purely an ephemeral routing concern.

The internal structure of each factspace (what records are stored, how
compaction works, snapshot strategy) is a handler-subsystem concern.

This design covers both aggregates and integrations. Aggregates have
instances and use `RouteCommandToInstance()` to derive an instance ID.
Integrations do not have instances; they route by handler key or command
UUID depending on the concurrency preference. The acceptance path is the
same for both -- only the routing key derivation and scratchspace value
differ.

### Unkeyed commands (1 sync write)

**On the source node (synchronous):**

0. Identify the handler node using the ranked routing protocol from
   [0004-ranked-instruction-routing.md](../adr/0004-ranked-instruction-routing.md).
   This is pure in-memory routing with no disk I/O.

**On the handler node (synchronous):**

1. Determine the handler for this command type using the application's
   routing configuration. For aggregates, call `RouteCommandToInstance()`
   to determine the instance ID.

2. Write a per-node acceptance entry keyed
   `(node, app, command_uuid)` with a value containing the handler key,
   the command envelope, and (for aggregates) the instance ID. The
   handler node's UUID is part of the key so that on restart, each node
   finds its own entries naturally.

3. Dispatch the command to the handler subsystem (in-memory handoff to the
   handler's goroutine/channel).

4. Return `nil` to the source node (and on to the original caller). This
   is the formal acceptance point.

**Why the acceptance keyspace is a KV keyspace, not a Set.** The entry
must store routing decisions and the full command envelope, not just a
UUID. Sets have no value payload.

**Why per-node keying.** The persistencekit `Range()` method enumerates all
entries with no pagination -- cost is linear in entry count. Per-node keying
bounds the recovery enumeration cost to one node's workload. Cluster-wide
keying would make recovery cost proportional to all nodes' combined workload.
Per-node keying also gives the persistence backend a natural partitioning
boundary (separate tables, separate partition key prefixes, etc.).

**What is async.** After returning `nil`, the handler node runs the
remaining command lifecycle steps (load state, call `HandleCommand`, OCC
write, event commit, finalization) entirely in background goroutines. These
do not block the caller.

### Keyed commands (1 sync write)

When the caller provides an idempotency key (a keyed command), the
acceptance path uses an idempotency journal instead of the per-node
acceptance keyspace.

**On the handler node (synchronous):**

1. Append the command envelope to the idempotency journal keyed
   `(app, idempotency_key)` at position 0. A `ConflictError` here means
   the command was already accepted (same key, different submission);
   treat as a no-op success.

2. Dispatch the command to the handler subsystem.

3. Return `nil` to the source node. This is the formal acceptance point.

No per-node acceptance entry is written. The caller has committed to retrying
on failure by providing an idempotency key (see the caller retry contract
discussion below). The engine does not need its own tracking for keyed
commands -- the caller IS the recovery mechanism.

**Orphaned journal entries.** If the caller provides an idempotency key
but never retries after a failure, the journal entry may be orphaned.
This is accepted as a tradeoff.

**Integrations.** Integration handlers follow the same keyed command path
as aggregates when an idempotency key is provided. The idempotency key
provides cluster-wide identity; the handler type determines only how the
command is dispatched after acceptance.

### Why unkeyed commands need only the per-node acceptance keyspace

The original design (earlier in this document's history) wrote two stores
synchronously for every command: a per-node acceptance keyspace + a cluster-wide
per-command journal. This meant every command paid for two round-trips
on the synchronous acceptance path.

The per-command journal served three purposes, none of which require a
separate store for unkeyed commands:

1. **Durable envelope storage.** Now moved into the per-node acceptance entry.
   The keyspace needed to exist anyway; making it a KV (not a Set)
   eliminates the need for a second store.
2. **Cluster-wide dedup** (append at position 0, `ConflictError` =
   duplicate). Unnecessary for UUIDv4 commands -- two nodes will never
   independently generate the same UUID. Only needed when the caller
   supplies an idempotency key.
3. **Recovery source** (any node can read the journal). The per-node
   acceptance keyspace now provides this, with dead-node adoption for the case
   where the accepting node dies.

With all three purposes covered, the per-command journal adds only
latency. Eliminating it cuts the synchronous path from two writes to one.

---

## Recovery

### Restart: per-node acceptance enumeration

On startup, each node opens its own per-node acceptance keyspace `(self, app, *)` and
iterates all entries. For each entry:

1. Check the handler's factspace to determine whether this command was
   already processed. For aggregates, look up
   `(app, handler_key, instance_id)`; for integrations, look up
   `(app, handler_key, command_uuid)`. Both checks are uniform: read
   the factspace and check for a completion marker.
   - If processed: delete the acceptance entry (cleanup). Done.
2. Otherwise: dispatch the command to the handler subsystem for execution.
   This goes through the routing validation path described below.

### Dead-node adoption (unkeyed commands only)

When a node is detected as dead via the heartbeat system, a surviving node
opens the dead node's per-node acceptance keyspace `(dead_node, app, *)` and iterates
all entries. For each entry:

1. Check the handler's factspace to determine whether this command was
   already processed (same check as restart enumeration).
   - If processed: delete the acceptance entry (cleanup). Done.
2. Re-route the command through the current application configuration. This
   goes through the same routing validation path, including the reroute
   mechanism if the routing decision has changed.

**Why dead-node adoption is not needed for keyed commands.** The caller
committed to retrying. If the accepting node dies, the caller eventually
times out and resubmits. The idempotency journal catches the
duplicate.

### Keyed command recovery: caller retry

For keyed commands, the caller is the recovery mechanism. If
`ExecuteCommand` returns an error, the caller resubmits with the same
idempotency key. The idempotency journal catches the duplicate and
the command proceeds.

---

## Routing validation at handler load time

When the handler subsystem picks up a command and does not already have the
handler (or, for aggregates, the instance) hot in memory, it validates the
stored routing decision against the current application configuration
before execution.

Application-layer configuration -- routing rules, concurrency preferences,
handler registrations -- can change across deployments. Since these changes
require a new binary and therefore a restart, every handler goes through
this validation on restart. There is no window where a command executes
under stale configuration.

The general contract: on load, the handler subsystem must verify that
the command can still be executed by the handler recorded in the
acceptance entry. If the handler no longer accepts the command type,
or if the handler no longer exists, the command is moved to the poison
backlog.

The specifics of how each handler type validates and (for aggregates)
reroutes commands are an internal concern of each handler subsystem and
will be captured in their respective ADRs. Key considerations:

- **Aggregates:** `RouteCommandToInstance()` can change across deployments.
  A command stored against one instance ID may now route to a different
  instance. This requires an OCC drain of the old instance's factspace
  before rerouting. See the aggregate subsystem design.

- **Integrations:** concurrency preference can change across deployments,
  which affects node assignment. However, concurrency is a preference, not
  a correctness constraint -- the command can safely execute on whichever
  node currently holds it. The integration factspace is keyed by command
  UUID, not by routing key or node, so the idempotency check works
  identically regardless of which node runs the command or which
  preference was active at acceptance time. See the integration subsystem
  design.

### Unroutable commands

If a command's handler no longer exists in the current application config
and no other handler accepts that command type, the command is moved to the
poison backlog. Trickle-back on future restart re-checks routing, so the
command recovers if the handler is re-added in a future deployment.

---

## Caller retry contract (dogma-level change)

The current dogma docs say callers "should retry" on error and "pass
`WithIdempotencyKey` when retrying." This is advice, not a contract.

In this design, the engine relies on caller retry as the sole recovery
mechanism for keyed commands. This strengthens "should retry" to a
contractual guarantee: by providing an idempotency key, the caller
accepts responsibility for retrying failed submissions. Without caller
retry, a command that fails mid-acceptance may be silently lost.

This decision has been ratified as
[dogma/ADR-31](https://github.com/dogmatiq/dogma/blob/main/docs/adr/0031-require-retries-for-idempotency-keyed-commands.md).

---

## Phases needed on the synchronous critical path

### Phase 2 (persistence options only)

The persistence store options -- `WithJournals`, `WithKeyspaces`, `WithSets`
-- introduced in Phase 2 are required so the engine can open the
per-node acceptance keyspace and (for keyed commands) the idempotency
journal. The rest of Phase 2 (heartbeat keyspace, live node set) is
NOT on the synchronous path and can be deferred.

For a single-node implementation, the live node set can be initialised as
`[self]` inside the Phase 10 wiring without any cross-process heartbeat
plumbing. Phase 2 heartbeat work is only needed when multi-node is in scope.

### Phase 3 -- acceptance portion

Write the per-node acceptance entry (or journal entry for keyed commands),
dispatch to the handler subsystem, return `nil`.

### Phase 10 -- partial wiring

Connect the persistence stores to the acceptance logic and replace
`noopExecutor` with a real executor:

1. Resolve node identity (`WithNodeID` / `DOGMA_NODE_ID` / random UUID).
2. Resolve persistence stores from options (or environment fallback).
3. Wire the acceptance path (per-node acceptance keyspace opener +
   idempotency journal opener).
4. Signal readiness by storing the real executor into `executor.future`
   (currently stores `noopExecutor{}`), which unblocks any callers already
   waiting in `executor.ExecuteCommand`.

The pre-startup blocking behaviour of `executor.future.Wait` is already
implemented and does not change. Only the value stored by `Run()` changes.

---

## Phases NOT needed for this goal

| Phase                             | Why deferred                                                 |
| --------------------------------- | ------------------------------------------------------------ |
| Phase 2 heartbeat / live node set | Not on the synchronous path; single-node needs only `[self]` |
| Phase 3 background execution      | Async; does not block return                                 |
| Phase 4 Integration subsystem     | Aggregate-only is sufficient for the first milestone         |
| Phase 5 Event stream              | Async; only needed by background Phase 3 event commit        |
| Phase 6 Process subsystem         | Async consumer of the event stream                           |
| Phase 7 Projection subsystem      | Async consumer of the event stream                           |
| Phase 8 Poison backlog            | Failure path; deferred                                       |
| Phase 9 Inter-node gRPC           | Single-node first                                            |
| `WithEventObserver` V1/V2/V3      | Separate problem; Open Question 8                            |

---

## Storage summary

This table covers the stores used by the acceptance path, including
factspaces whose keying is an acceptance-path concern (they determine
how idempotency and recovery checks work). The internal structure of
each factspace is a handler-subsystem concern.

| Store                        | Key                                | Type    | Used by         | Lifetime              |
| ---------------------------- | ---------------------------------- | ------- | --------------- | --------------------- |
| Per-node acceptance keyspace | `(node, app, command_uuid)`        | KV      | Unkeyed command | Until completion      |
| Idempotency journal          | `(app, idempotency_key)`           | Journal | Keyed command   | Until completion      |
| Aggregate factspace          | `(app, handler_key, instance_id)`  | Journal | Aggregates      | Permanent (compacted) |
| Integration factspace        | `(app, handler_key, command_uuid)` | Journal | Integrations    | Until completion      |

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

The acceptance path is the same for both aggregates and integrations --
only the routing key derivation differs. If aggregate-only is the first
implementation milestone, the acceptance code can be scoped to commands
routed to aggregate handlers. Integration can be layered in as Phase 4
without changing the acceptance path design.

---

## ADR status

The following foundations referenced by this document are already ratified:

- [0002-rendezvous-hashing-for-workload-assignment.md](../adr/0002-rendezvous-hashing-for-workload-assignment.md)
- [0003-optimistic-conflict-resolution.md](../adr/0003-optimistic-conflict-resolution.md)
- [0004-ranked-instruction-routing.md](../adr/0004-ranked-instruction-routing.md)
- [dogma/ADR-31](https://github.com/dogmatiq/dogma/blob/main/docs/adr/0031-require-retries-for-idempotency-keyed-commands.md) (caller retry contract for commands with idempotency keys)

The main runkit ADR described by this document has been filed:

- [6. Command acceptance and recovery](../adr/0006-command-acceptance-and-recovery.md)
  — covers keyed vs unkeyed acceptance, factspace keying, recovery
  (restart/adoption/caller retry), and routing validation at handler load time.

This thought document records the detailed reasoning and open questions that
informed that ADR.

---
