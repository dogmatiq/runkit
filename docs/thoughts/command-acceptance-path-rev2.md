# Command acceptance path (rev 2)

This document supersedes [command-acceptance-path-rev1.md]. It records the
revised design for the command acceptance path — the synchronous sequence
from `ExecuteCommand()` to returning `nil` (without `WithEventObserver`).

The key change from rev 1 is the replacement of the per-node, per-command
acceptance keyspace with entity-level dirty flags and
factspace-as-acceptance-record. The idempotent command submission layer
(idempotency keys) is separated into its own concern; this document covers
the base system that makes every accepted command durable.

> **Terminology note.** This document uses "entity" as informal shorthand
> for "the thing that owns a factspace and receives commands" — an
> aggregate instance or an integration handler. This term is a convenience
> for this thinking document only. It should not enter the ADRs or
> glossary. In formal writing, use the specific term for each handler
> type: "instance" for aggregates, "handler" for integrations.

---

## Acceptance, not completion

The Dogma API requires `ExecuteCommand()` to return only after the
engine has confirmed ownership of the command. Returning `nil` is a
promise: the command will execute, regardless of subsequent crashes.
Without `WithEventObserver`, `ExecuteCommand` returns `nil` at the
acceptance point. The remaining lifecycle steps -- loading instance
state, calling `HandleCommand()`, OCC journal write, event commit, and
finalization -- all happen asynchronously in background goroutines. The
caller never waits for them.

The acceptance path operates identically regardless of whether
`WithEventObserver` is passed. How the observer is notified is a
separate problem (Open Question 8 in the big-picture plan) and is out
of scope here.

Handler factspace keying is settled by this design. Subsystem ADRs are
free to decide internal structure and compaction policy without
revisiting the keying choice.

---

## Ratified foundations

The command acceptance path depends on three ratified ADRs:

- [0002-rendezvous-hashing-for-workload-assignment.md]
- [0003-optimistic-conflict-resolution.md]
- [0004-ranked-instruction-routing.md]

This document does not restate those ADRs. It only captures
acceptance-path-specific consequences.

### Routing consequences for command acceptance

Command delivery uses the ranked offering protocol from [ADR-4], with
workload assignment based on [ADR-2]. The primary goal is warm-state
affinity so the common path avoids storage reads and minimizes OCC
pressure.

For aggregates, the routing key is derived from the instance ID:

```
owner = rendezvous_hash(uuid5(app_key, instance_id), live_node_uuids)
```

For integrations, the routing key depends on the handler's concurrency
preference. There are three tiers:

- **No configured preference** (the handler never calls
  `SetConcurrencyPreference`): commands execute on whichever node
  receives them. The engine performs no routing of its own;
  distribution is left to the infrastructure (e.g. load balancer).
  This is the default and the simplest path.
- **`MinimizeConcurrency`**: all commands for a handler funnel to one
  node (routing key = handler key).
- **`MaximizeConcurrency`**: the engine actively distributes commands
  across nodes using `rendezvous_hash(command_uuid, live_node_uuids)`.
  This guarantees even spread regardless of how the infrastructure
  routes requests.

The concurrency preference can change across deployments; see the
routing validation section for how this is handled.

---

## Core mechanism: factspace-as-acceptance-record

In rev 1, the acceptance path wrote each command envelope to a per-node
acceptance keyspace — a throwaway record deleted after the handler
completed the command. The handler subsystem then wrote to the entity's
factspace during async execution.

In this revision, the acceptance path writes the command envelope directly
to the entity's factspace journal. The factspace write is the acceptance
record. No separate per-command tracking store exists. The acceptance
write is real work — the envelope reaches the store it needs to be in for
execution — rather than a throwaway that must later be cleaned up.

### Dirty flags

A per-node key-value entry tracks which entities are active on this node.
The key shape differs by handler type:

- Aggregates: `(node, app, handler_key, instance_id)`
- Integrations: `(node, app, handler_key)`

Integrations have no instance dimension because the factspace is
per-handler, not per-instance.

The dirty flag is set when an entity is loaded and cleared when it shuts
down cleanly due to idle timeout. It tracks entity liveness, not
individual commands. A single dirty flag covers any number of pending
commands for that entity.

**Crash safety ordering.** The dirty flag must be durably written before
the factspace write. If a crash occurs between the two, recovery finds an
orphaned dirty flag with no pending command in the factspace — safe to
clean up. The reverse order risks a command in the factspace that recovery
cannot discover.

### Write cost comparison

Per-command acceptance (rev 1):

| Phase           | Writes                                 |
| --------------- | -------------------------------------- |
| Sync acceptance | 1 (acceptance entry)                   |
| Async handling  | 1+ (factspace write for command state) |
| Cleanup         | 1 (delete acceptance entry)            |
| **Total**       | **3+**                                 |

Entity-level acceptance (rev 2):

| Phase           | Writes                                                       |
| --------------- | ------------------------------------------------------------ |
| Sync acceptance | 1 (factspace) + 1 conditional (dirty flag, skipped if warm)  |
| Async handling  | 0 extra (envelope already in factspace)                      |
| Cleanup         | 0 per-command (dirty flag cleared on idle unload, amortized) |
| **Total**       | **1-2**                                                      |

For warm entities (already loaded, dirty flag already set), the
acceptance path is a single factspace journal append — the same cost as
rev 1 but without the deferred cleanup.

---

## Factspace keying

Each handler type has a factspace that records command processing state.
The factspace key determines the granularity of OCC and idempotency
checks, and is therefore an acceptance-path concern.

### Aggregates

Keyed by `(app, handler_key, instance_id)`. Multiple commands may target
the same instance, so the factspace tracks the full instance lifecycle.
OCC contention is per-instance. This is unchanged from rev 1.

### Integrations

Keyed by `(node, app, handler_key)`. Each node has its own factspace
journal for each integration handler. A supervisor goroutine per handler
per node serializes journal I/O and fans out command handling to worker
goroutines.

This replaces the rev 1 design where each command had its own factspace
keyed `(app, handler_key, command_uuid)`. The per-command design was
incompatible with entity-level dirty tracking — a dirty flag per handler
cannot identify which per-command factspaces are outstanding. Node-scoped
per-handler keying makes entity-level tracking work uniformly for both
handler types.

The concurrency preference is purely a routing concern:

- **No configured preference**: commands execute on whichever node
  receives them. Each node's supervisor manages its share
  independently.
- **`MinimizeConcurrency`**: all commands route to one node via
  `rendezvous_hash(uuid5(app_key, handler_key), live_node_uuids)`.
  That node's supervisor handles all commands serially.
- **`MaximizeConcurrency`**: the engine distributes commands across
  nodes using `rendezvous_hash(command_uuid, live_node_uuids)`. Each
  node's supervisor manages its share independently.

The factspace key does not include any routing information, so a command
accepted under one concurrency preference and executed after a change
works correctly. The factspace structure is identical regardless of which
preference is active.

### Factspace internal structure

The internal structure of each factspace — what records are stored, how
completion is signaled, how compaction works — is a handler-subsystem
concern and not decided here. Those details belong to the aggregate and
integration subsystem ADRs.

---

## Acceptance path

**On the source node (synchronous):**

1. Determine the handler for this command type using the application's
   routing configuration. For aggregates, call
   `RouteCommandToInstance()` to determine the instance ID.

2. Identify the handler node using the ranked routing protocol from
   [ADR-4]. The routing key is derived from the instance ID
   (aggregates), the handler key (`MinimizeConcurrency` integrations),
   the command UUID (`MaximizeConcurrency` integrations), or is absent
   (no-preference integrations, handled locally). This is pure
   in-memory routing with no disk I/O.

**On the handler node (synchronous):**

3. Dispatch the command to the handler subsystem (in-memory handoff to
   the handler's supervisor goroutine/channel).

4. If the entity is not already loaded, the supervisor writes the
   dirty flag:
   - For aggregates: `(node, app, handler_key, instance_id)`
   - For integrations: `(node, app, handler_key)`

5. The supervisor appends the command envelope to the entity's
   factspace journal:
   - For aggregates: `(app, handler_key, instance_id)`
   - For integrations: `(node, app, handler_key)`

6. Return `nil` to the source node (and on to the original caller).
   This is the formal acceptance point.

**What is async.** After returning `nil`, the handler node runs the
remaining command lifecycle steps (load state, call `HandleCommand`, OCC
write, event commit, finalization) entirely in background goroutines.
These do not block the caller.

---

## Recovery

Recovery uses the dirty flags to discover entities with potentially
unfinished work, then opens each entity's factspace to find unprocessed
commands.

**Routing validation invariant.** Application routing rules can change
across deployments. Every stored command goes through routing validation
before execution. Since configuration changes require a new binary and
therefore a restart, every entity is validated on startup. There is no
window where a command executes under stale configuration.

### Restart

On startup, each node iterates its own dirty flags
`(self, app, *, *)` and opens each entity's factspace. For each
factspace:

1. Scan for unprocessed commands (commands that were appended during
   acceptance but not yet completed by the handler subsystem).
2. Validate routing against the current application configuration.
   - If the command still routes to this entity: execute it.
   - If routing has changed: hand off the command (see below).
   - If unroutable: move to the quarantine.
3. If the factspace contains no unprocessed commands, clean up the
   dirty flag.

### Dead-node adoption

When a node is detected as permanently failed, a surviving node
iterates the dead node's dirty flags `(dead_node, app, *, *)` and
performs the same factspace enumeration, completion check, and routing
validation. The dead node's UUID serves as the partition key;
[self-affinity][ADR-2] enables any surviving node to locate that
partition without coordination.

### Entity-to-entity handoff

When recovery (or a live entity) finds a pending command that no longer
routes to it under the current application configuration, it hands the
command off to the correct entity:

1. Determine the new target entity using current routing rules.
2. Write the dirty flag for the target entity (if not already set).
3. Append the command envelope to the target entity's factspace.

This reuses the same acceptance mechanism — dirty flag, then factspace
write — so no separate rerouting logic is needed. The handoff is
entity-to-entity: the holder writes to the recipient's factspace
directly.

Rerouting is triggered when the command type is now handled by a
different handler, or when an aggregate handler's
`RouteCommandToInstance()` returns a different instance ID across
deployments. Concurrency preference changes (e.g. a handler switching
between `MinimizeConcurrency` and `MaximizeConcurrency`) do not trigger
rerouting — preferences are not guarantees, and a command executed on
a "wrong" node is still correct.

### Unroutable commands

If a command's handler no longer exists in the current application
configuration and no other handler accepts that command type, the
command is moved to the quarantine. Trickle-back on future
restart re-checks routing, so the command recovers if the handler is
re-added in a future deployment.

---

## Idempotent command submission (separate concern)

The base system described above makes every accepted command durable
regardless of how it was submitted. It does not distinguish between
commands with or without an idempotency key.

The idempotent command submission layer — the idempotency journal, the
`ConflictError` retry path, the caller recovery contract from
[Dogma ADR-31] — sits in front of this base system. It adds
cluster-wide deduplication using the application-supplied idempotency
key. After deduplication, the command enters the same base acceptance
path described above.

This separation means:

- The base system answers: "how do accepted commands survive crashes?"
- The idempotency layer answers: "how does the engine deduplicate
  submissions?"
- The dirty flag mechanism provides a free bonus for keyed commands:
  if the command reached an entity's factspace before a crash, the
  base system recovers it before the application retries.

The idempotency layer will be described in its own ADR.

---

## Evolution from rev 1

### What changed

1. **Per-command acceptance keyspace eliminated.** The per-node KV store
   keyed `(node, app, command_uuid)` no longer exists. The factspace
   write replaces it.

2. **Dirty flags introduced.** A per-node KV tracks entity liveness
   for recovery, keyed `(node, app, handler_key, instance_id)` for
   aggregates and `(node, app, handler_key)` for integrations. Set on
   load, cleared on clean idle unload.

3. **Integration factspace rekeyed.** Changed from
   `(app, handler_key, command_uuid)` (per-command) to
   `(node, app, handler_key)` (per-handler, node-scoped). A supervisor
   goroutine per handler serializes journal I/O.

4. **Keyed command content extracted.** Moved to a separate concern
   (future ADR-7). The base system is uniform for all commands.

5. **Recovery model changed.** Recovery iterates active entities (dirty
   flags) rather than individual commands (acceptance entries).
   Entity-to-entity handoff replaces inline rerouting.

### Why

- **Fewer total writes per command.** The acceptance entry was a
  throwaway record requiring creation and deletion. The factspace
  write is real work — the envelope reaches its destination
  immediately.
- **Smaller recovery index.** Dirty flags count active entities, not
  individual commands. A busy aggregate instance with N pending
  commands produces 1 dirty flag instead of N acceptance entries.
- **Single source of truth.** The factspace is both the acceptance
  record and the execution state. No split between "where the
  command was accepted" and "where the command is processed."
- **Natural alignment with handler lifecycle.** The dirty flag tracks
  entity liveness, which the handler subsystem already cares about.

### What from rev 1 is now a dismissed alternative

- **Per-command acceptance keyspace.** Introduces a throwaway write and
  cleanup delete that the factspace approach avoids. Total writes per
  command are higher. Recovery index counts individual commands rather
  than active entities.

- **Per-command integration factspace.** Keying by
  `(app, handler_key, command_uuid)` is incompatible with entity-level
  dirty tracking. It also prevents the supervisor pattern, which is
  needed to serialize journal I/O for the per-handler factspace.

### What from rev 1 remains

- **Cluster-wide acceptance store — dismissed.** Recovery cost
  proportional to entire cluster's workload. Still dismissed for the
  same reasons.

- **Two synchronous writes for unkeyed commands — dismissed.** The
  per-node acceptance entry + per-command journal design from the
  earliest iteration. Still dismissed, and the new design further
  strengthens the argument (the factspace write alone is sufficient).

- **Set-backed acceptance store — dismissed.** Still relevant as a
  dismissed alternative for any recovery index that needs to carry
  data alongside keys.

- **Cluster-wide integration factspace `(app, handler_key)` —
  dismissed.** OCC contention between concurrent nodes writing to the
  same journal under `MaximizeConcurrency`. Node-scoped keying avoids
  this.

---

## Phases needed on the synchronous critical path

### Phase 2 (persistence options only)

The persistence store options — `WithJournals`, `WithKeyspaces`,
`WithSets` — introduced in Phase 2 are required so the engine can open
the dirty flag keyspace and entity factspace journals. The rest of
Phase 2 (heartbeat keyspace, live node set) is NOT on the synchronous
path and can be deferred. A single-node cluster is not a special case —
it is indistinguishable from a multi-node cluster with all other nodes
dead — so the heartbeat and live node set machinery is always needed,
just not on the acceptance critical path.

### Phase 3 — acceptance portion

Write the dirty flag (if needed), append the command envelope to the
entity's factspace, dispatch to the handler subsystem, return `nil`.

### Phase 10 — partial wiring

Connect the persistence stores to the acceptance logic and replace
`noopExecutor` with a real executor:

1. Resolve node identity (`WithNodeID` / `DOGMA_NODE_ID` / random
   UUID).
2. Resolve persistence stores from options (or environment fallback).
3. Wire the acceptance path (dirty flag keyspace opener + factspace
   journal openers).
4. Signal readiness by storing the real executor into
   `executor.future` (currently stores `noopExecutor{}`), which
   unblocks any callers already waiting in
   `executor.ExecuteCommand`.

The pre-startup blocking behavior of `executor.future.Wait` is already
implemented and does not change. Only the value stored by `Run()`
changes.

---

## Phases NOT needed for this goal

| Phase                             | Why deferred                                          |
| --------------------------------- | ----------------------------------------------------- |
| Phase 2 heartbeat / live node set | Not on the synchronous acceptance path                |
| Phase 3 background execution      | Async; does not block return                          |
| Phase 4 integration subsystem     | Aggregate-only is sufficient for the first milestone  |
| Phase 5 event stream              | Async; only needed by background Phase 3 event commit |
| Phase 6 process subsystem         | Async consumer of the event stream                    |
| Phase 7 projection subsystem      | Async consumer of the event stream                    |
| Phase 8 quarantine                | Failure path; deferred                                |
| Phase 9 inter-node gRPC           | Single-node first                                     |
| `WithEventObserver` V1/V2/V3      | Separate problem; Open Question 8                     |

---

## Storage summary

This table covers the stores used by the acceptance path. The internal
structure of each factspace is a handler-subsystem concern.

| Store                 | Key                                     | Type    | Used by        | Lifetime                     |
| --------------------- | --------------------------------------- | ------- | -------------- | ---------------------------- |
| Dirty flags (agg)     | `(node, app, handler_key, instance_id)` | KV      | Aggregates     | Set on load, cleared on idle |
| Dirty flags (int)     | `(node, app, handler_key)`              | KV      | Integrations   | Set on load, cleared on idle |
| Aggregate factspace   | `(app, handler_key, instance_id)`       | Journal | Aggregates     | Permanent (compacted)        |
| Integration factspace | `(node, app, handler_key)`              | Journal | Integrations   | Active while has work        |
| Idempotency journal   | `(app, idempotency_key)`                | Journal | Keyed commands | Until completion             |

The idempotency journal is listed for completeness but belongs to the
idempotent command submission layer (separate ADR).

<!-- references -->

[0002-rendezvous-hashing-for-workload-assignment.md]: ../adr/0002-rendezvous-hashing-for-workload-assignment.md
[0003-optimistic-conflict-resolution.md]: ../adr/0003-optimistic-conflict-resolution.md
[0004-ranked-instruction-routing.md]: ../adr/0004-ranked-instruction-routing.md
[ADR-2]: ../adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-4]: ../adr/0004-ranked-instruction-routing.md
[Dogma ADR-31]: https://github.com/dogmatiq/dogma/blob/main/docs/adr/0031-require-retries-for-idempotency-keyed-commands.md
[command-acceptance-path-rev1.md]: command-acceptance-path-rev1.md
[self-affinity]: ../glossary.md#self-affinity
