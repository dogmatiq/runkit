<!-- Shelved 2026-04-05: This document is too vague without more ADRs to
     anchor it. Revisit once the foundational design decisions have been
     written. -->

# Articles of Faith

> "Do you know about voodoo? No real doctrine of faith to speak of - more an
> arrangement of superstitions."
>
> -- Loki, [Dogma](https://www.imdb.com/title/tt0120655/) (1999)

In any system built from independent subsystems, the hardest properties to
reason about are the ones that no single subsystem owns. They emerge from
assumptions each subsystem makes about the others. Those assumptions are not
encoded anywhere in the codebase, and no one subsystem's tests can verify them.
A change to one subsystem can silently violate an assumption in another, causing
anything from retry storms to data loss, depending on which assumption breaks.

This document exists because those relationships are genuinely difficult to keep
in your head. It names the assumptions, maps the dependencies between them, and
makes them visible, laying out what must be true for the system to behave
correctly.

This is a living document. Many of the decisions it references have not been
made yet. Where a decision is pending, this document states what that decision
must satisfy to preserve the guarantee it supports. As [ADRs] are written, the
pending entries should be replaced with links.

[ADRs]: adr/README.md

## How to read this document

Each section below describes one guarantee the system must uphold. The
individual mechanisms are enforced by code, but their correct combination is
not. Getting that right requires faith that every subsystem involved will keep
its end of a bargain.

Each article has the same structure:

- **Guarantee** -- a one-sentence statement of the property.
- **Scenario** -- a narrative showing what must hold and why, without
  prescribing mechanisms that have not yet been decided.
- **Supporting decisions** -- the individual design decisions that make the
  scenario work. Each is either decided (with an ADR link) or pending (with
  the constraints that the future decision must satisfy).
- **What breaks** -- what specific failure occurs if each supporting decision
  is violated.
- **Testable invariants** -- properties we can enforce with tests, and whether
  those tests belong inside a single subsystem or across subsystem boundaries.

### A note on terminology

The Dogma framework defines "command" and "event" as application-level message
types. Inside the engine, subsystems exchange their own messages that are not
Dogma commands or events. To avoid confusion, this document uses distinct terms
for engine-internal messaging:

- **Directive** -- an imperative engine-internal message ("do this"). For
  example, a request to append events to a stream partition.
- **Confirmation** -- a point-to-point reply to a directive, carrying either a
  positive or negative outcome. For example, the result of an append attempt,
  which may succeed or fail with a conflict.
- **Signal** -- an informational engine-internal message ("this happened"). For
  example, a notification that a partition has been reassigned.

---

## 1. Durability

**Guarantee:** once the engine has accepted a piece of work, that work is never
silently lost, even if every node in the cluster restarts.

### Scenario

The application executes a command via [`CommandExecutor`]. The engine accepts it, begins processing, and
crashes partway through. On restart, the work must be recoverable.

This requires that at least one durable artifact records the directive at every
point after acceptance. The specific mechanisms -- how acceptance is recorded,
how incomplete work is tracked, how persistently failing work is preserved --
are individual decisions. What matters here is that they form an unbroken chain:
if any one is missing, there is a window where accepted work can be silently
lost.

A crash can also occur after processing completes but before cleanup finishes.
In that case, the directive reappears on restart. This is safe only if
re-execution is detectable or idempotent (see [Idempotency](#3-idempotency)).

### Supporting decisions

| Decision              | Status  | Contribution                                                                                                                 |
| --------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Acceptance journaling | Pending | Must record the full directive before any processing begins. Must fail atomically: no partial acceptance.                    |
| Backlog design        | Pending | Must be queryable on restart to discover incomplete work. Must survive node restarts.                                        |
| Poison backlog        | Pending | When a directive fails repeatedly, it must be preserved indefinitely until explicitly retried. Must not silently discard.    |
| Completion cleanup    | Pending | Must tolerate a crash between "work done" and "cleanup done." The idempotency chain must prevent double-execution on replay. |

### What breaks

- Remove acceptance journaling: a crash after the caller is told "accepted" but
  before processing begins causes silent work loss.
- Remove the backlog: a crash mid-processing loses the directive because nothing
  tracks that it was in progress.
- Remove the poison backlog: a persistently failing directive is dropped after
  exhausting retries, with no way to recover it.
- Remove cleanup crash tolerance: a crash between completion and cleanup causes
  re-execution on restart, which corrupts state unless idempotency holds.

### Testable invariants

- **Per-subsystem:** accept a directive, crash at each step, restart, verify the
  directive is still discoverable and eventually processed.
- **Cross-subsystem:** verify that the backlog, poison backlog, and journal
  agree on the set of in-flight directives after an unclean restart.

---

## 2. Correctness

**Guarantee:** concurrent execution of the same workload by multiple nodes never
produces corrupted state.

### Scenario

Nodes A and B both believe they own aggregate instance X. This can happen during
a membership transition: a node joins or leaves the cluster, and the rendezvous
hashing output shifts. For a brief window, both nodes may route directives for X
to themselves.

1. Node A loads instance X's state from the journal. The journal is at position
   N. Node A remembers N as the expected next write position.
2. Node B independently loads the same state. It also sees position N.
3. Node A executes the directive and attempts to append the result to the
   journal at position N. The append succeeds.
4. Node B executes the same (or a different) directive and attempts to append at
   position N. The append fails with a conflict, because position N is already
   occupied.
5. Node B discards its stale state, reloads from the journal (now at position
   N+1), and retries.

No corruption occurs. The journal's position-based append is the sole arbiter of
which write wins. The loser detects the conflict. Recovery depends on the retry
logic correctly reloading state and re-executing.

Under stable routing, this scenario does not arise. Rendezvous hashing
consistently assigns X to one node, and that node runs a single goroutine for X.
The OCC mechanism exists but is never exercised. Serialisation is the liveness
optimisation; OCC is the correctness backstop.

### Supporting decisions

| Decision                       | Status  | Contribution                                                                                                                                  |
| ------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| [Rendezvous hashing][ADR-0002] | Decided | Produces stable workload-to-node assignment, minimizing the window where two nodes compete for the same workload.                             |
| OCC via journal position       | Pending | Journal append must reject concurrent writes at the same position. The conflict must be detectable (a distinct error), not silent corruption. |
| In-process serialisation       | Pending | Must guarantee at most one goroutine per active instance on a given node. Must not break under load or backpressure.                          |
| Instance state loading         | Pending | After a conflict, the node must reload state from the journal (the source of truth). It must never retry against a stale in-memory cache.     |

### What breaks

- Remove OCC: two concurrent writes at the same position both succeed. The
  journal contains interleaved state from two executions. State is corrupted.
- Remove serialisation: multiple goroutines on the same node race to execute
  the same instance. OCC still prevents corruption, but every execution
  contends, causing heavy retry overhead and wasted work.
- Remove stable routing (rendezvous hashing): every directive triggers the OCC
  race. The system is still correct but practically unusable because of
  constant conflicts.
- Reload from stale cache after conflict: the retrying node applies its
  directive on top of state that was already overwritten. The journal rejects
  the write (OCC still fires), but the node is stuck in an infinite retry loop
  because it never sees the current state.

### Testable invariants

- **Per-subsystem:** two goroutines racing to append at the same journal
  position; exactly one succeeds, the other gets a conflict error.
- **Cross-subsystem:** simulate a membership transition; verify that the
  instance ends up in a consistent state regardless of which node "wins."
  The journal must reflect a linear sequence of valid state transitions.

---

## 3. Idempotency

**Guarantee:** a directive is never executed more than once, even if a node
crashes and replays it from the backlog.

### Scenario

Node A accepts a directive (journal append at position 0 succeeds). It begins
executing the directive against aggregate instance X but crashes before marking
the work as complete. On restart:

1. The backlog still contains the directive. The node re-dispatches it.
2. At the acceptance layer, the node attempts to append the directive journal at
   position 0 again. The append fails because position 0 is already occupied.
   This proves the directive was already accepted. The node skips acceptance and
   proceeds to execution.
3. At the execution layer:
   - **For aggregates:** the node loads instance X from the journal. If the
     journal already reflects the outcome of this directive, the OCC position
     has advanced past the expected position. The node detects this and skips
     re-execution.
   - **For integrations:** the node checks an idempotency store keyed by the
     directive's UUID. If the entry exists, the work was already completed. The
     node skips re-execution.
4. The node completes the cleanup it missed before the crash.

For this to hold, each layer must correctly detect prior execution. The dedup mechanisms are
layered: journal position dedup at acceptance, OCC or idempotency store at
execution.

### Supporting decisions

| Decision                      | Status  | Contribution                                                                                                                                                                             |
| ----------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal dedup at acceptance   | Pending | Append at position 0 must fail if the directive was already recorded. This is the same journal used for durability, so the mechanism is shared.                                          |
| Instance journal OCC          | Pending | Same mechanism as the correctness chain. A replayed directive whose outcome is already in the journal is detected by the advanced position.                                              |
| Integration idempotency store | Pending | Must survive node restarts. Must have configurable retention. Must not be bypassed during integration preference transitions (MaximizeConcurrency to MinimizeConcurrency or vice versa). |

### What breaks

- Remove journal dedup: a replayed directive is accepted a second time, creating
  a duplicate entry in the backlog. The directive is dispatched twice.
- Remove instance journal OCC: a replayed directive is applied to the aggregate
  a second time. State reflects the same transition applied twice.
- Remove integration idempotency store: a replayed integration directive
  executes the handler a second time. Side effects (HTTP calls, emails, etc.)
  are repeated.

### Testable invariants

- **Per-subsystem:** accept a directive, crash mid-execution, restart, verify
  the directive completes exactly once (state reflects one application, not
  two).
- **Cross-subsystem:** submit the same directive concurrently from two nodes
  during a membership transition. Verify exactly one execution completes and
  the other is deduplicated or rejected.

---

## 4. Causality

**Guarantee:** signals produced by the same aggregate instance are observed in
the order they were produced. Signals from different instances carry no ordering
guarantee.

### Scenario: intra-instance ordering

Aggregate instance X handles directive D1, producing signal S1. Later, it
handles directive D2, producing signal S2.

1. On its first signal-producing execution, instance X was permanently bound to
   stream partition P. This binding is recorded in the instance journal and
   never changes.
2. S1 is appended to partition P at offset N.
3. S2 is appended to partition P at offset N+1.
4. A process handler consuming partition P reads sequentially. It sees S1 at
   offset N, then S2 at offset N+1. The causal order is preserved.

This works because of three cooperating properties: the binding is permanent (so
both signals land on the same partition), the stream is append-only (so offsets
are monotonically increasing), and exactly one node owns partition P at any time
(so no concurrent appends can interleave).

### Scenario: cross-instance non-ordering (the deliberate boundary)

Aggregate instances X and Y each produce signals. X produces S1, then Y
produces S2 a moment later. A process handler consuming both partitions may
observe S2 before S1.

The system provides no causal ordering across instance boundaries. Instances X
and Y may be bound to different partitions, owned by different nodes, with no
shared clock or coordination. We chose not to provide cross-instance ordering
because it would require coordination that conflicts with the system's
independence model.

### Supporting decisions

| Decision                       | Status  | Contribution                                                                                                                                                                 |
| ------------------------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Instance-stream binding        | Pending | Must be permanent: set atomically on first signal production, never changed thereafter. Must survive restarts.                                                               |
| Stream append ordering         | Pending | Within a partition, appends must be ordered. Concurrent appends from multiple writers must be prevented, either by rejecting them or by guaranteeing single-owner semantics. |
| Partition ownership            | Pending | Must assign exactly one writing node per partition at any time. Uses the same [rendezvous hashing][ADR-0002] mechanism applied to partition workloads.                       |
| [Rendezvous hashing][ADR-0002] | Decided | Provides deterministic partition-to-node assignment, ensuring single-writer ownership.                                                                                       |

### What breaks

- Change the binding: an instance's signals land on different partitions at
  different times. A consumer reading one partition sees an incomplete history.
  Events appear out of order or are missing entirely.
- Allow concurrent partition writers: two nodes append to the same partition
  simultaneously. Signals from different instances interleave at arbitrary
  offsets. Intra-instance ordering is destroyed.
- Remove single-owner partition semantics: same effect as concurrent writers.
  No mechanism prevents interleaving.

### Testable invariants

- **Per-subsystem:** produce N signals from one instance. Verify a consumer
  observes them in production order, with no gaps or duplicates.
- **Per-subsystem:** produce signals from two different instances. Verify the
  system tolerates arbitrary observation order without corruption.
- **Cross-subsystem:** reassign a partition to a new owner mid-stream. Verify
  that the new owner continues appending in order and does not duplicate or
  lose signals written by the previous owner.

---

## 5. Liveness

**Guarantee:** work always makes progress. If a node goes offline, its workloads
are reassigned and eventually processed by surviving nodes.

### Scenario

Node B crashes. It does not restart immediately.

1. Node B's heartbeat entry has a TTL. After the TTL expires, other nodes no
   longer see B in the candidate set.
2. On their next membership refresh, the surviving nodes recompute rendezvous
   hashing with B removed. Workloads previously assigned to B are now assigned
   to the surviving nodes.
3. Only B's workloads move: the minimal disruption property of rendezvous
   hashing means that workloads owned by surviving nodes are unaffected.
4. Node B had a self-affinity workload (its own UUID, used for its private
   partition). That workload is reassigned to another node. The partition is not
   orphaned.
5. Later, node B restarts with the same UUID (via `WithNodeID`). It re-enters
   the candidate set. Rendezvous hashing is recomputed, and B reclaims the same
   workloads it owned before the crash.

### Supporting decisions

| Decision                       | Status  | Contribution                                                                                                                                                                                                    |
| ------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Rendezvous hashing][ADR-0002] | Decided | Deterministic reassignment with minimal disruption. Self-affinity ensures private partitions are inherited, not orphaned.                                                                                       |
| Heartbeat mechanism            | Pending | Must have a bounded TTL. Must be writable by any node and readable by all nodes.                                                                                                                                |
| Membership refresh             | Pending | Must be frequent enough that stale membership does not cause prolonged dual-ownership (tension with [Correctness](#2-correctness): too-frequent refreshes cause unnecessary OCC contention during transitions). |
| Work loop resumption           | Pending | Each subsystem must be able to pick up reassigned workloads without requiring the previous owner to hand off explicitly. The previous owner may be permanently offline.                                         |

### What breaks

- Remove the heartbeat: a dead node is never removed from the candidate set. Its
  workloads are assigned to a node that will never process them. Work stops.
- Increase TTL excessively: same effect, delayed. Workloads are stuck until the
  TTL expires. The system appears to hang.
- Remove work loop resumption: workloads are reassigned in the routing layer but
  no subsystem picks them up. The routing says "node A owns workload W" but node
  A never starts processing it.
- Remove self-affinity inheritance: a dead node's private partition is not
  reassigned. Any work queued in that partition is stuck permanently.

### Testable invariants

- **Per-subsystem:** remove a node from the candidate set. Verify that every
  workload previously assigned to it is reassigned and eventually processed.
- **Cross-subsystem:** kill a node, wait for TTL, verify that all of its
  workloads are picked up by surviving nodes and completed.
- **Cross-subsystem:** restart a node with the same UUID. Verify it reclaims its
  original workloads without duplicating work that was reassigned during its
  absence.

---

## Cross-chain dependencies

Some mechanisms are shared by multiple articles. Changing one of these mechanisms
can break several guarantees simultaneously. This section maps the shared
dependencies so that any future change can be evaluated against all affected
articles.

| Mechanism                                    | Articles                                                                          | Role in each                                                                                                                                                                                                       |
| -------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal append at position 0                 | [Durability](#1-durability), [Idempotency](#3-idempotency)                        | Durability: the acceptance point that makes a directive recoverable. Idempotency: the dedup check that prevents double-acceptance. These two rely on the same journal write.                                       |
| Instance journal OCC (position-based append) | [Correctness](#2-correctness), [Idempotency](#3-idempotency)                      | Correctness: the conflict detection that prevents concurrent corruption. Idempotency: the double-apply detection that prevents re-execution after a crash.                                                         |
| [Rendezvous hashing][ADR-0002]               | [Correctness](#2-correctness), [Causality](#4-causality), [Liveness](#5-liveness) | Correctness: stable routing minimizes OCC contention. Causality: partition ownership ensures single-writer semantics. Liveness: deterministic reassignment when nodes leave or join.                               |
| In-process serialisation                     | [Correctness](#2-correctness), [Liveness](#5-liveness)                            | Correctness: eliminates OCC contention under stable routing. Liveness: a stuck goroutine must not prevent reassignment. These two are in tension: serialisation helps correctness but must not mask liveness bugs. |

---

## Related documents

- [Architecture Decision Records](adr/README.md)
- [Big picture architecture plan](agent-plans/000-big-picture.md)
- [Glossary](glossary.md)

<!-- references -->

[ADR-0002]: adr/0002-rendezvous-hashing-for-workload-assignment.md
[`CommandExecutor`]: https://pkg.go.dev/github.com/dogmatiq/dogma#CommandExecutor
