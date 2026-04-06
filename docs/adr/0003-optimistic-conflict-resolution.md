# 3. Optimistic conflict resolution

Date: 2026-04-05

## Status

Accepted

- References [2. Rendezvous hashing for workload assignment][ADR-2]
- Referenced by [5. Homogeneous cluster nodes][ADR-5]

## Context

A multi-node engine needs a way to decide which node handles a given piece of
work and what happens when two nodes disagree.

There are two broad approaches. The first is coordinated ownership: a node
acquires a lock or lease on a workload before it begins, other nodes respect the
lock, and a protocol governs handoff when ownership changes. The second is
independent routing: every node computes ownership locally from shared inputs,
acts on the result without waiting for agreement, and conflicts are detected and
resolved after the fact.

Coordinated ownership aims to prevent conflicts from occurring, at the cost of
coordination infrastructure and new failure modes (expired leases, stale lock
holders). Independent routing accepts that conflicts will occasionally occur and
relies on the storage layer to make them harmless, at the cost of retries during
transitions.

[Rendezvous hashing][ADR-2] already gives us independent routing. Each node
computes workload-to-node assignment from the workload and the current candidate
set, with no shared state or negotiation. Under stable membership, every node
agrees on ownership and no conflicts arise. During membership transitions — a
node joining, leaving, or restarting — the candidate set changes and two nodes
may transiently believe they own the same workload.

The question is what to do about that transient window: add a coordination layer
to prevent it, or handle it optimistically at the storage layer.

## Decision

We will pair independent routing with optimistic concurrency control (OCC) at
the storage layer. We will not introduce distributed locks, leases, fencing
tokens, or ownership handoff protocols.

Each node routes work independently based on rendezvous hashing. When two nodes
attempt conflicting writes to the same piece of state, the storage layer rejects
all but one. The losing node detects the conflict, reloads the current state,
and retries. There is no silent corruption path: every conflict is visible to
the loser.

This requires three properties of any storage mechanism used for
mutable state:

1. **Conflict detection.** Concurrent writes to the same logical state must not
   silently merge or overwrite. Exactly one writer succeeds; all others receive
   an explicit rejection.
2. **Source of truth.** After a conflict, the losing node must be able to read
   the current state as written by the winner. The storage layer is the
   authority, not any node's in-memory cache.
3. **No bypass.** Every state mutation must go through a storage operation that
   enforces conflict detection. There are no side channels.

### Dismissed alternatives

- **Coordinated ownership.** A node acquires a lock or lease before processing a
  workload. Other nodes wait or back off. Ownership transfer follows a protocol:
  the old owner releases, the new owner acquires, and processing resumes. This
  eliminates the transient dual-ownership window entirely, but it introduces a
  coordination service as a dependency, adds new failure modes (lock expiry
  during processing, stale lock holders), and requires a fencing mechanism to
  prevent a node that outlives its lease from writing stale results. The system
  becomes correct only if the coordination service is correct — a dependency we
  prefer not to take.

- **Fencing tokens.** A coordination service issues monotonically increasing
  tokens; the storage layer rejects writes bearing a stale token. This is the
  standard solution when a lock holder outlives its lease. It solves a real
  problem, but only one that arises when you have locks in the first place.
  Without locks, there is nothing to fence.

## Consequences

We need no coordination infrastructure. Nodes share a persistence backend and a
way to discover the candidate set. Nothing else is required for correctness.

The two decisions — rendezvous hashing ([ADR-2]) and optimistic conflict
resolution — form a pair. Rendezvous hashing minimizes conflicts by producing
stable, independent routing. OCC makes the remaining conflicts (during
membership transitions) safe. Neither is sufficient alone: without stable
routing, every write would conflict; without OCC, a single routing disagreement
would corrupt state.

In-process serialisation — at most one goroutine per workload on a given node —
reduces the frequency of conflicts under stable routing to effectively zero. But
serialisation is a liveness optimisation, not a correctness mechanism. If it
fails (a bug, a race, a missed reassignment), OCC still prevents corruption. We
are free to tune, relax, or remove serialisation without affecting correctness —
only liveness and efficiency are at stake.

The cost is paid during membership transitions: conflicting writes are wasted
work. The losing node must reload state and re-execute. OCC catches the state
write conflict, but it cannot undo side effects that the losing node already
performed (e.g., an HTTP call made by an integration handler). Preventing
duplicate side effects is a separate concern — idempotency — not addressed by
this decision.

This cost is acceptable because transitions are brief and infrequent under
stable operations.

<!-- references -->

[ADR-2]: 0002-rendezvous-hashing-for-workload-assignment.md
[ADR-5]: 0005-homogeneous-cluster-nodes.md
