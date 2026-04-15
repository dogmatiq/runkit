# 5. Homogeneous cluster nodes

Date: 2026-04-06

## Status

Accepted

- References [2. Rendezvous hashing for workload assignment][ADR-2]
- References [3. Optimistic conflict resolution][ADR-3]
- References [4. Ranked instruction routing][ADR-4]
- Referenced by [6. Durable command executor][ADR-6]
- Referenced by [7. Node heartbeat][ADR-7]
- Referenced by [10. Event stream model][ADR-10]

## Context

`runkit` needs a cluster model: a description of how nodes relate to each other,
what each node is responsible for, and how they share work and state.

[Verity], the predecessor engine, ran each application on a single node. All
active handler state was held in memory on that process, which was both the
single point of failure and the single limit on capacity. Correctness depended
on ensuring that no two processes operated on the same state simultaneously,
which required a cluster-wide lock — a shared dependency that became a single
point of failure in its own right.

The design also constrained persistence. If multiple pieces of state had to be
updated in the same transaction — a message queue entry, an aggregate journal
record, and an event store append — then all of that state had to live in the
same database. Scaling storage meant scaling one database, not distributing data
independently.

[`persistencekit`] was designed as an external constraint to prevent these
problems from recurring in `runkit`. It exposes three isolated, backend-agnostic
primitives: append-only journals with positional [OCC][ADR-3] on writes,
key-value keyspaces, and sets. There is deliberately no cross-store transaction
primitive. Any engine built on `persistencekit` is forced to decompose state
into independent stores, because the abstraction provides no other option.

The cluster model must be compatible with these constraints and, where possible,
exploit them. [ADR-2], [ADR-3], and [ADR-4] were written assuming this model was
already in place; this ADR formalizes that assumption.

## Decision

The engine will operate as a cluster of homogeneous peer nodes. Every node runs
all engine components. No node has a specialized role, and no node is designated
as master or coordinator.

For example, any node may receive a command from application code. That node
uses [ranked instruction routing][ADR-4] to forward the command to a target
node for execution. The application is not blocked waiting for execution to
complete. This pattern — accept work, then route it to a target node — applies
to all workloads, not just commands. `runkit` handles all work asynchronously.

State is decomposed into isolated `persistencekit` stores. Work is partitioned
across nodes so that each node handles a subset of the total workload, with
[rendezvous hashing][ADR-2] determining the assignment. Any node can read from
or write to any store. When two nodes write to the same store concurrently,
[OCC][ADR-3] resolves the conflict.

Although any node _can_ access any store, performance is best when the same node
handles the same store repeatedly. A warm in-memory cache reduces round-trips to
the backing storage. [Ranked instruction routing][ADR-4] optimizes for this by
preferring the same target node for a given piece of state, while still allowing
any peer to take over if that node is unavailable.

### Dismissed alternatives

We considered two alternatives:

- **Specialized node roles.** Separating nodes into command-receiving nodes,
  execution nodes, and storage nodes would allow each tier to scale
  independently. However, it reintroduces topology configuration and creates
  dependencies between node types. If only certain nodes can access certain
  stores, a dead node's work cannot be resumed by an arbitrary peer.

- **Raft-style consensus cluster.** Nodes could replicate state among themselves
  and agree on writes via a consensus protocol. This provides strong consistency
  at the engine layer but duplicates work that the persistence backends already
  handle. It also couples the engine to a fixed quorum size, complicating
  scaling. We chose to defer consistency and redundancy to the persistence layer
  instead.

## Consequences

Capacity scales horizontally. Adding a node redistributes partitions via
[rendezvous hashing][ADR-2], increasing both the memory available for warm
state and the throughput for processing. Removing a node triggers the same
reassignment, with minimal disruption.

When a node dies, its peers can take over its work. Because any node can access
any store and all engine components run on every node, a peer can enumerate the
failed node's in-progress work and resume it without any special recovery
protocol.

All work is handled asynchronously. Acceptance and execution are always
decoupled, and execution may proceed on a different node.

The design preserves `persistencekit`'s isolation constraint: no cross-store
transactions appear anywhere.

<!-- references -->

[ADR-2]: 0002-rendezvous-hashing-for-workload-assignment.md
[ADR-3]: 0003-optimistic-conflict-resolution.md
[ADR-4]: 0004-ranked-instruction-routing.md
[ADR-6]: 0006-durable-command-executor.md
[ADR-7]: 0007-node-heartbeat.md
[ADR-10]: 0010-event-stream-model.md
[`persistencekit`]: https://pkg.go.dev/github.com/dogmatiq/persistencekit
[Verity]: https://github.com/dogmatiq/verity
