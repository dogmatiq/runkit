# 4. Ranked instruction routing

Date: 2026-04-06

## Status

Accepted

- References [2. Rendezvous hashing for workload assignment][ADR-2]
- Referenced by [5. Homogeneous cluster nodes][ADR-5]
- Referenced by [6. Durable command executor][ADR-6]

## Context

[Rendezvous hashing][ADR-2] gives us a way to score candidate nodes against a
routing key and select a preferred owner for a piece of work. Multiple nodes
with the same inputs independently reach the same conclusion, without
coordination.

Scoring alone is not a routing procedure. When a source node has an
[instruction] to deliver, it needs a protocol for finding a destination node
that will accept responsibility. Under stable membership every node agrees on
the ranking, so the top-scored candidate is the obvious choice. During
membership transitions, however, nodes may have different views of the candidate
set. The top-scored node from the source's perspective may not consider itself
the owner, and may decline.

We need a procedure that handles this disagreement gracefully: one that finds a
willing destination node without coordination, without disk I/O on the source,
and without dropping work.

## Decision

We will route [instructions] using a ranked offering protocol built on
[rendezvous hashing][ADR-2].

When a source node needs to deliver an instruction, it computes a rendezvous
score for every live node against the instruction's routing key and ranks the
nodes in descending score order. It then offers the instruction to each node in
turn, starting with the highest-scored. The first node that accepts becomes the
destination node. If no remote node accepts, the source node handles the
instruction itself.

This gives us several properties:

- **No disk I/O on the source.** Routing is a pure [control plane] operation.
  The source computes scores from in-memory state (its view of the live node
  set) and makes network calls to offer the instruction. No storage reads or
  writes are involved.

- **Stale-view tolerance.** If the top-scored node has a different view of the
  candidate set and declines, the source simply moves to the next candidate.
  The ranking provides a deterministic fallback order without any negotiation.

- **Liveness guarantee.** The source node is always the final candidate in the
  offering sequence. If every remote node declines, the source handles the
  instruction itself. Work is never dropped, even if routing views are
  transiently inconsistent across the cluster.

- **Warm-state affinity.** The second-ranked node is a good fallback candidate
  because rendezvous scoring is stable: a node that is currently ranked #2 was
  likely recently ranked #1 (or will be again after the next membership change),
  so it may already have warm in-memory state for the workload. Walking the
  ranking in order maximizes the chance of reaching a node with warm state
  before falling back to a cold start.

The algorithm is generic. It operates on routing keys and candidate sets with no
awareness of what the instruction represents or how the destination node will
process it.

### Dismissed alternatives

We considered several alternatives:

- **Direct routing.** Always forward to the top-scored candidate; return a
  failure if it declines. This is the simplest possible protocol: one hop, no
  iteration. However, a single node with a stale membership view blocks routing
  entirely. The source must wait for its own view to update and retry, which
  pushes retry complexity to the caller and risks liveness gaps during
  transitions.

- **Direct routing with local fallback.** Forward to the top-scored candidate;
  if it declines, handle the instruction locally instead of trying further
  candidates. This limits forwarding to at most one attempt, which is simpler
  than walking the full ranking. However, it skips candidates that are likely to
  have warm state. The #2 and #3 ranked nodes are the next-best choices
  precisely because rendezvous scoring is stable. Falling back to self after a
  single miss discards useful ranking information and increases [OCC][ADR-3]
  conflicts unnecessarily.

- **Local execution only.** Always handle the instruction on the source node;
  accept OCC conflicts as the cost. This eliminates inter-node communication for
  routing entirely. However, it defeats the primary routing goal: keeping state
  warm on a single responsible node. Every node would independently load state
  from storage, and OCC conflicts would be the norm rather than the exception
  during stable operation.

## Consequences

No coordination infrastructure is needed for routing decisions. The only shared
input is the candidate set, which is populated by the node registry.

Liveness is guaranteed even during membership transitions. The fallback to self
ensures that an instruction is always handled, regardless of how many remote
nodes decline.

The cost of the protocol is proportional to the number of candidates that
decline before one accepts. Under stable membership, the first candidate
accepts and the cost is a single network call. During transitions, the source
may iterate several candidates, but transitions are brief and infrequent.

The algorithm is generic. We are free to apply it to any domain that requires
assigning an instruction to one of a set of candidate nodes.

This ADR introduces one term to the [glossary]:
**[ranked instruction routing]**.

<!-- references -->

[ADR-2]: 0002-rendezvous-hashing-for-workload-assignment.md
[ADR-3]: 0003-optimistic-conflict-resolution.md
[ADR-5]: 0005-homogeneous-cluster-nodes.md
[ADR-6]: 0006-durable-command-executor.md
[control plane]: ../glossary.md#control-plane
[glossary]: ../glossary.md
[instruction]: ../glossary.md#instruction
[instructions]: ../glossary.md#instruction
[ranked instruction routing]: ../glossary.md#ranked-instruction-routing
