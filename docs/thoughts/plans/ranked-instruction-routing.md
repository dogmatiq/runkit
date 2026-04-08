# Plan: Ranked Instruction Routing

This document describes the planned implementation of the ranked instruction
routing loop. The algorithm is fully specified by [ADR-0004]. The inter-node
transport is not specified; this plan defers that to Phase 9 (gRPC) and
introduces an abstract interface instead.

## What it is

When a source node needs to deliver an instruction, it must find a destination
node willing to accept it. [ADR-0004] specifies the algorithm:

1. Rank all live nodes by rendezvous score for the instruction's routing key
   (descending).
2. Offer the instruction to each node in turn, starting with the
   highest-scored.
3. The first node that accepts becomes the destination.
4. If no remote node accepts, the source node handles the instruction itself.

The source node is always the last candidate, guaranteeing liveness: work is
never dropped regardless of how many remote nodes decline.

## Package

`internal/routing`

The algorithm is generic -- it has no knowledge of what the instruction
represents or how the destination processes it. It belongs in a dedicated
package rather than inside any subsystem.

## Design: abstract transport

The transport layer (how an offer travels from source to destination) is not
specified by ADR-0004 and will not be finalized until Phase 9 (inter-node gRPC).
The routing loop is implemented against an interface so it can be developed,
tested, and used in-process before the gRPC layer exists.

```go
// Offerer sends an instruction to a candidate node and reports whether the
// node accepted.
type Offerer[I any] interface {
    Offer(ctx context.Context, node *uuidpb.UUID, instruction I) (accepted bool, err error)
}
```

An in-process implementation of `Offerer` can be used for single-node mode and
testing. The gRPC implementation follows in Phase 9.

## Interface

```go
// Router routes an instruction to the best available node using ranked
// instruction routing (ADR-0004).
type Router[I any] struct {
    // SelfID is the UUID of the source node. It is always last in the offer
    // sequence and cannot decline.
    SelfID *uuidpb.UUID

    // Offerer delivers an instruction to a candidate node.
    Offerer Offerer[I]
}

// Route offers instruction to nodes in descending rendezvous score order for
// routingKey, stopping at the first accepting node. candidates is the live
// node set; it must include SelfID.
//
// Route always delivers: if no remote node accepts, Route delivers to SelfID
// via the Offerer before returning.
func (r *Router[I]) Route(
    ctx context.Context,
    routingKey *uuidpb.UUID,
    candidates []*uuidpb.UUID,
    instruction I,
) error
```

Internally `Route` calls `rendezvous.RankAbove(routingKey, SelfID, candidates)`,
which returns only the candidates ranked above `SelfID` in descending score order.
`Route` iterates those candidates,
calling `Offerer.Offer` for each and stopping at the first `accepted == true`.
If the loop exhausts all higher-ranked candidates without an acceptance, `Route`
calls `Offerer.Offer` for `SelfID` directly -- which cannot decline.

This is the correct fallback boundary: if the source node is ranked 5th, nodes
ranked 6th through 10th are never offered to. They are worse choices than the
source node itself, so the source takes the work rather than delegating downward.

## Constructing the candidate set

`Route` takes the live node set as a parameter -- it does not maintain its own
view of cluster membership. The candidate set is provided by the node registry
(Phase 2). The routing package has no dependency on the node registry; the
caller passes in whatever candidates it currently knows about.

## Error handling

A network error from `Offerer.Offer` should not abort the routing loop -- it is
equivalent to a decline during a membership transition. The router logs the
error and moves to the next candidate. Only an error from the self-offer (which
never crosses the network) should propagate as a hard failure.

> This policy means transient failures on a candidate are silently skipped.
> Consider whether a context cancellation should still abort the loop. Answer:
> yes -- `ctx.Err() != nil` should break out unconditionally.

## Relationship to rendezvous

`Route` calls a new helper `internal/rendezvous.RankAbove` rather than the
existing `Rank`. The distinction:

- `Rank(w, candidates)` -- returns all candidates ranked, with the
  self-affinity winner (where `w == c` in the candidate set) placed first.
- `RankAbove(w, c, candidates)` -- returns only the candidates that rank
  strictly above `c` for `w`, in descending score order. `c` itself and
  any lower-ranked candidates are omitted entirely.

`RankAbove` simplifies the routing loop: the result is exactly the sequence of
candidates to offer to before the caller handles the workload itself. There is
no need to move, skip, or sentinel the source node in the iteration. Candidates
ranked below the source node are not offered to because the source is already a
better choice than they are -- delegating to a lower-ranked node would be
counterproductive. The self-affinity guarantee from ADR-0002 is not relevant
here because the source node ID is unrelated to the instruction's routing key.

## Out of scope

- Inter-node transport implementation (Phase 9 -- gRPC)
- Node registry and live candidate set management (Phase 2)
- Instruction type definitions (defined per subsystem)
- Acceptance logic on the destination node (defined per subsystem)

## Dependencies

- `internal/rendezvous` -- `Rank` function
- `github.com/dogmatiq/enginekit/protobuf/uuidpb` -- UUID type

No dependency on any subsystem, node registry, or transport package.

<!-- references -->

[ADR-0002]: ../../adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-0004]: ../../adr/0004-ranked-instruction-routing.md
