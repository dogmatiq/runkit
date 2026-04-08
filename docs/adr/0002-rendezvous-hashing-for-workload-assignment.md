# 2. Rendezvous hashing for workload assignment

Date: 2026-04-04

## Status

Accepted

- Referenced by [3. Optimistic conflict resolution][ADR-3]
- Referenced by [4. Ranked instruction routing][ADR-4]
- Referenced by [5. Homogeneous cluster nodes][ADR-5]
- Referenced by [6. Durable command executor][ADR-6]

## Context

We need a way to assign workloads — such as aggregate instances or stream
partitions — to one of a set of candidate cluster nodes.

The assignment must be deterministic so that any node aware of the same work and
the same set of nodes independently reaches the same conclusion, without relying
on centralized coordination, leader election, or lookup tables.

## Decision

We will identify all workloads and candidates using [RFC 9562] UUIDs. UUIDs give
us globally unique, fixed-size identifiers that are cheap to generate.

We will use an algorithm based on [rendezvous hashing] (also known as highest
random weight hashing) for all workload-to-candidate assignment. For each
workload, we compute a score against every candidate and select the highest:

```
score(workload, candidate) = hash(workload_uuid, candidate_uuid)
winner                     = candidate with highest score
                             (ties broken by lowest UUID)
```

This gives us:

- **Deterministic results.** Any participant with the same workload and
  candidate set picks the same winner.
- **No coordination.** No shared state, no locks, no leader.
- **Minimal disruption.** Adding or removing a candidate only reassigns
  workloads that were mapped to the affected candidate.
- **Uniform distribution.** workloads distribute evenly across candidates
  without tuning parameters.
- **Order independence.** The result is the same regardless of the order in
  which candidates are presented. Ties (equal scores) are broken
  deterministically by selecting the candidate with the lowest UUID, so the
  winner is a function of the set of candidates, not a sequence.

Assignment is a pure function of the workload and the current candidate set, so
any participant can recompute it independently.

We will use [xxhash] (XXH3, 64-bit) as the hash function. The hash input is 32
bytes (two concatenated UUIDs), which XXH3 handles in the order of nanoseconds.
This matters because every incoming command requires a hash evaluation against
each candidate in the set. We don't need cryptographic properties because the
worst case of a skewed distribution is uneven work assignment, not a security
concern. Its 64-bit output distributes scores uniformly across candidates even
when the UUIDs are similar.

### Self-affinity

When a candidate's own UUID is used as the workload, the algorithm assigns the
maximum possible score to that candidate, guaranteeing it wins regardless of
other candidates' scores. This logic is explicit, not an emergent property of
the hash function. Other candidates evaluating the same workload independently
reach the same conclusion, so a candidate can claim exclusive ownership of work
identified by its own UUID without any negotiation.

If that candidate later leaves the candidate set, the workload is reassigned to
another candidate using the regular rendezvous scoring.

### Dismissed alternatives

We considered several alternatives:

- **[Consistent hashing]** offers O(log n) lookup compared to rendezvous
  hashing's O(n), but with a small candidate set (cluster nodes, not thousands
  of endpoints) the difference is negligible. It adds structural complexity with
  no practical benefit for our use case.
- **XOR distance** scores candidates by `workload XOR candidate`, selecting the
  closest. Self-affinity falls out naturally since `X XOR X = 0` is the minimum,
  removing the need for an explicit special case.
- **Numeric distance** (`|workload - candidate|` treating UUIDs as 128-bit
  integers) produces a [Voronoi partition] of the UUID number line. Like XOR,
  self-affinity is natural since `|X - X| = 0`.

The XOR and numeric distance approaches are appealing because they provide
self-affinity without a special case. However, similar UUIDs produce similar
scores, so UUIDs that are "close" to each other tend to land on the same
candidate. This is especially relevant to [UUIDv7], which encodes a monotonic
timestamp in the high bits, so temporally close UUIDs would cluster together,
skewing the distribution. A hash function scrambles the bits so that even very
similar UUIDs produce independent scores.

Today we enforce UUIDv4/v5 via [`uuidpb.Validate()`], so the UUIDv7 problem does
not currently apply, but the rendezvous hashing approach avoids coupling the
scoring function's correctness to a constraint enforced elsewhere.

Numeric distance also has the practical disadvantage that Go has no native
128-bit integer type.

## Consequences

We avoid any cluster-wide coordination or stored state for work assignment. The
only shared knowledge required is the candidate set itself: each participant
must know which candidates are currently available.

Adding or removing a candidate causes minimal reassignment; only workloads that
map to the affected candidate move.

Self-affinity enables local-write patterns where a candidate needs a private
partition without coordination. If the candidate goes offline permanently, the
workload is reassigned, so the partition is not orphaned.

The algorithm is generic. It operates on UUIDs and is not specific to any
particular type of work or candidate. We are free to apply it to specific
domains such as command routing and partition ownership.

This ADR introduces two terms to the [glossary]:
**rendezvous hashing** and **self-affinity**.

<!-- references -->

[ADR-3]: 0003-optimistic-conflict-resolution.md
[ADR-4]: 0004-ranked-instruction-routing.md
[ADR-5]: 0005-homogeneous-cluster-nodes.md
[ADR-6]: 0006-durable-command-executor.md
[consistent hashing]: https://en.wikipedia.org/wiki/Consistent_hashing
[glossary]: ../glossary.md
[rendezvous hashing]: https://en.wikipedia.org/wiki/Rendezvous_hashing
[rfc 9562]: https://www.rfc-editor.org/rfc/rfc9562.html
[`uuidpb.Validate()`]: https://pkg.go.dev/github.com/dogmatiq/enginekit/protobuf/uuidpb#Validate
[uuidv7]: https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_7_(timestamp_and_random)
[voronoi partition]: https://en.wikipedia.org/wiki/Voronoi_diagram
[xxhash]: https://xxhash.com
