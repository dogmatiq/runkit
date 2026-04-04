# Research: XOR scoring as a replacement for rendezvous hashing

This document summarises an investigation into whether the xxhash-based rendezvous scoring
function in ADR-0002 can be simplified, given that all inputs and candidates are UUIDs.

**Related documents:**

- [ADR-0002](../adr/0002-rendezvous-hashing-for-work-assignment.md) — current decision

---

## Question

ADR-0002 uses `score(input, candidate) = xxh64(input_uuid, candidate_uuid)`, selecting the
candidate with the highest score. Since both inputs and candidates are always UUIDs (fixed-length,
high-entropy values), is a hash function actually necessary, or could we use a simpler arithmetic
operation?

---

## Options considered

### XOR maximum (naive)

```
score(I, C) = I XOR C
winner      = candidate with highest score
```

Distribution is uniform for random inputs (XOR with a fixed value is a bijection). However,
`I XOR I = 0` -- the minimum possible score -- so self-affinity (the property that a candidate
always wins for its own UUID as input) would require a special case, just as the current xxhash
implementation does.

### XOR minimum ("closest node wins")

```
score(I, C) = I XOR C
winner      = candidate with lowest score
```

This is used in distributed hash table designs (notably the Kademlia protocol, a peer-to-peer
lookup algorithm). Because `I XOR I = 0` is the minimum, self-affinity falls out naturally with
no special case. XOR is a valid metric (it satisfies the triangle inequality), so "distance" is
a mathematically accurate framing, not just a metaphor.

The 1/N distribution property and minimal-disruption property both hold: XOR with a fixed value
is a bijection, so the N scores are still N independent uniform random values, and each candidate
is equally likely to hold the minimum.

### Subtraction/difference

```
score(I, C) = |I - C|   (treating the 128-bit UUID as an unsigned integer)
winner      = candidate with lowest score
```

Like XOR-minimum, `|I - I| = 0` gives self-affinity naturally. The 1/N load distribution
property also holds: all candidates are iid uniform, so by symmetry each is equally likely to be
numerically closest to any fixed I, giving `P(C_j wins) = 1/N` for all j. The minimal-disruption
property holds on average too.

What subtraction actually produces is a Voronoi partition of the UUID number line -- each input
routes to the numerically nearest candidate, forming contiguous ranges. XOR produces the same
structure but in Hamming-cube metric space rather than on a number line. For uniform random UUIDs,
both are statistically equivalent in terms of load balance.

Subtraction offers no statistical advantage over XOR-minimum but has two practical disadvantages:

- Go has no native 128-bit integer type. XOR on a pair of `[16]byte` values is a trivial loop;
  subtraction requires explicit borrow-propagation across two 64-bit words.
- If `uuidpb` were ever extended to accept v7 UUIDs, XOR and subtraction would both degrade --
  temporally close v7 UUIDs share high timestamp bits, so their XOR distance and their numeric
  difference would both cluster near zero. Neither approach has an advantage here; the v7 problem
  is symmetric. The protection in both cases is the same: `uuidpb.Validate()` enforcing v4/v5.

---

## The UUID version constraint

The concern with XOR-based scoring for arbitrary UUIDs is that UUIDv7 encodes a monotonic
timestamp in the high bits. Two v7 UUIDs from similar creation times would have near-zero XOR
in the high 48 bits, concentrating competition and causing unequal load distribution. xxhash
avoids this because it avalanches the input bits.

However, `uuidpb.Validate()` in `enginekit` explicitly rejects any UUID that is not v4 or v5:

```go
switch (x.GetUpper() >> 8) & 0xf0 {
case 0x40, 0x50:
default:
    return errors.New("UUID must use version 4 or 5")
}
```

This is an enforced invariant, not a convention. A v7 UUID would fail validation before reaching
the scorer. The temporal-clustering problem therefore does not apply to this codebase.

Expanding `uuidpb` to accept v7 would be a deliberate, visible breaking change -- not something
that could silently degrade distribution. If that ever happens, the scorer can be updated at the
same time.

---

## Conclusion

The case for keeping xxhash over XOR-minimum rests entirely on robustness against UUID versions
with non-uniform bit distributions. That case does not apply here. Both xxhash-maximum and XOR-
minimum provide equivalent correctness guarantees given the v4/v5-only constraint.

XOR-minimum is the simpler algorithm and eliminates the explicit self-affinity special case.
ADR-0002 should be considered for update to adopt it.

---

## Recommended change to ADR-0002

Replace the scoring rule:

```
score(input, candidate) = hash(input_uuid, candidate_uuid)
winner                  = candidate with highest score
```

with:

```
score(input, candidate) = input_uuid XOR candidate_uuid
winner                  = candidate with lowest score (minimum XOR distance)
```

The self-affinity section of ADR-0002 would change from describing an explicit maximum-score
override to noting that `I XOR I = 0` is naturally the minimum, so self-affinity is an emergent
property of the algorithm rather than a special case.

The dependency on xxhash can be removed.
