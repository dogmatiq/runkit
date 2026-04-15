# Event stream: implementation notes

This document captures implementation guidance that complements
[ADR-10](../adr/0010-event-streams.md) without duplicating its decisions.

## Bloom filter pre-check for dedup scans

ADR-10's dedup procedure scans forward from the search floor to find a prior
append operation with the same `causation_id`. An in-memory bloom filter over
recent `causation_id` values can skip this scan on the common (non-retry) write
path without requiring the first-attempt assertion at all. A filter miss means
"definitely not present" — skip the scan. A false positive triggers an
unnecessary scan, which is safe.

The filter does not need to cover the full journal history; it is only useful
when the search floor falls within the range the filter covers. The filter
therefore carries a `base_offset` — the stream offset of the earliest record
included when the filter was constructed. When a dedup request arrives:

- If `search_floor >= base_offset`: the filter covers this range. A filter miss
  means skip the scan; a false positive still requires a scan, but the search
  floor bounds it.
- If `search_floor < base_offset`: the filter does not cover this range; fall
  back to the search-floor-bounded scan.

On restart, rebuild the filter from a recent window of journal records and set
`base_offset` accordingly. Writers are distributed and their search floors are
not observable by the stream, so tracking the global minimum search floor is
not viable; the `base_offset` comparison is the correct per-request mechanism.

### Filter rollover

As the filter accumulates entries its false positive rate rises with bit
occupancy. When occupancy exceeds a useful threshold, discard the filter and
rebuild it from a fresh recent window, resetting `base_offset` to the start of
that window. Search floors below the new `base_offset` fall back to scanning;
those above benefit from the rebuilt filter immediately. A standard bloom
filter's false positive rate is approximately $(1 - e^{-kn/m})^k$ where $k$ is
the number of hash functions, $n$ is the number of inserted elements, and $m$
is the bit array size. Monitoring $n/m$ gives a direct signal for when to roll
over.

### UUID-aware bit extraction

Because `causation_id` values are UUIDs (v4 or v5), their bytes are already
uniformly distributed. The hash computation normally required by a bloom filter
can be eliminated entirely: instead of hashing the key to produce $k$ bit
indices, slice non-overlapping $b$-bit windows directly from the UUID bytes and
use each as an array index.

v4 and v5 UUIDs both have 6 fixed bits — a 4-bit version nibble (bits 48-51)
and a 2-bit variant field (bits 64-65). Windows that span those positions have
reduced entropy. The simplest avoidance strategy is to draw windows only from
the two clean ranges: bits 0-47 (48 bits) and bits 66-127 (62 bits), giving
110 usable bits. That supports $k = 5$ windows at $b = 20$ ($m = 2^{20}$,
1 Mbit) or $k = 6$ windows at $b \leq 18$.

In practice, even including the fixed bits causes only a marginal increase in
false positive rate — and false positives in this filter are already safe (they
trigger an unnecessary scan, not a correctness failure). The meaningful
optimization is eliminating the hash call, not the precise window layout.

This optimization does not apply to v7 UUIDs, whose high 48 bits encode a
millisecond timestamp and are neither uniform nor independent across entries.
