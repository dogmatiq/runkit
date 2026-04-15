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
