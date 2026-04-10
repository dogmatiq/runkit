# Event stream: implementation notes and open questions

This document captures design discussion that arose while writing ADR-10. It
records dismissed alternatives in full, algorithm options for the seek and
dedup operations, and an open question about how writers signal first attempts.

---

## One event per journal record

The simplest possible storage structure is to write each event as its own
journal record. Journal position then equals stream offset exactly, so seeking
to offset K is just "open the journal at position K" -- O(1) with no metadata
required.

This was considered and rejected for three reasons.

**Dedup matching.** With one record per event, deduplication must match each
event in the batch individually. The writer scans from LPO for each event ID in
the batch independently. A partial write -- e1 committed, e2 and e3 not -- is
indistinguishable from a full write of a single-event batch of e1 followed by
two unrelated events. There is no structural guarantee that finding e1 implies
e2 and e3 are present; the writer must scan further and confirm each ID. With a
batch record, finding the first event ID in a transaction unconditionally
implies the rest of the batch is present, because the whole record was committed
atomically or not at all.

**Partial-write recovery.** A handler invocation that records N events must
write them across N separate journal operations. A crash after position K of N
leaves the instance in partially-written state. On recovery, the subsystem must
inspect the journal to determine how many events from the batch were committed,
derive a per-event LPO for each remaining event, and resume from exactly the
right position. With a single batch record, there is only one fact to durably
track before attempting the write -- the LPO for the whole batch -- and
recovery reduces to exactly-once at the batch level: either the whole batch is
there or none of it is.

**Forward compatibility.** A single-operation record format has no room for
future record types. A batch transaction wrapper leaves space to introduce
additional operation types -- stream resets, metadata updates, administrative
markers -- without breaking the format of existing records. Whether any such
operations will ever be needed is uncertain, but the encoding cost of the
wrapper is low and the optionality is worth preserving.

The O(1) seek that one-event-per-record enables is real, but these costs are
not worth it. The seek cost of the batch model is manageable, as discussed
below.

---

## Seek algorithm options

The batch-per-transaction model requires searching the journal to locate the
record that covers a given offset. The `offset_before` and `offset_after`
metadata on each record enables several strategies.

### Binary search (baseline)

Scan the journal using a standard binary search, comparing the target offset
against each record's `[offset_before, offset_after)` range. O(log N) reads,
where N is the number of journal records. This is the guaranteed baseline.

### Direct probe using the journal-position/offset correlation

Anecdotally, most handler invocations record exactly one event. If that holds
universally, journal position equals stream offset exactly -- every transaction
has one event, so `offset_after = journal_position + 1` -- and the target
record is at journal position `target_offset`.

The tightness of this correlation is directly observable: the difference
`end_offset - transaction_count` equals the total excess events contributed by
all multi-event batches to date. If that difference is small, a direct probe at
`min(target_offset, transaction_count - 1)` lands close to the right record,
requiring only a short forward or backward scan to find the bracket. If it is
large, the correlation has degraded and binary search is more reliable.

In practice: probe first, binary-search as fallback if the probe misses by more
than a small window.

A rolling estimate of batch size could sharpen the initial probe:

- **Mean**: trivially `end_offset / transaction_count` -- O(1) space, O(1)
  update, exact. Sufficient for the probe-reliability signal and the
  interpolation correction factor.
- **Median**: not trivially computable from two counters. Exact computation
  requires O(N) storage or a two-heap structure (O(log N) per update). The P²
  algorithm provides a good approximation in O(1) space with O(1) updates; it
  may be worth the complexity if the mean is distorted by occasional large
  batches.
- **Mode**: batch sizes are small positive integers with a heavy-tailed
  distribution. A frequency histogram over the first few values (e.g., 1
  through 10, with an overflow bucket) is exact for the interesting part of the
  distribution at negligible cost.

Any of these could be persisted as journal metadata to survive restarts, though
the mean is recomputable at negligible cost from `end_offset` and
`transaction_count`, both of which are already in memory.

### Interpolation search

If batch sizes are roughly uniform, the offset-to-position mapping is
approximately linear. Interpolation search estimates the record position as
`round(target_offset * transaction_count / end_offset)` and iterates.
O(log log N) average for uniform distributions, O(N) worst case for adversarial
variance. The rolling mean described above is exactly the correction factor
that makes the initial interpolation estimate accurate. With a good estimate,
interpolation converges in very few probes -- but the direct probe strategy is
simpler and nearly as good for the common case.

### Exponential search from the tail

For two specific seek sites, the target is structurally near the current stream
end:

- A projection that is keeping up with a live stream stores its checkpoint
  after each event; on restart its checkpoint is close to the tail. A
  catching-up projection, a new projection, or one doing a historical replay
  may seek from anywhere in the stream.
- An LPO hint is set to the stream end immediately before the first write
  attempt and stays at that value on retries; it was near the tail when it was
  recorded.

For these cases, exponential (gallop) search backward from the tail is O(log d)
where d is the distance from the end in journal positions -- effectively O(1)
when d is small. It is not a general-purpose strategy; for seeks into older
parts of a long stream, binary search or interpolation are more appropriate.

### Sparse index

An in-memory or persisted index recording the journal position for every K-th
offset reduces any of the above to O(1) probe + O(K / avg_batch_size) forward
scan. The algorithmic improvement over binary search is modest (O(log(N/K))
rather than O(log N)), but the I/O reduction is more significant for large
streams backed by slow storage, since random-position reads become bounded.
Worth considering once profiling identifies seek I/O as a bottleneck.

---

## Dedup key: first event message ID vs causation ID

One approach identifies a batch for deduplication by the message ID of its
first event. An alternative is to use the command's message ID as the dedup
key -- i.e. the causation ID of the recorded events.

**Semantic argument for causation ID.** The question the dedup scan is
answering is "has this handler invocation's output already been persisted?" The
natural identity for a handler invocation is the command that triggered it.
Causation ID is stable across all retry attempts -- it never changes. First
event message ID is also stable across OCC retries of the same attempt (the
same events are retried), but it is only known after the handler runs. Causation
ID is known before the handler runs.

**Practical asymmetry with the first-attempt skip.** If the writer can assert
"this is my first attempt" and skip the dedup scan entirely (see below),
the dedup key does not matter for the common case -- the scan is not run. The
key matters only on retries, where the writer must identify the batch it
previously (may have) written. On a retry, the writer has the causation ID at
hand before re-invoking the handler, whereas the first event message IDs from
the prior attempt require reading the committed record first (if it exists) or
re-running the handler (if it does not). Causation ID is strictly easier to use
correctly on retries.

**Bloom filter pre-check.** Either key could be combined with an in-memory
bloom filter over recent batch identities. A filter miss means "definitely not
present" -- the dedup scan can be skipped entirely on the common write path
without requiring the first-attempt flag at all. A false positive triggers an
unnecessary scan, which is safe. The filter does not need to cover the full journal history. It is only useful
for an LPO that falls within the range the filter was built from. The filter
therefore carries a `base_offset` -- the stream offset of the earliest record
included when the filter was constructed. When a dedup request arrives:

- If `LPO >= base_offset`: the filter covers this range. A filter miss means
  the batch is definitely absent; skip the scan. A false positive still
  requires a scan, but the LPO bounds it.
- If `LPO < base_offset`: the filter does not cover this range; fall back to
  the LPO-bounded scan regardless.

On restart, rebuild the filter from a recent window of journal records and set
`base_offset` accordingly. Any LPO from before that window falls through to
scanning, which is always safe. Writers are distributed and their LPOs are not
observable by the stream, so attempting to track the global minimum LPO is not
viable. The `base_offset` comparison is the correct per-request mechanism.

**Filter rollover.** As the filter accumulates entries its false positive rate
rises (its bit occupancy increases). When occupancy exceeds a useful threshold,
the filter should be discarded and rebuilt from a fresh recent window, resetting
`base_offset` to the start of that window. LPOs below the new `base_offset`
fall back to scanning; those above benefit from the rebuilt filter immediately.
The right terminology for the degradation condition is fill rate or bit
occupancy -- a standard bloom filter's false positive rate is approximately
$(1 - e^{-kn/m})^k$ where $k$ is the number of hash functions, $n$ is the
number of inserted elements, and $m$ is the bit array size. Monitoring $n/m$
gives a direct signal for when to roll over.

**Open question.** The dedup key is described abstractly in ADR-10 as "a
stable batch identity." The specific key choice is a backward-incompatible
change to the journal record format and the query interface; it should be
deliberate when the integration and aggregate subsystems are designed.

---

## First-attempt flag: skipping the dedup scan

If a writer can reliably assert that no prior attempt has been made to write a
given batch, the stream can skip the dedup scan entirely -- O(1) append
regardless of stream length or concurrent activity on other batches.

The assertion is only valid if it is backed by durable state. The writer must
have persisted something before sending the "first attempt" request, so that if
the request was sent and the writer crashes before hearing back, the restart
path knows to use an LPO instead. If the writer sends a "first attempt" flag
without durably recording that it did so, a crash-and-restart could send
another "first attempt" flag for the same logical invocation, causing the
previous commit (if it succeeded) to be overwritten by duplicate events at
different offsets.

This is the same relationship as the general LPO contract: the LPO must be
persisted before it is sent, so its validity survives a crash. A "first
attempt" flag is structurally equivalent -- it is a special value of the LPO
meaning "I have not yet attempted this write."

The multi-writer point does not undermine this. We are not asking "did any
writer write a batch containing this event IDs?" -- we are asking "did _I_ write
this batch?" Other writers writing other batches to the same stream is
irrelevant; they cannot produce the specific event identity we would be
searching for.

### Thought exercise: integration subsystem

An integration handler (per ADR-6) has a per-node data store keyed by
`(node, app_key, handler_key)`. Commands accepted for that handler are appended
to this store before execution continues asynchronously.

Consider how the handler's executor could safely use the first-attempt flag:

**On first invocation of a command:**

1. Load the data store. If it contains no record for this command, no prior
   stream append attempt exists.
2. Record the current stream end as the LPO and mark the command as
   "stream-write in progress" in the data store atomically (a single journal
   append at the current position).
3. Send the append request to the stream with the first-attempt flag set and
   the recorded LPO as a hint. Because no prior attempt exists, the stream
   skips the dedup scan.

**On retry (after a crash between step 2 and a confirmed response):**

1. Load the data store. It contains an "in progress" record with the stored LPO.
2. Send the append request with the first-attempt flag _clear_ and the stored
   LPO as the hint. The stream scans from LPO.

**On restart after a confirmed commit:**

1. Load the data store. It contains a "committed" record (written after
   receiving the stream's success response).
2. No stream append is needed; proceed to the next stage of execution.

This gives O(1) stream writes on the common (non-crash) path and O(log N) on
crash recovery. The data store's "in progress" record is the durability
guarantee that makes the first-attempt assertion valid.

The precise format of the data store records and the journal write at step 2
are not decided here. The key point is structural: the "first attempt" decision
is not the stream's to make -- it is the writer's, and the writer must prove it
with durable state before the stream can honour it.

Notably, this requires no new abstractions. The integration handler's existing
data store journal already supports exactly the kind of append needed at step 2.
The first-attempt protocol is a usage pattern on top of infrastructure ADR-6
already defines, not a new subsystem.

**Open question.** Should the first-attempt flag be a literal boolean in the
append request, a sentinel LPO value (e.g. max uint64), or implicit in the
absence of an LPO field? This is a protocol design choice deferred to the
integration and aggregate ADRs.

---

## Aggregate instance history

An aggregate instance reconstructs its state by replaying its own events from
the stream. The per-instance journal stores a `first_event_offset` that
bounds the start of the scan; reconstruction reads forward from that offset,
skipping events that belong to other instances, and replays matching events
through `ApplyEvent`.

The stream is shared across all instances and integrations writing into it. The
forward scan is therefore O(events in the stream since `first_event_offset`),
not O(events for this instance). On a busy stream with many instances, the skip
rate may be high; on a stream that belongs to a single active instance, the
skip rate is zero.

A per-instance index on the stream side -- recording the journal position of
each instance's events -- would reduce reconstruction to O(events for this
instance). This was considered and dismissed in the aggregate event storage
discussion: the per-instance journal is upstream of the stream, so building a
second index on the stream side duplicates information already available to the
aggregate subsystem, and would require the stream to understand instance
identity, which is an abstraction violation. The aggregate subsystem is
therefore responsible for bounding the scan cost, e.g. via snapshots and the
offset hint recorded at clean unload.

The problematic case -- a formerly high-traffic instance that has gone quiet,
with a long event history and no recent snapshot -- is the primary motivation
for maintaining a per-instance offset index. The design of that index, including
the choice of Roaring Bitmap and its interaction with snapshots, is discussed in
`docs/thoughts/aggregate-event-storage.md`.
