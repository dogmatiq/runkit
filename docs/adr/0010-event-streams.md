# 10. Event streams

Date: 2026-04-15

## Status

Accepted

- References [6. Durable command executor][ADR-6]

## Context

The engine must deliver event messages recorded by aggregate and integration
handlers to three kinds of consumers: projection handlers, process handlers,
and [`WithEventObserver()`] functions. All events recorded by a single
aggregate instance must be delivered in the order they were recorded.

An [event stream] is an immutable, ordered sequence of events, with each
event numbered by a zero-based integer offset. An application may have any
number of streams, each identified by a UUID, and ordered independently. The
[`ProjectionMessageHandler`] interface uses this abstraction explicitly;
[`ProcessMessageHandler`] and `WithEventObserver()` do not. In the context
of a projection, each event belongs to exactly one stream.

## Decision

### Scope

This ADR attempts only to solidify decisions about the core functionality
of event streams. Specifically, it does **not** address:

- How event producers determine which of the application's streams to use.
- How to enforce Dogma's requirement that each event is written to exactly one
  stream.
- How event consumers determine which stream(s) to read from.
- The API used for reading historical events.
- The API used for reading real-time events ("live-tailing").
- Whether all event delivery mechanisms within `runkit` will use an event
  stream.

### Stream model

Each stream has its own identity and lifetime, decoupled from any particular
handler or aggregate instance. The engine places no constraint on the number
of streams an application may use.

Streams come into existence when the first **append request** targeting its
stream ID succeeds; no separate stream creation operation is required. The
decision about how to choose a stream ID is outside the scope of this ADR. There
is no mechanism within the engine that truncates or deletes streams.

Each stream's state is persisted using a single [`persistencekit` journal],
keyed `(app_key, stream_id)`. The `app_key` dimension is not required for
correctness because stream IDs are globally unique; it is included for
operational convenience at the persistence layer.

### Journal structure

The journal is structured as a transaction log:

- Each journal record represents one **transaction**.
- Each transaction contains a **header** and one or more **operations**.
- Each operation represents a single concrete change to the event stream's
  state.

Each record is appended to a [`persistencekit` journal] atomically. Because each
record represents one event stream transaction, each stream transaction is an
atomic operation. Within the journal, transactions are encoded as a Protocol
Buffers message.

We define a single operation: the **append operation**. It represents the
addition of events to the end of the stream. The append operation contains an
ordered sequence of [`envelopepb.Envelope`] values containing events recorded as
a result of a single Dogma command message.

#### Transaction header

The transaction header conveys information about the stream's state and the
transaction itself. Its structure does not change based on the operations the
transaction contains. It contains the following fields:

- `committed_at` — the time at which the transaction was committed to the
  journal. The engine makes no use of this field; it is included for
  operational observability only.

- `[begin, end)` — the half-open range of event offsets appended within this
  transaction.
  - `begin` is the **offset of** the first event appended by this transaction.
  - `end` is the **offset after** the last event appended by this transaction.

The `[begin, end)` range is used to implement efficient searches for events by
offset. If a new operation type is introduced, a transaction may exist without
appending any events. This does not change the definition of `[begin, end)` —
they are both equal to the `end` offset of the prior transaction.

### Append requests

An append request is an atomic, idempotent assertion that a specific sequence of
events exists on the stream. Each request carries an ordered sequence of
[`envelopepb.Envelope`] values containing events recorded as a result of a
single Dogma command message.

All envelopes in the request must have the same `causation_id` — the message ID
of the command that produced them. We use this as a deduplication key, allowing
at most one append operation per `causation_id` per stream. A producer must not
submit requests carrying the same `causation_id` to different streams; the
implementation cannot detect duplicates across streams.

To support safe retries in the presence of transient errors, each request
carries two fields:

- **Search floor** — the lowest stream offset at which a prior attempt to append
  these events could have succeeded. This bounds the search for an existing
  append operation with the same `causation_id`. The producer is responsible for
  persisting this value accurately across retries and restarts.

- **First-attempt assertion** — a flag indicating that no prior attempt to
  append these events has been made. The producer must set this only on the
  initial attempt.

If the first-attempt assertion is present, the implementation attempts to commit
directly. If the assertion is absent, or the direct commit fails due to a
journal conflict, the implementation performs duplicate detection:

1. Find the transaction whose header range contains the search floor.
2. Scan forward through transactions to find an append operation with the same
   `causation_id` as the append request.

If the request is found to be a duplicate, it succeeds but is treated as a
no-op. Otherwise, a new append operation is committed, either in its own
transaction or combined with other operations in a single transaction.

Events from the request are appended to the stream in the order they appear in
the request as a contiguous block. The response to an append request is always
the `[begin, end)` range of the events in the request, regardless of whether the
request resulted in a new append or was deduplicated. Because the events are
contiguous, the producer can determine the offset of every event in the request
from this range.

### Reading events

To read events from a stream, we must locate the transaction whose header range
contains a specific target offset. During duplicate detection, this target is
the search floor provided in the append request.

We will use a [search algorithm] to find the transaction that contains the event
at the target offset. This transaction becomes the starting point for a forward
scan through the transactions. No other access pattern is required.

Events are yielded in increasing offset order. Event order is defined by
transaction commit order, append-operation order within each transaction, and
envelope order within each append operation.

A read operation continues until the reader chooses to stop, or it reaches the
end of the stream.

Decisions related to real-time event consumption (live-tailing) are beyond the
scope of this ADR.

### Search algorithm

The search algorithm used to locate the transaction containing a target offset
is based on [interpolation search] — a generalization of binary search that
uses the offset ranges in transaction headers to estimate where in the journal
the target is likely to sit, rather than always reading the midpoint.

The algorithm is as follows:

- Let $T$ be the target offset.

- Let the search bracket $[P_\text{beg}, P_\text{end})$ span the entire range of
  journal positions.

Loop:

1. If $P_\text{beg} \geq P_\text{end}$ then $T$ is not present in the stream.

2. Let $[O_\text{beg}, O_\text{end})$ be the offset range covered by $[P_\text{beg}, P_\text{end})$.

3. If $O_\text{beg} \geq O_\text{end}$ or $T \geq O_\text{end}$ then $T$ is not present in the stream.

4. Let the probe position $P'$ be the position that is the same proportion
   through $[P_\text{beg}, P_\text{end})$ as $T$ is through $[O_\text{beg}, O_\text{end})$:

```math
P' = P_\text{beg}
   + \frac{T - O_\text{beg}}{O_\text{end} - O_\text{beg}}
   \cdot (P_\text{end} - P_\text{beg})
```

5. Let $[H_\text{beg}, H_\text{end})$ be the offset range from the header of the record
   at $P'$.

6. If $T \lt H_\text{beg}$ then $P_\text{end} \leftarrow P'$ and return to step 1.

7. If $T \geq H_\text{end}$ then $P_\text{beg} \leftarrow P' + 1$ and return to step 1.

8. The record at $P'$ contains $T$.

#### Rationale

The benchmark suite under `docs/adr/benchmarks/adr10` measures how many journal
reads each algorithm requires to locate a target offset. It uses a fixed journal
whose transactions are generated according to the distribution model below, and
selects search targets according to the query model below. Both reflect
assumptions about typical production behavior rather than empirical data. To run
the benchmarks and see the ranked results:

```shell
go test -bench=. -v ./docs/adr/benchmarks/adr10/
```

The **distribution model** defines how many events each transaction carries:

| Probability | Events | Description                           |
| ----------- | ------ | ------------------------------------- |
| 0.1%        | 0      | Empty transaction (every 1,000th)     |
| 0.5%        | 0–99   | Rare large batch                      |
| 9.5%        | 0–4    | Occasional small batch or zero events |
| ~90%        | 1      | Single-event command                  |

The **query model** defines what offset each search targets:

| Probability | Description                                                             |
| ----------- | ----------------------------------------------------------------------- |
| 1%          | Offset 0 — cold-start consumer reading from the beginning               |
| 9%          | Uniformly random offset anywhere in the stream (may be mid-transaction) |
| 25%         | Begin offset of a uniformly random transaction — historical replay      |
| 65%         | Begin offset of one of the last 5 transactions — live-tail consumer     |

Under these models, the results are as follows:

| Rank | Algorithm                      | Mean reads/op | Max reads/op |
| ---- | ------------------------------ | ------------- | ------------ |
| 1    | Interpolation search by mean   | 4.86          | 25           |
| 2    | Coarse-fine search             | 7.47          | 24           |
| 3    | Interpolation search by EWMA   | 10.38         | 31           |
| 4    | Interpolation search by secant | 14.02         | 27           |
| 5    | Exponential search             | 15.53         | 40           |
| 6    | Fenwick search                 | 17.44         | 22           |
| 7    | Binary search                  | 20.11         | 21           |

Each read represents one network round-trip on a production-worthy
`persistencekit` journal implementation — either fetching a record by position
or querying the journal's bounds — so reads/op is a direct proxy for latency
and load on the persistence layer.

Algorithms are ranked by mean reads/op, with max reads/op as a tiebreaker for
worst-case behavior; interpolation search by mean event density ranks first.
The workload is dominated (65%) by live-tail queries, where the mean density
formula's first probe lands very close to the end of the journal — typically
resolving the search in two or three reads. Variants that adapt to local
density (EWMA and secant) require additional per-record bookkeeping with no
benefit — the mean formula is accurate enough for these queries.

Binary search is the simpler alternative. It requires no assumptions about event
distribution and is trivial to implement correctly. The fatal flaw is that it
ignores information that is freely available — the offset range in each
transaction header tells the algorithm exactly where in the offset space each
probe lands, making a more precise estimate possible without any additional
reads.

## Consequences

The append-only transaction log structure aligns naturally with [event sourcing]
— the physical order of journal records matches the logical ordering of events,
so there is no structural mismatch to reconcile.

The transaction header format is independent of the operations a transaction
contains. We are free to introduce new operation types alongside the append
operation without changing the journal structure or requiring a migration of
existing data.

Deduplication correctness depends on the producer satisfying the obligations
set out in the [append requests section]. A producer that violates any of them
risks appending duplicate events to the stream.

There is no mechanism to truncate or delete streams. Storage grows monotonically
with the number of events appended.

Each stream acts as a convergence point, collecting events from potentially many
producers — aggregate instances and integration handlers spread across many
cluster nodes — into a single ordered sequence. A single stream per application
would become a write bottleneck at scale; the model supports any number of
streams to avoid this. This ADR places no constraint on how stream IDs are
assigned.

The response to every append request includes the `[begin, end)` range of the
events, even when the request was deduplicated. A producer can always determine
the offset of every event it has appended without performing a separate read.

<!-- references -->

[ADR-6]: 0006-durable-command-executor.md
[append requests section]: #append-requests
[`envelopepb.Envelope`]: https://pkg.go.dev/github.com/dogmatiq/enginekit/protobuf/envelopepb#Envelope
[event sourcing]: https://en.wikipedia.org/wiki/Event_sourcing
[event stream]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#event-stream
[interpolation search]: https://en.wikipedia.org/wiki/Interpolation_search
[`persistencekit` journal]: https://pkg.go.dev/github.com/dogmatiq/persistencekit/journal#Journal
[`ProcessMessageHandler`]: https://pkg.go.dev/github.com/dogmatiq/dogma#ProcessMessageHandler
[`ProjectionMessageHandler`]: https://pkg.go.dev/github.com/dogmatiq/dogma#ProjectionMessageHandler
[search algorithm]: #search-algorithm
[`WithEventObserver()`]: https://pkg.go.dev/github.com/dogmatiq/dogma#WithEventObserver
