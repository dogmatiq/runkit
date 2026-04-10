# Backpressure from handlers: flow control and load shedding

## The problem

An engine orchestrating distributed handlers faces a natural tension:

- The command intake rate may exceed what the cluster can process.
- Handlers vary drastically in throughput (some process 1000/sec, others 10/sec).
- The engine buffers commands somewhere (acceptance keyspace, per-node staging, handler input queue) but buffers are finite.
- When a buffer fills, what should the engine do?

## Approaches

### Backpressure: push upstream

The engine signals "I'm full" to the client. The client experiences:

- rejection (immediate "try again later")
- queueing delay (request arrives but waits for acceptance)
- increased latency, not lost commands

The client must handle rejection gracefully (retry, exponential backoff, etc.). Advantage: no data loss, work naturally flows at the rate the cluster can sustain.

Disadvantage: clients must implement retry logic; command acceptance latency becomes externally visible.

### Buffering: absorb locally

The engine accepts the command, promises durability, and schedules execution. The client sees low latency. But:

- Buffer must be finite (memory bound).
- If the buffer fills while latency is low, subsequent commands still experience backpressure (or get dropped).
- If the buffer is large enough to "smooth" all bursts, it can mask domain problems (slow handler, stuck consumer) and delay failure detection.

### Load shedding: drop or defer

When overloaded, the engine sheds work tactically:

- drop the oldest or least-important command from buffer
- defer execution to a future epoch/cycle
- selective rejection based on priority or SLA

Advantage: prevents cascade failures under sustained overload. Disadvantage: data loss potential, semantic complexity (does the client know its command was dropped?).

## Considerations

### Runkit is not a message queue

An important architectural distinction: Runkit is not a message queue. It's an event-driven engine that materializes state and routes commands to handlers based on that state. This distinction shapes backpressure strategy fundamentally.

A traditional queue applies backpressure by blocking producers, managing queue depth, or rejecting at the inlet. Runkit cannot use these strategies directly because the engine's flow is driven by event materialization and routing decisions, not by queue mechanics. Backpressure must instead surface through handler capacity constraints and routing choices — the engine can signal that a particular handler cannot accept more work (or is overloaded), but this falls out of handler state or observed load, not internal queue state.

This means backpressure in Runkit is more about observability and routing intelligence than flow control at the engine's boundary.

### Visibility and observability

- What does "full" mean? How full is the buffer?
- Should handlers expose their own capacity or processing rate?
- Can the engine estimate per-handler queue depth and reserve capacity?
- Is there a per-handler or global backpressure signal, or both?

### Handler heterogeneity

Handlers differ wildly in throughput. A process handler processing 10/sec becomes a bottleneck for the whole application if commands destined for that handler are prioritized uniformly. Per-handler capacity reservation or separate queues per handler can mitigate this, but adds complexity.

### SLA vs cost trade-off

- Strict backpressure (reject immediately if over capacity) guarantees "accept or reject quickly" but raises client burden and SLA latency.
- Large, absorbing buffers lower client-visible latency but risk memory bloat and cascade failures.
- Tiered buffering (small urgent queue for high-priority, larger overflow for normal) allows prioritization but increases operational complexity.

### Interaction with idempotency and deduplication

If the engine defers or retries a command, idempotency handling must absorb the second attempt. This is fine and expected. But if backpressure causes a client to retry more aggressively than intended (due to timeout), the engine may see higher retry load than designed for.

## Open questions

- Should the per-node acceptance keyspace (ADR-6) be subject to backpressure, or should commands be rejected at intake before they reach the keyspace?
- Is there a handler-published "max in-flight commands" or capacity reservation? Or is capacity managed globally?
- What metrics should the engine expose to allow clients to implement smart backoff?
- For projection handlers (or other async consumers), does backpressure propagate backwards through event streams?

## Possibly related

- [Durable command executor (ADR-6)](../adr/0006-durable-command-executor.md) — per-node acceptance keyspace
- [Event stream model (ADR-10)](../adr/0010-event-stream-model.md) — stream as sink
- [Big picture thought](./000-big-picture.md) — overall flow
