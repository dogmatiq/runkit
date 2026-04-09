# 9. Handler-keyed aggregate routing

Date: 2026-04-10

## Status

Accepted

- Amends [6. Durable command executor][ADR-6]

## Context

[ADR-6] established the aggregate routing key as `uuid5(app_key, instance_id)`,
deliberately omitting the handler key. The rationale was co-location: aggregate
types that share an instance ID — for example, `Customer` and `CustomerProfile`
both keyed by `customer-7` — would gravitate to the same node, keeping warm
state for related types together.

In practice, the benefit is almost non-existent. Once a command produces events,
those events are written to a stream on a node chosen independently of aggregate
routing. Co-locating aggregate types only helps if the stream node happens to be
the same node — coincidence, not design. One could imagine assigning stream
partitions in a way that biases them toward the same node as the aggregate, but
this breaks down as the cluster grows: with more nodes, the chance that any
stream partition and any given aggregate instance land on the same node drops
towards zero.

Against this negligible benefit, there are concrete costs. When the same
instance ID is shared across multiple aggregate types — as with `Customer` and
`CustomerProfile` both using `customer-7` — all commands for those types
concentrate on the same node. A heavily trafficked instance ID becomes a hot
spot. And when that node fails, all of those types fail together; none remain
available on other nodes.

The original design does produce one secondary property worth noting: when a
node fails all aggregate types sharing that instance ID fail together. While
this may be easier to reason about than partial failure across types, it is not
a correctness or liveness guarantee.

## Decision

We will include the handler key in the aggregate routing key:

```
routing_key = uuid5(app_key, handler_key, instance_id)
destination = rendezvous_hash(routing_key, available_nodes)
```

This amends the formula established in [ADR-6]. Each aggregate type routes
independently. Instances of `Customer` and `CustomerProfile` with the same
instance ID will typically be assigned to different nodes.

## Consequences

Aggregate instances of different types with the same instance ID are assigned
to different nodes. A node failure affects only the aggregate types whose
routing key maps to it; others keep running. Load from a heavily trafficked
instance ID distributes across multiple nodes rather than concentrating on one.

No routing key derivation established so far incorporates co-location as a
goal. Keeping it that way — each workload type routing independently, without
assumptions about where related workloads land — is a simpler model and worth
preserving as future routing decisions are made.

<!-- references -->

[ADR-6]: 0006-durable-command-executor.md
