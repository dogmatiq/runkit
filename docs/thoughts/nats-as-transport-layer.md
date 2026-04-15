# NATs as a transport layer

## Idea

Explore the use of NATs as a transport layer for event streams or inter-node messaging in Dogmatiq Runkit. Consider how NATs' publish/subscribe model, durability features, and clustering could fit into the event stream architecture. Evaluate trade-offs versus the current approach (e.g., gRPC, custom TCP, Kafka, etc.), including operational complexity, reliability, ordering guarantees, and integration with existing subsystems.

## Possibly related

- [docs/adr/0010-event-streams.md](adr/0010-event-streams.md)
- [docs/adr/0004-ranked-instruction-routing.md](adr/0004-ranked-instruction-routing.md)
- [docs/thoughts/event-stream-implementation.md](thoughts/event-stream-implementation.md)
