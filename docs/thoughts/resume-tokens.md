# Resume Tokens for Event Streams

## Idea

Introduce the concept of "resume tokens" for resuming reading from an event stream efficiently. These tokens would encode the stream's internal journal position, but in a way that does not expose it as a journal position explicitly to the consumer.

### Motivation

- Allows clients to resume reading from the correct position after a disconnect or restart, without exposing internal implementation details (such as the raw journal offset or sequence number).
- Enables future changes to the underlying storage or journal format without breaking clients, as the token format can evolve independently.
- Supports stateless clients and scalable stream processing.

### Implementation Sketch

- When a client reads from a stream, the server issues a resume token alongside each event or batch.
- The token is an opaque, versioned blob (e.g., base64-encoded, possibly signed or encrypted) that encodes the necessary information to resume from the same logical position.
- When resuming, the client presents the token; the server decodes it and seeks to the correct position.
- The token may include:
  - Journal position (encoded, not exposed directly)
  - Stream identity/version
  - Optional integrity/authentication data

### Considerations

- Tokens must not leak implementation details or allow clients to infer the underlying journal structure.
- The format should allow for future extension (e.g., version prefix).
- Security: tokens should be tamper-evident (e.g., HMAC or signature).
- Expiry: tokens may become invalid if the stream is truncated or compacted; clients must handle this gracefully.

## Possibly Related

- [docs/adr/0010-event-streams.md](../adr/0010-event-streams.md)
- [docs/thoughts/event-stream-implementation.md](event-stream-implementation.md)
