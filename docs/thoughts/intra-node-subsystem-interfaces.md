# Intra-node subsystem interface design

When subsystems on the same node need to communicate with each other, should
they use the generated gRPC client/server interfaces, or a Go-native abstraction
with a gRPC adapter for the cross-node case?

## The question

There are three broad options:

**Option A -- use gRPC interfaces directly for everything.** Both intra- and
inter-node calls go through the proto-generated client/server types. Intra-node
calls use a local gRPC server on loopback (or in-process via `bufconn`).

Pros: single interface, no adapter layer, wire format and internal representation
are always in sync.

Cons: proto marshaling cost even for same-process calls; internal types are
permanently coupled to the wire schema; tests require gRPC scaffolding; any
internal evolution requires changes to the proto definition first.

**Option B1 -- own Go interface, RPC-like shape, gRPC adapter for cross-node.**
The subsystem exposes a Go interface with context-bearing method signatures (e.g.
`Append(ctx, req) (resp, error)`). A gRPC adapter implements the interface by
calling a remote node; an in-process implementation calls the service directly.

Pros: no serialization on fast path; Go types can be richer than wire types;
tests stub the interface cheaply; maps cleanly to gRPC (one method per RPC).

Cons: two representations to keep in sync; an additional adapter layer to write
and test.

**Option B2 -- own Go abstraction, channel-based, gRPC adapter for cross-node.**
The subsystem exposes channels as its external surface, consistent with the
existing pattern in e.g. `poisonqueue` (`EnqueueRequests <-chan EnqueueRequest`
with a `Response chan<-` embedded in each request). A gRPC adapter bridges
channel semantics to a streaming RPC.

Pros: consistent with existing codebase style; natural backpressure and
decoupling; goroutine-level parallelism without explicit coordination.

Cons: channel-to-gRPC bridging is non-trivial (needs a goroutine to pump the
channel and handle stream lifecycle, reconnect, etc.); error propagation through
channels is more complex than a method return; the adapter is harder to get
right than the RPC-like variant.

## Tentative lean

Option B1 (RPC-like Go interface + gRPC adapter) seems like the sweet spot:

- The intra-node fast path is pure Go.
- The interface shape mirrors gRPC naturally, keeping the adapter thin.
- Testing is cheaper than Option A.
- It avoids the channel-bridging complexity of B2 while still decoupling
  internals from the wire format.

The existing channel-based pattern in `poisonqueue` and `eventstream` may have
been chosen before gRPC was in scope; it does not necessarily prescribe the
inter-subsystem communication model once a network boundary exists.

An alternative middle ground: expose a channel-based interface internally (for
intra-node, in-process use) and a separate RPC-like interface (or just the gRPC
client) for cross-node communication, with the calling code responsible for
choosing the right one based on whether the target is local. This avoids forcing
the channel model onto the network adapter but adds a second interface per
subsystem.

## Is gRPC the right transport at all?

If the internals are channel-based, the natural cross-node mapping is a
bidirectional streaming RPC, not a set of unary calls. Each logical channel
becomes a stream; messages flow in both directions. But once you are running
bidirectional streams for multiple subsystems over the same connection, you are
essentially multiplexing your own message types over HTTP/2 -- which is exactly
what gRPC already does. The question then becomes: is gRPC adding value, or just
adding constraints?

**The case for staying with gRPC:**

- Schema enforcement via protobuf: the wire format is typed and validated.
- Ecosystem: TLS, auth interceptors, observability (tracing, metrics) are
  well-supported.
- Interoperability: if external consumers ever need to talk to a runkit node
  (e.g. CLI tools, dashboards), gRPC is a standard surface.
- Generated code handles framing, backpressure, and reconnect.

**The case for a custom multiplexed connection:**

- Full control over the message envelope: a single TCP connection between two
  nodes carries all traffic in both directions; no per-subsystem stream
  lifecycle to manage.
- No per-RPC overhead (HTTP/2 header frames per stream, gRPC trailers, etc.).
- Connection topology is an application-level concern: if node A connects to
  node B, the same connection carries traffic in both directions, with no
  need for B to also initiate a connection back to A.
- Simpler reconnect logic: one connection to manage, not N per-service
  connections.

**The connection direction problem:**

With conventional gRPC a node pairs a server (inbound) with a client pool
(outbound). In an N-node cluster each node maintains N-1 outbound connections
and accepts N-1 inbound connections -- 2*(N-1) connections per node, or N*(N-1)
total. A bidirectional transport (one connection per peer pair, direction of
dial determined by UUID ordering or similar) halves this to N\*(N-1)/2.

This is mostly a resource concern (file descriptors, goroutines for keepalives)
but it also matters for firewall rules and network policy in environments where
only certain nodes can initiate connections.

gRPC does not prevent sharing a single HTTP/2-level connection for all service
calls between two nodes, but it does not enforce this either -- the `grpc.Dial`
API creates one connection per `ClientConn`, so unless you share a single dial
across all service clients to the same peer, you get multiple connections
anyway.

**Proposed: UUID-ordered dial with a short accept window.**

When two nodes discover each other (via the heartbeat store), they need to
agree on who dials without coordinating. Comparing UUIDs gives a deterministic,
coordination-free answer:

1. The node with the lower UUID dials immediately.
2. The node with the higher UUID waits a short fixed window (e.g. 2s) before
   dialling. If an inbound connection from the lower node arrives within that
   window it accepts it and skips the outbound dial entirely.

This eliminates duplicate connections in the normal case. The 2s window covers
typical network and scheduling jitter and does not need to be exact -- a
simultaneous-open that slips through is harmless as long as one side detects
the duplicate (e.g. by the peer's UUID already being in the connection table)
and closes it.

For the case where a node receives a connection from a UUID it does not yet
recognise (e.g. the heartbeat write propagated to the connecting node before
it propagated to us):

- The accepting node immediately refreshes its live node set from the heartbeat
  store.
- If the connecting node's UUID is present after the refresh, it accepts the
  connection and does not initiate its own outbound dial.
- If it is still absent, the connection can be rejected (the peer will retry
  after its own heartbeat write propagates).

## How this interacts with the internal interface choice

If a custom multiplexed transport is used, the question of RPC-like vs.
channel-based internal interfaces becomes almost orthogonal: the transport is
itself channel-like (send a tagged message, receive a tagged reply), so either
internal style can be adapted to it without the awkwardness of bridging Go
channels to gRPC stream lifecycle.

The RPC-like option (B1) still maps cleanly: each call becomes
"send request message, block for response message with matching correlation ID."
The channel option (B2) maps as: "pump channel sends onto the connection, demux
replies back onto per-request response channels." Both are straightforward once
framing and multiplexing are handled at the transport layer.

## Alternative transports

**ConnectRPC.** ConnectRPC is a gRPC-compatible protocol that runs over plain
HTTP/1.1 or HTTP/2. It uses protobuf for the wire format and generates the same
Go interfaces as the standard gRPC plugin, but uses `net/http` handlers instead
of the `google.golang.org/grpc` server. The practical differences for this use
case:

- Pros: lighter dependency footprint; plain `net/http` is easier to reason
  about; compatible with standard HTTP middleware; still gets protobuf schema
  enforcement and code generation.
- Cons: bidirectional streaming over HTTP/1.1 is not supported (server-sent
  events only, which is unidirectional -- see below); over HTTP/2 it is
  essentially gRPC with a different framing header. The connection-topology
  problem (duplicate connections, direction negotiation) is unchanged.
- Verdict: a reasonable swap for gRPC if HTTP/2 is available and the service
  interface is mostly request/response or server-streaming. Less compelling
  for bidirectional streaming between peers.

**Server-sent events (SSE).** SSE is a unidirectional HTTP streaming protocol:
the server pushes a sequence of text events to the client over a single
long-lived HTTP response. For bidirectional communication it requires two
connections -- one client-to-server (conventional HTTP request or WebSocket),
one server-to-client (SSE). This makes it a poor fit for the peer-to-peer
node communication model:

- Two connections per peer pair, which is what we are trying to avoid.
- Direction of each connection is fixed (client dials server), so both nodes
  would each need to dial the other regardless of UUID ordering.
- Designed for browser-to-server use; its text-frame encoding is inefficient
  for binary protobuf payloads (typically base64-wrapped).
- No backpressure mechanism; the server can flood a slow client.

SSE is worth keeping in mind for future external-facing consumer APIs (e.g.
a web dashboard that needs to tail an event stream), where its browser
compatibility is an asset. For internal node-to-node communication it is not
the right choice.

**WebSockets.** Full-duplex over a single HTTP upgrade. Better than SSE for
bidirectional use, but carries the HTTP upgrade overhead, has no built-in
framing for length-prefixed binary messages, and is not natively protobuf-aware.
Roughly equivalent in complexity to rolling a custom TCP transport. Not
obviously better than `yamux` or `smux` over a plain TCP connection for an
internal protocol.

**Raw TCP + length-prefixed protobuf frames.** The minimal custom approach:
dial a TCP connection, write `uint32 length | proto bytes` frames in both
directions, multiplex logical streams with a header field in the message. This
is exactly what HTTP/2 does, minus HTTP semantics. The UUID-ordered dial
protocol works naturally here. Main downside: no ecosystem (no TLS helpers,
no interceptors, no code generation for the service layer).

**Summary.** For internal node-to-node communication the realistic candidates
are: standard gRPC, ConnectRPC over HTTP/2, or a custom mux (yamux/smux) with
length-prefixed protobuf frames. All three support bidirectional streaming and
the UUID-ordered single-connection topology. ConnectRPC is the easiest swap
from gRPC. A custom mux gives the most control at the cost of ecosystem support.

## Open questions

- Should all cross-node subsystem communication be mediated by a single
  "routing" layer that picks local vs. remote, so individual subsystems do not
  need to know whether their peer is local?
- If the channel-based style is retained for intra-node use, what is the right
  pattern for the gRPC adapter -- a goroutine-per-stream pump, or a unary RPC
  per channel receive?
- Does the choice differ by subsystem? The event stream service naturally
  produces a stream of events (streaming RPC maps well); the poison queue is
  fire-and-forget with a reply (unary maps well). A single adapter strategy may
  not fit all cases.
- Is there an existing Go library for the "one bidirectional mux connection per
  peer pair" pattern that avoids reinventing framing from scratch?
  (`yamux`, `smux`, and similar multiplexers sit at this level.)
