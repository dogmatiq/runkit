# Inter-node RPC

This document captures decisions and open questions about the inter-node RPC
layer. It complements [intra-node-subsystem-interfaces.md] which covers how
the gRPC layer is abstracted from the engine's internal subsystems.

## Transport: gRPC

gRPC over HTTP/2 is the transport. It provides TLS, connection multiplexing,
backpressure, keepalives, and protobuf code generation. Custom multiplexed
transports (yamux/smux, raw TCP) and broker-mediated transports (NATS) were
considered but not chosen:

- A custom mux requires owning framing, keepalives, flow control, and TLS
  integration -- all provided for free by gRPC.
- NATS embedded would eliminate connection topology concerns but adds JetStream
  as a persistence-adjacent dependency; since the persistence layer must remain
  independently pluggable (DynamoDB, PostgreSQL, etc.), NATS would run
  alongside the real persistence driver purely as a transport, which is not
  justified.

## Connection model

Each node proactively dials every peer it discovers via heartbeat reads. When a
node UUID appears in the live node set, the local node dials it immediately and
holds the connection open.

A single `*grpc.ClientConn` per peer is shared by all outbound RPCs to that
peer. All gRPC services to a given peer multiplex over the same underlying
HTTP/2 connection.

Keepalives are configured with `PermitWithoutStream: true` so the connection is
maintained even when no RPCs are in flight.

### Presence via connection liveness

The live connection is the real-time presence signal. A broken connection means
the peer is unreachable; the heartbeat record in the persistence store remains
the authoritative liveness signal. Dial failure does not immediately remove a
peer from the live node set -- gRPC reconnects silently in the background; the
heartbeat expiry is the authoritative eviction trigger.

The proactive dial also drives an immediate heartbeat cache refresh on the
receiving node (via the TLS handshake described below), which allows the
heartbeat poll interval to be increased significantly -- it only needs to cover
the eviction case, not the discovery case.

### No UUID-ordered dialing

Both nodes in a peer pair will eventually dial each other once each discovers
the other via heartbeat reads. Two connections per pair (one in each direction)
is acceptable and requires no coordination. gRPC does not automatically share
connections between independently created `ClientConn` objects, so this is an
expected consequence of the model.

## mTLS and the `CredentialsProvider` abstraction

All inter-node communication uses mutual TLS. The mechanism by which a node
obtains its certificate and verifies its peers is encapsulated behind a
`CredentialsProvider` interface:

```go
type CredentialsProvider interface {
    ClientCredentials() credentials.TransportCredentials
    ServerCredentials() credentials.TransportCredentials
}
```

Two concrete implementations are supported from day one. Implementing both from
the start stress-tests the abstraction boundary and increases confidence that a
future SPIFFE implementation will slot in without structural changes.

### Strategy 1: heartbeat-anchored (default)

Every node generates an ephemeral key pair at startup. Its public key is
published in the `HeartbeatRecord` under a `oneof credentials` field (see
below). Nodes present self-signed certificates over gRPC; peers verify them by
checking the presented public key against the live node set.

Trust is anchored in the persistence store: the ability to write a heartbeat
record (which requires access to the cluster's storage credentials) is the
implicit cluster membership proof.

**Security assumption:** write access to the persistence store is equivalent to
cluster membership. An attacker with persistence write access can publish a
public key and impersonate a node. This is acceptable because such an attacker
could also corrupt journals and KV entries, causing arbitrary Byzantine failure
regardless. The persistence store's access controls are the cluster's security
boundary. This assumption must be documented explicitly in any future ADR
covering this strategy.

#### On-demand verification refresh

If a peer presents a certificate whose fingerprint is not in the local live node
set (e.g. the node dialed before the local heartbeat read cycle ran), the
verifier triggers an immediate out-of-band read of the heartbeat keyspace and
retries the lookup.

A per-fingerprint negative cache with a TTL equal to the heartbeat interval
prevents this from becoming a denial-of-service vector. Source address rate
limiting is a secondary backstop.

### Strategy 2: pre-shared CA

The operator provisions a shared CA cert and key. Each node generates its own
key pair at startup and obtains a certificate signed by the CA key (or the
operator pre-provisions cert/key pairs). Peer verification is standard X.509
chain validation against the CA cert pool -- no heartbeat lookup is required.

This strategy is stronger than heartbeat-anchored under defense-in-depth: an
attacker with persistence write access cannot impersonate a node without also
possessing the CA private key, which lives out-of-band.

The `HeartbeatRecord`'s `oneof credentials` field is absent for pre-shared CA
nodes; the verifier does not consult the heartbeat store.

### `HeartbeatRecord` proto sketch

The `oneof credentials` field encodes strategy-specific trust material. Its
absence is meaningful -- it indicates an out-of-band trust mechanism is in use
(pre-shared CA, SPIFFE, etc.) and is not missing data.

```proto
message HeartbeatRecord {
    string address = 1;
    google.protobuf.Timestamp expires_at = 2;
    oneof credentials {
        SelfSignedCredentials self_signed = 3;
    }
}

message SelfSignedCredentials {
    bytes public_key = 1; // DER-encoded SubjectPublicKeyInfo
}
```

### Strategy 3: SPIFFE (future)

SPIFFE/SPIRE is a viable future extension for Kubernetes/cloud-native
environments where a SPIRE Agent runs co-located with each node.
`github.com/spiffe/go-spiffe/v2` provides `workloadapi.NewX509Source`, making
the implementation cost low. SPIRE Server is a required external dependency.

SVIDs rotate automatically (typically hourly); the public key changes on each
rotation. A SPIFFE implementation does not publish trust material in the
`HeartbeatRecord` -- the `oneof credentials` field is absent and verification
uses the SPIFFE trust bundle from the agent. The stable per-node identity is its
SPIFFE URI, derivable from the node UUID already in the heartbeat record.

The `CredentialsProvider` interface design is validated against this future
strategy: both `ClientCredentials()` and `ServerCredentials()` are satisfied by
`go-spiffe`'s output without any structural changes.

## gRPC services

There will be multiple gRPC services, grouped by functional area. Exact names
are TBD and will be decided when the proto files are written. The broad areas:

## Observer delivery

See also: [dogma API docs][dogma-observer] for `WithEventObserver()`.

When a command is submitted with `WithEventObserver()`, the submitter node
embeds an observer descriptor in the envelope baggage. The descriptor contains:

- The submitter node's gRPC address (for push delivery)
- A deadline
- An optional set of message type IDs to filter delivery

As the causal chain propagates, every handler invocation that produces events
checks for the observer descriptor in the envelope baggage. If present, the
handler node opens a gRPC connection to the submitter node and streams matching
events back to it.

**Causal scope:** `correlation_id` in `envelopepb.Header` identifies the root
of the causal chain. Any event whose envelope carries the same `correlation_id`
as the original command is in scope, regardless of how many handler hops it
took to get there. Events from unrelated concurrent work are excluded.

**Delivery semantics:** best-effort. If the submitter node is unreachable or
has crashed, the push fails silently and handler processing continues
uninterrupted. The deadline is the only termination signal -- there is no
"causal closure" notification. `ErrEventObserverNotSatisfied` may be returned
to the submitter if the deadline expires with no events delivered.

**Early cancellation (optimization, not required for correctness):** the
submitter could propagate a "done" signal to active handler nodes before the
deadline, allowing them to stop pushing.

## Event stream: historical vs. live-tail

**Open question:** should a consumer node read historical events directly from
the shared persistence journal (bypassing the owning node), transitioning to a
live-tail connection to the owning node only when it catches up to the stream
head?

The two phases have different characteristics:

- **Historical catchup:** the journal is in shared persistence; any node can
  read it directly without involving the owning node. Routing through the owner
  is an unnecessary hop.
- **Live tail:** new appends happen on the owning node's write path. Real-time
  notification requires a connection to the owner.

The seam between the two phases requires care: the consumer must transition from
direct journal reads to owner-pushed events without missing an event or
double-delivering at the boundary.

**Consumer-responsibility recovery:** when ownership transfers (graceful or
crash), the consumer is responsible for re-hashing the stream ID against the
updated live node set, dialing the new owner, and resuming from its last
confirmed offset. The server is stateless with respect to consumer position.
This is the same path for both graceful and crash recovery, chosen for
uniformity.

<!-- references -->

[intra-node-subsystem-interfaces.md]: intra-node-subsystem-interfaces.md
[dogma-observer]: https://pkg.go.dev/github.com/dogmatiq/dogma#WithEventObserver
