# 7. Node heartbeat

Date: 2026-04-08

## Status

Accepted

- References [2. Rendezvous hashing for workload assignment][ADR-2]
- References [3. Optimistic conflict resolution][ADR-3]
- References [4. Ranked instruction routing][ADR-4]
- References [5. Homogeneous cluster nodes][ADR-5]
- Referenced by [8. Persistence isolation][ADR-8]
- Referenced by [11. Mutual TLS][ADR-11]

## Context

[Rendezvous hashing][ADR-2] assigns workloads to nodes deterministically, but it
requires every participant to work from the same input: the set of nodes
currently available to handle work. [Ranked instruction routing][ADR-4] requires
a network address for each candidate node. Neither mechanism specifies how this
information is obtained or kept current.

Every node in the cluster needs a consistent view of which nodes exist and how
to reach them. This view must address the following concerns:

- **Identity** — The UUIDs of all nodes in the cluster must be available for
  [rendezvous hashing][ADR-2].
- **Addressability** — [Ranked instruction routing][ADR-4] needs a network
  address for each candidate node.
- **Liveness** — Nodes that are not making progress must be excluded from the
  live node set within a bounded time to trigger [orphaned workload adoption].

The cluster has no coordinator that could maintain an authoritative member
list ([ADR-5]). Any mechanism for tracking which nodes are active must
therefore be self-organizing and tolerant of eventual consistency.

## Decision

Each node periodically writes a heartbeat record to a cluster-wide key-value
keyspace, keyed by its own node UUID. The heartbeat record carries two fields: a
network address for inter-node communication, and an expiry timestamp
recording when the record becomes invalid.

### Writing a heartbeat record

Writes happen on a fixed interval. Each write sets the expiry timestamp to a
fixed offset from the current time.

Writes are performed using [`Keyspace.Set()`], which enforces OCC. Since a node
is the sole legitimate writer to its own key, a conflict means another node is
using the same UUID — an unambiguous sign of misconfiguration — and is treated
as fatal. A node must write its initial heartbeat record before it starts
processing work.

If a write fails, the node retries continuously until its last committed record
expires. A node that cannot refresh its record before it expires must stop
accepting new work and shut down. Continuing to operate after expiry is
disruptive: its peers have already excluded it from the live node set and may
have begun adopting its workloads, generating unnecessary OCC conflicts.

On graceful shutdown, the node deletes its heartbeat record before its server
stops accepting connections.

### Reading heartbeat records

Each node also reads the full heartbeat keyspace at a fixed interval to rebuild
its live node set — the nodes considered active. It ignores any records with an
expiry time in the past.

### Pruning expired records

A node that has crashed or been permanently removed cannot delete its own
heartbeat record. Any peer that encounters an expired record can safely delete
it, since the record is already excluded from the live node set. This pruning
prevents unbounded growth of expired records, but it is not critical to
correctness.

### Dismissed alternatives

**Gossip protocols, including [SWIM],** offer fast failure detection without
polling. We considered adding a gossip layer but dismissed it on two grounds.

First, gossip treats a pairwise observation as a cluster-wide fact. Node A
being unreachable from node B does not mean node A is unreachable from node C.
Circulating "A is unreachable" causes nodes that can reach A to skip it in
[ranked instruction routing][ADR-4], misrouting work unnecessarily.

Second, gossip detects network unreachability, but what matters is whether a
node can commit work — which requires writing to storage, not being reachable
over the network. A node can be unreachable from some peers and still commit
work; another can be reachable from all peers and unable to commit due to a
storage fault. We use heartbeat writability as a proxy for general storage
access; network reachability is a weaker signal for a different thing.

**External service registries** — etcd, Consul, ZooKeeper — are purpose-built
for this problem and provide TTL-based membership management with
well-understood operational properties. However, requiring one would impose
additional operational complexity on every deployment. Operators who want to use
such a system can do so via a [`persistencekit`] driver, without any first-class
support in `runkit`.

**Recording the write timestamp** rather than an expiry time was also
considered. Under that design, each reader would apply its own expiry threshold
to determine whether a record is still valid. Recording the expiry time directly
instead encodes the validity window on the writer side, where the write interval
is already known, and removes the need for readers to agree on a shared
threshold through configuration alone.

### Timing values

- **Heartbeat interval** (5s) — How often a node writes its heartbeat record.
- **Refresh interval** (5s) — How often a node reads the heartbeat keyspace.
- **Grace period** (10s) — Extra time beyond the heartbeat interval before a
  record expires.

Each write sets the expiry time as:

$$\text{expires\_at} = \text{now} + \text{heartbeat\_interval} + \text{grace\_period}$$

This gives a retry window of 10s and a maximum detection lag of 20 seconds.

These values are a judgment call. Five-second intervals are frequent enough to
detect failures within a human-noticeable timeframe without generating excessive
storage load. The grace period gives comfortable headroom to retry a transient
storage write failure without unduly extending the detection lag. The values are
currently fixed constants; we are free to make them configurable if there is a
genuine operational need to tune detection lag or retry headroom.

## Consequences

The mechanism requires no dependencies outside the persistence layer already
required by [ADR-5], and introduces no coordinator, shared state, or external
service.

Storage liveness — not network reachability — is the operative definition of
node liveness. A node that loses its storage connection is excluded within 20
seconds of its last successful write, in the same way as a crashed node.

The live node set used for rendezvous scoring is always slightly out of date: it
reflects the state of the cluster up to 20 seconds ago. Two nodes may
transiently compute different rendezvous winners for the same workload as their
views converge after a membership change. [OCC][ADR-3] ensures no write is lost
during this window.

The expiry check is sensitive to clock skew: it compares the reader's clock
against an expiry timestamp written by the writer, so correctness depends on
clocks being loosely synchronized across the cluster. NTP-level synchronization
is assumed. [RFC 5905] reports typical accuracy of a few hundred microseconds
on fast LANs and a few tens of milliseconds in the general case — both
negligible against the 15s window (`heartbeat_interval` + `grace_period`).

We are free to add fields to the heartbeat record in a future ADR. If necessary,
we could treat any node whose record is missing an expected field as
incompatible and exclude it from the live node set.

This ADR introduces two terms to the [glossary]: **heartbeat record** and
**live node set**.

<!-- references -->

[ADR-2]: 0002-rendezvous-hashing-for-workload-assignment.md
[ADR-3]: 0003-optimistic-conflict-resolution.md
[ADR-4]: 0004-ranked-instruction-routing.md
[ADR-5]: 0005-homogeneous-cluster-nodes.md
[ADR-8]: 0008-persistence-isolation.md
[ADR-11]: 0011-mutual-tls.md
[glossary]: ../glossary.md
[`Keyspace.Set()`]: https://pkg.go.dev/github.com/dogmatiq/persistencekit/kv#Keyspace.Set
[orphaned workload adoption]: ../glossary.md#orphaned-workload-adoption
[`persistencekit`]: https://pkg.go.dev/github.com/dogmatiq/persistencekit
[RFC 5905]: https://www.rfc-editor.org/rfc/rfc5905
[swim]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
