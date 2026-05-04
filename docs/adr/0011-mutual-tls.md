# 11. Mutual TLS

Date: 2026-05-03

## Status

Proposed

> [!NOTE]
> This decision has not yet been accepted and is subject to change.

- References [5. Homogeneous cluster nodes][ADR-5]
- References [7. Node heartbeat][ADR-7]

## Context

Every node communicates directly with every peer ([ADR-5]) over the network.
Nodes may be deployed across hosts, data centers, or cloud availability zones
where the network path is not fully under operator control. A cluster should be
secure regardless of the network it runs on.

## Decision

We will require all inter-node connections to use [mutual TLS][mTLS]. Both
parties must present a certificate — not just the server — so only nodes that
hold a valid credential can communicate. Each certificate includes the node's
advertised network addresses as IP or DNS [subject alternative name][SANs] so
that dialers can verify the server using standard host verification.

The mechanism by which a node obtains its certificate and verifies its peers
must be extensible, because the right approach varies by deployment. We will
support two strategies from day one.

1. **Self-signed certificates (default)**: Each node generates an ephemeral key
   pair at startup and publishes its public key in its [heartbeat
   record][ADR-7]. The certificate also carries a URI SAN identifying the node
   by ID. Both sides of a connection verify the peer's public key against its
   heartbeat record: the dialer already knows the target node's identity; the
   receiver reads it from the URI SAN in the client certificate. No operator
   configuration is required.

   > [!WARNING]
   > An attacker with write access to the persistence store could publish a
   > fraudulent public key and impersonate a node. Combined with a privileged
   > network position, this enables interception of live inter-node traffic.
   > These are real risks, but the self-signed strategy is still strictly
   > better than the alternative default of no TLS. Deployments where the
   > store is not a sufficient trust boundary should use the pre-shared CA
   > strategy instead.

2. **Pre-shared certificate authority (CA)**: Each node holds a certificate
   signed by a shared CA; peers verify using standard [X.509] chain validation.
   The operator provisions certificates outside the cluster and is responsible
   for ensuring each certificate carries the correct address SANs. An attacker
   with persistence write access cannot impersonate a node without also
   possessing the CA private key, which is never distributed to nodes.

## Consequences

Every cluster gets encrypted, mutually authenticated inter-node traffic without
any operator configuration. Deployments that need stronger guarantees can swap
in the pre-shared CA strategy, but the baseline is secure by default.

Under the self-signed strategy, connections from nodes that have no published
heartbeat record are rejected. The verifier can consult the store directly
during the TLS handshake to resolve an unknown peer.

Because connections are bounded by heartbeat liveness ([ADR-7]), a node whose
heartbeat record expires or is removed is disconnected from all peers within one
heartbeat interval — no separate revocation mechanism is needed.

[SPIFFE]/SPIRE is a natural future extension: certificates are issued and
rotated by a co-located agent rather than published in the heartbeat store.
Implementing two strategies from day one helps strengthen the credential
abstraction early.

<!-- references -->

[ADR-5]: 0005-homogeneous-cluster-nodes.md
[ADR-7]: 0007-node-heartbeat.md
[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication#mTLS
[SANs]: https://en.wikipedia.org/wiki/Subject_Alternative_Name
[SPIFFE]: https://www.redhat.com/en/topics/security/spiffe-and-spire
[X.509]: https://en.wikipedia.org/wiki/X.509
