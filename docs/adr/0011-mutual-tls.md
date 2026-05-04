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
hold a valid credential can communicate.

The mechanism by which a node obtains its certificate and verifies its peers
must be extensible, because the right approach varies by deployment. We will
support two strategies from day one.

1. **Self-signed certificates (default)**: Each node generates an ephemeral key
   pair at startup and publishes its public key in its [heartbeat
   record][ADR-7]. Rather than validating a CA chain, a peer reads the claimed
   node identity from the certificate, looks up that node's heartbeat record,
   and confirms the public key matches. No operator configuration is required.

   > [!WARNING]
   > An attacker with write access to the persistence store could publish a
   > public key and impersonate a node — but could just as easily manipulate
   > persisted state directly. The store's access controls are an adequate
   > security boundary for the default strategy.

2. **Pre-shared certificate authority (CA)**: Each node holds a certificate
   signed by a shared CA; peers verify using standard [X.509] chain validation
   and confirm the certificate identifies the expected node. The operator
   provisions certificates outside the cluster and is responsible for ensuring
   each certificate carries the correct node identity. An attacker with
   persistence write access cannot impersonate a node without also possessing
   the CA private key, which is never distributed to nodes.

## Consequences

Every cluster gets encrypted, mutually authenticated inter-node traffic without
any operator configuration. Deployments that need stronger guarantees can swap
in the pre-shared CA strategy, but the baseline is secure by default.

Under the self-signed strategy, a peer may connect before its public key has
been seen locally. The verifier must handle this gracefully without creating a
denial-of-service vector.

[SPIFFE]/SPIRE is a natural future extension: certificates are issued and
rotated by a co-located agent rather than published in the heartbeat store.
Implementing two strategies from day one helps strengthen the credential
abstraction early.

<!-- references -->

[ADR-5]: 0005-homogeneous-cluster-nodes.md
[ADR-7]: 0007-node-heartbeat.md
[mTLS]: https://en.wikipedia.org/wiki/Mutual_authentication#mTLS
[SPIFFE]: https://www.redhat.com/en/topics/security/spiffe-and-spire
[X.509]: https://en.wikipedia.org/wiki/X.509
