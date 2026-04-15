# 8. Persistence isolation

Date: 2026-04-09

## Status

Accepted

- References [2. Rendezvous hashing for workload assignment][ADR-2]
- References [7. Node heartbeat][ADR-7]

## Context

`runkit` may be deployed in a variety of configurations. A single operator may
run multiple independent clusters — for example, one per application, one per
region, or one per environment — and those clusters may share physical
persistence infrastructure such as a database server. Without isolation,
nodes from different clusters could interfere with each other: most critically,
they would see each other's heartbeat records ([ADR-7]) and treat each other
as peers, corrupting [rendezvous hashing][ADR-2] scores and misrouting work.

The question is whether the engine should enforce this isolation at the key
level, by embedding a cluster or site identifier in every storage key, or leave
it to the operator.

## Decision

We will not embed a cluster or site identifier in engine storage keys.
Operators are responsible for configuring each cluster to use a distinct
persistence namespace — a separate database, schema, key prefix, or equivalent
scoping provided by the backing store. The engine treats the persistence
configuration it receives as already scoped to its cluster.

### Dismissed alternatives

**Embedding a `cluster_key` in all storage keys.** Storage keys would become
self-describing: any record could be attributed to a cluster without consulting
deployment configuration. However, this requires a new required engine option
such as `WithClusterID`. The cluster boundary is an infrastructure concern:
operators already know which nodes form a cluster because they configure the
same persistence endpoints and peer addresses. Replicating that knowledge as
an engine identity adds no isolation that a properly configured namespace does
not already provide, at the cost of an additional identity the engine must
manage.

**Embedding a `site_key` in all storage keys.** Site identity is already a
first-class engine concept, so it would require no new option. However, a
site may contain multiple independent clusters — for example, one cluster per
application or per environment within a single region — so site identity does
not discriminate between them.

## Consequences

Operators must ensure each cluster uses a distinct persistence namespace.
Most persistence backends expose natural scoping mechanisms — separate
databases, schemas, or key prefixes — so this requirement typically maps
directly onto existing deployment practice rather than imposing new work.

Storage keys remain minimal, identifying logical entities — applications,
handlers, instances, nodes — without encoding deployment topology.

<!-- references -->

[ADR-2]: 0002-rendezvous-hashing-for-workload-assignment.md
[ADR-7]: 0007-node-heartbeat.md
