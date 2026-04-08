# Runkit glossary

This glossary defines terms specific to the runkit engine. Terms already
defined in the [Dogma glossary] are not redefined here.

[A](#a) •
B •
[C](#c) •
D •
E •
F •
G •
[H](#h) •
[I](#i) •
J •
[K](#k) •
[L](#l) •
M •
N •
[O](#o) •
P •
Q •
[R](#r) •
[S](#s) •
T •
[U](#u) •
V •
[W](#w) •
X •
Y •
Z

## A

### Application plane

The layer of the engine where application-defined [handler] logic executes.
[Dogma messages] — [commands], [events], and [timeouts] — exist in the
application plane.

See [control plane].

## C

### Confirmation

A [control message] sent in reply to an [instruction], carrying either a
positive or negative outcome. For example, the result of a command execution
attempt, which may succeed or fail.

### Control message

A message exchanged between engine components to coordinate work within the
[control plane]. [Instructions] and [confirmations] are control messages.
Distinct from [Dogma messages], which exist in the [application plane].

### Control plane

The layer of the engine responsible for coordination: workload assignment,
[instruction] routing, node discovery, conflict resolution, and failover.
Control plane components decide _where_ and _when_ work happens.

## H

### Heartbeat record

A record written periodically by each node to signal that it is still active
and to publish its network address.

See [live node set], [ADR-7].

## I

### Instruction

A [control message] sent from one engine component to another to request that
work be performed. For example, a request to execute a command on a specific
handler instance.

See [confirmation].

## K

### Keyed command

A [command] executed with an application-defined [idempotency key] via
[`WithIdempotencyKey`].

See [unkeyed command].

## L

### Live node set

The set of cluster nodes currently considered active. It is the source of
network addresses used for [ranked instruction routing].

See [heartbeat record], [ADR-7].

## O

### Orphaned workload adoption

The process by which a surviving node takes ownership of workloads
belonging to a node that is no longer alive. The [recovery index]
makes those workloads naturally discoverable.

See [live node set], [ADR-6], [ADR-7].

## R

### Rendezvous hashing

A coordination-free algorithm for deterministically assigning an input to one of
a set of candidates. Any participant with the same input and candidate set
independently computes the same result. When candidates are added or removed,
only the inputs assigned to the affected candidate are reassigned.

See [self-affinity], [ADR-2].

### Ranked instruction routing

The procedure by which a source node selects a destination node for an
[instruction]. Candidates are ranked by [rendezvous hashing] score; the source
node offers the instruction to each in descending order until one accepts,
falling back to itself if none do.

See [ADR-4].

### Recovery index

A per-node index that records which aggregate instances and integration handlers
currently have work in progress on that node. On startup, a node iterates its
recovery index to locate unfinished work without scanning every handler's data
store in the cluster.

See [ADR-6].

### Rerouting

The process of moving a pending [command] from one handler's data store to
another in response to a change in the application's routing configuration.

See [ADR-6].

## S

### Self-affinity

A property of the [rendezvous hashing] implementation that guarantees a
candidate always wins when the input matches its own UUID. This enables
per-candidate private partitions: a candidate can use its own UUID as an input,
knowing it will always select itself while it remains in the candidate set. If
the candidate leaves the set, another candidate inherits ownership through
normal rendezvous selection.

See [ADR-2].

## U

### Unkeyed command

A [command] executed without an application-defined [idempotency key].

See [keyed command].

## W

### Workload

A discrete unit of processing that the engine assigns to a cluster node using
[rendezvous hashing].

<!-- anchors -->

[application plane]: #application-plane
[command]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#command
[commands]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#command
[confirmation]: #confirmation
[confirmations]: #confirmation
[control message]: #control-message
[control plane]: #control-plane
[Dogma messages]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#message
[events]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#event
[handler]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#handler
[heartbeat record]: #heartbeat-record
[idempotency key]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#idempotency-key
[instruction]: #instruction
[instructions]: #instruction
[keyed command]: #keyed-command
[live node set]: #live-node-set
[ranked instruction routing]: #ranked-instruction-routing
[recovery index]: #recovery-index
[rendezvous hashing]: #rendezvous-hashing
[self-affinity]: #self-affinity
[timeouts]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#timeout
[`WithIdempotencyKey`]: https://pkg.go.dev/github.com/dogmatiq/dogma#WithIdempotencyKey
[unkeyed command]: #unkeyed-command

<!-- ADRs -->

[ADR-2]: adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-4]: adr/0004-ranked-instruction-routing.md
[ADR-6]: adr/0006-durable-command-executor.md
[ADR-7]: adr/0007-node-heartbeat.md

<!-- references -->

[Dogma glossary]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md

<!-- project-specific rules

These notes capture conventions specific to this glossary. Agents should check
these rules in addition to the general glossary skill.

- Do not link "engine" to the Dogma glossary. This repository is the engine;
  the word is self-evident in context. Use it as plain text.
- Do not use bare [message] or [messages] references. Dogma messages must be
  written as [Dogma message] or [Dogma messages] to avoid ambiguity with
  control messages.
-->
