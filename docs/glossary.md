# Runkit glossary

This glossary defines terms specific to the runkit engine. Terms already
defined in the [Dogma glossary] are not redefined here.

[A](#a) •
B •
[C](#c) •
[D](#d) •
E •
[F](#f) •
G •
H •
[I](#i) •
J •
[K](#k) •
L •
M •
N •
O •
P •
Q •
[R](#r) •
[S](#s) •
T •
[U](#u) •
V •
W •
X •
Y •
Z

## A

### Aggregate factspace

A [factspace] that holds the operational metadata needed to reload an
[aggregate instance] and detect conflicts; event data itself lives in
the stream.

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

## D

### Dead-node adoption

The process by which a surviving node takes ownership of resources
belonging to a node that is no longer alive. [Self-affinity] makes the
dead node's resources naturally discoverable.

## F

### Factspace

A private, authoritative store for a specific entity, with a permanent
lifetime. A factspace is not tied to a particular storage primitive — it
may be backed by a journal, key-value entries, or a combination.

See [aggregate factspace], [integration factspace], [scratchspace].

## I

### Instruction

A [control message] sent from one engine component to another to request that
work be performed. For example, a request to execute a command on a specific
handler instance.

See [confirmation].

### Integration factspace

A [factspace] that holds the idempotency and completion state for a
single [command] handled by an [integration].

## K

### Keyed command

A [command] executed with an application-defined [idempotency key] via
[`WithIdempotencyKey`].

See [unkeyed command].

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

## S

### Scratchspace

A private, transient store that exists for the duration of a specific
operation or unit of work. Like a [factspace], a scratchspace is not tied
to a particular storage primitive.

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

<!-- anchors -->

[aggregate factspace]: #aggregate-factspace
[aggregate instance]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#aggregate-instance
[application plane]: #application-plane
[command]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#command
[commands]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#command
[confirmation]: #confirmation
[confirmations]: #confirmation
[control message]: #control-message
[control plane]: #control-plane
[dead-node adoption]: #dead-node-adoption
[Dogma messages]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#message
[events]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#event
[factspace]: #factspace
[handler]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#handler
[idempotency key]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#idempotency-key
[instruction]: #instruction
[instructions]: #instruction
[integration]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#integration
[integration factspace]: #integration-factspace
[keyed command]: #keyed-command
[ranked instruction routing]: #ranked-instruction-routing
[rendezvous hashing]: #rendezvous-hashing
[scratchspace]: #scratchspace
[self-affinity]: #self-affinity
[timeouts]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#timeout
[`WithIdempotencyKey`]: https://pkg.go.dev/github.com/dogmatiq/dogma#WithIdempotencyKey
[unkeyed command]: #unkeyed-command

<!-- ADRs -->

[ADR-2]: adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-4]: adr/0004-ranked-instruction-routing.md

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
