# Runkit glossary

This glossary defines terms specific to the runkit [engine]. Terms already
defined in the [Dogma glossary] are not redefined here. Each term links back to
the ADR that introduced it.

[Dogma glossary]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md

A •
B •
C •
D •
E •
F •
G •
H •
I •
J •
K •
L •
M •
N •
O •
P •
Q •
[R](#r) •
[S](#s) •
T •
U •
V •
W •
X •
Y •
Z

## R

### Rendezvous hashing

A coordination-free algorithm for deterministically assigning an input to one of
a set of candidates. Any participant with the same input and candidate set
independently computes the same result. When candidates are added or removed,
only the inputs assigned to the affected candidate are reassigned.

See also [self-affinity], [ADR-0002].

## S

### Self-affinity

A property of the [rendezvous hashing] implementation that guarantees a
candidate always wins when the input matches its own UUID. This enables
per-candidate private partitions: a candidate can use its own UUID as an input,
knowing it will always select itself while it remains in the candidate set. If
the candidate leaves the set, another candidate inherits ownership through
normal rendezvous selection.

See also [ADR-0002].

<!-- anchors -->

[engine]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#engine
[rendezvous hashing]: #rendezvous-hashing
[self-affinity]: #self-affinity

<!-- ADRs -->

[ADR-0002]: adr/0002-rendezvous-hashing-for-workload-assignment.md
