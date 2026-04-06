# Content-Addressed Message IDs (Research Notes)

This document records a research conversation about whether `MessageId` in
`envelopepb.Envelope` should be derived from envelope content rather than
generated as an opaque random UUID.

**Conclusion:** the property is real but its practical value is unclear. No
action recommended at this time.

---

## The Idea

A content-addressed `MessageId` means:

```
MessageId = hash(stable envelope fields)
```

where "stable" means fields whose values do not change across retries -- the
serialized payload, type ID, `CausationId`, `CorrelationId`, and source
provenance fields, but not `CreatedAt` or `ScheduledFor`.

The defining property is a bijection between ID and content: if two envelopes
share a `MessageId`, their content must be identical, and this is checkable
by anyone who can hash an envelope, not just the component that created it.

---

## Motivation

The current codebase already encodes the assumption that same ID implies same
content. The most explicit example is in the event stream's `deduplicate()`
function:

```go
// Sanity check: if we found a transaction with the same first event
// ID, it must contain the exact same events as the request. If not,
// either the request is malformed, or the journal is corrupted.
if len(events) != len(req.EventEnvelopes) {
    panic(...)
}
```

This assumption is upheld by convention today -- the engine controls ID
assignment and never assigns the same UUID to different content. Content
addressing would convert that convention into a computable guarantee. Any
component, at any time, could verify a received envelope without trusting its
sender.

In a multi-node deployment (Phase 9 gRPC), this has mild security value: a
node forwarding an envelope to another node cannot tamper with the content
without also invalidating the ID. The receiving node could detect the
inconsistency without an extra round-trip.

---

## The Dedup Tension (a side-effect, not the motivation)

Content-addressed IDs have a secondary consequence: same content produces the
same ID across independent submissions, which provides automatic dedup for
retry storms without any client-side idempotency key. This sounds useful but
conceals an irresolvable tension.

Two desirable properties cannot coexist:

1. **Retry safety**: a resubmitted command gets the same ID as the original,
   so the engine treats it as a duplicate and does not execute it twice.

2. **Distinct repeat occurrences**: two commands with identical payload but
   genuinely different business intent (e.g. "Withdraw $100" on different
   days) get different IDs and are both executed.

`CreatedAt` is in the envelope and could in principle disambiguate repeat
occurrences, but it does not resolve the tension -- it shifts which property
you sacrifice. If `CreatedAt` is included in the hash, retries at a different
wall-clock time get a different ID, eliminating retry safety. If it is
excluded, two distinct occurrences with the same payload are silently
collapsed into one -- which is data loss for events and unintended dedup for
commands.

A caller-supplied nonce in the payload is the only mechanism that cleanly
separates the two cases, and Dogma's `Command` interface has no such field.

Because the dedup consequence would require significant care to avoid silent
data loss (particularly in the event stream, where the same business event
type with the same payload can genuinely recur), this side-effect should not
be used to motivate the proposal. The integrity argument stands on its own
and does not require dedup semantics to change.

---

## What Would Change

If content addressing were adopted, the changes would be:

- **ID generation**: wherever envelopes are created, the `MessageId` would be
  computed as a hash (UUIDv5 is a natural fit, using a fixed namespace UUID
  and the marshaled stable fields as the name) rather than generated as a
  random UUID.
- **Verification**: any component receiving a forwarded envelope could
  recompute the expected ID and compare it to the received ID.
- **The dedup panic in `deduplicate()`**: the panic currently encodes the
  assumption that ID collision means same request. With content addressing,
  this becomes a computable invariant rather than a trusted convention, but
  the logic is otherwise unchanged. It would only become problematic if
  dedup semantics were also changed to act on the collision.

---

## Open Questions

- Is the verifiable-commitment property useful enough to justify the
  complexity of deterministic ID generation (especially hash function choice,
  field ordering, and schema stability)?
- Does content addressing interact badly with the `CreatedAt` field? If
  `CreatedAt` is excluded from the hash, is the resulting ID still useful as
  a human-readable correlation handle?
- If a multi-node integrity check is the goal, is a content-addressed ID the
  right mechanism, or is a separate content signature (over the full envelope
  including `MessageId`) more appropriate?
