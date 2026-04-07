# Idempotent command submission

This document captures the design for the idempotent command submission
layer -- the mechanism by which the engine deduplicates commands
submitted with a caller-supplied idempotency key. This layer sits in
front of the base durable command execution system described in
[command-acceptance-path-rev2.md].

The base system (future ADR-6) makes every accepted command durable. It
does not distinguish keyed from unkeyed commands. This document covers
the addon layer (future ADR-7) that adds cluster-wide deduplication for
keyed commands.

---

## Relationship to the base system

The base system provides:

- Factspace-as-acceptance-record: the command envelope is written to the
  handler's factspace journal during the synchronous acceptance path.
- Dirty flags: per-node KV entries track entity liveness for recovery.
- Recovery: dirty flag enumeration finds entities with unfinished work;
  entity-to-entity handoff reroutes commands when routing has changed.

The idempotency layer adds a pre-check before the base acceptance path.
After the pre-check passes (or on first submission), the command enters
the base system unchanged. The two layers compose without interaction --
the base system never inspects idempotency keys, and the idempotency
layer never touches factspaces or dirty flags.

---

## Caller retry contract

By providing an idempotency key, the caller accepts responsibility for
retrying failed submissions with the same key. The engine may rely on
caller retry as the sole recovery mechanism for keyed commands. If the
caller does not retry after a failure during acceptance, the command may
be silently lost.

This contract is ratified as [Dogma ADR-31].

This is the key asymmetry between keyed and unkeyed commands:

- **Unkeyed commands**: the engine is solely responsible for durability.
  Once `ExecuteCommand` returns `nil`, the command will be executed
  regardless of crashes. The caller has no retry obligation.
- **Keyed commands**: durability is a shared responsibility. The engine
  provides deduplication; the caller provides recovery (retry).

The base system's dirty flag mechanism provides a free bonus: if the
command reached an entity's factspace before a crash, the base system
recovers it during restart -- before the caller retries. The caller's
retry then hits the idempotency journal and sees the command was already
accepted. This is never wrong, only early.

---

## Idempotency journal

The idempotency journal is a per-key journal keyed by
`(app, idempotency_key)`. Each journal stores the command envelope and
routing provenance (handler key, instance ID for aggregates).

### First submission

On the handler node (synchronous), before entering the base acceptance
path:

1. Append the command envelope and routing provenance to the
   idempotency journal `(app, idempotency_key)` at position 0.
2. If the append succeeds: proceed to the base acceptance path
   (dirty flag, factspace write, dispatch).
3. If `ConflictError`: another submission with this key already exists.
   Enter the retry path (see below).

The idempotency journal write happens before the base acceptance path.
If a crash occurs between the journal write and the factspace write,
the caller's retry will find the journal entry and re-enter the retry
path, which checks whether the command reached the factspace and
re-dispatches if needed.

### Retry path (ConflictError)

When a submission hits `ConflictError` on the idempotency journal
append, the command was already accepted under this key. The question is
whether it was fully accepted into the base system or crashed mid-way.

1. Read the existing journal entry at position 0 to recover the
   original routing provenance (handler key, instance ID).
2. Check the target entity's factspace for the command:
   - If present and completed: return success. The command was already
     handled.
   - If present and not completed: the base system will execute it via
     normal dirty flag recovery. Return success.
   - If absent: the original submission crashed between the idempotency
     journal write and the factspace write. Re-dispatch through the
     base acceptance path using the stored provenance.
3. Before re-dispatching, validate routing against the current
   application configuration. If routing has changed (different handler
   or different instance ID), use the current routing -- the provenance
   tells us where the command was originally targeted, but the current
   config is authoritative.

### Routing provenance

The idempotency journal entry stores the routing decision made at first
submission time:

- Handler key
- For aggregates: instance ID (from `RouteCommandToInstance()`)
- For integrations: accepting node UUID

This provenance serves two purposes:

1. **Factspace lookup on retry.** The retry path needs to check whether
   the command reached a specific factspace. Without provenance, it
   would not know which factspace to check.
2. **Routing change detection.** If the current routing differs from the
   stored provenance, the retry path knows the command needs
   re-routing rather than simple re-dispatch.

Provenance is a diagnostic record of what happened, not a routing
instruction. Current application configuration always takes precedence.

---

## Interaction with the base system

### Crash scenarios

The synchronous acceptance path for a keyed command has two durable
writes: the idempotency journal append and the factspace write. Crashes
can occur at three points:

1. **Before idempotency journal write.** No record exists. The caller
   retries and the submission proceeds as a first attempt.
2. **Between idempotency journal write and factspace write.** The
   journal entry exists but the command is not in any factspace. On
   retry, the retry path reads the journal, finds no factspace entry,
   and re-dispatches through the base acceptance path.
3. **After factspace write.** The command is in the factspace and
   covered by the dirty flag. The base system recovers it during
   restart. On retry, the retry path finds the command in the
   factspace and returns success.

In all cases, the command is executed exactly once (or not at all if the
caller never retries scenario 1).

### Ordering of writes

The idempotency journal write must precede the factspace write. This
matches the base system's own ordering (dirty flag before factspace) and
extends consistency: on any retry, the journal entry is guaranteed to
exist if the factspace entry exists.

The full write order for a keyed command is:

1. Idempotency journal append (position 0)
2. Dirty flag write (if entity not already loaded)
3. Factspace journal append

### No dirty flag interaction

The idempotency layer does not read or write dirty flags. The base
system's dirty flag mechanism handles entity liveness independently.
The "free bonus" (base system recovers the command before the caller
retries) is a consequence of the layered design, not an explicit
coordination point.

---

## Scope of deduplication

The idempotency journal provides cluster-wide deduplication. Any node
can check whether a key has been used by reading the journal at
`(app, idempotency_key)`. This is the only cluster-wide store in the
acceptance path -- all other stores (dirty flags, factspaces) are
node-scoped or entity-scoped.

Deduplication is permanent for the lifetime of the journal entry. The
journal entry persists until explicitly cleaned up. Cleanup strategy is
an open question (see below).

---

## What this layer does not do

- **Unkeyed command deduplication.** Unkeyed commands use UUIDv4 command
  IDs that do not collide. No cluster-wide dedup is needed. Process-
  produced commands have their own dedup via the process journal's OCC.
- **Execution.** The idempotency layer never calls `HandleCommand()` or
  writes to factspaces. It only gates entry to the base acceptance
  path.
- **Event observation.** `WithEventObserver` is orthogonal. The
  interaction between `WithEventObserver` and `WithIdempotencyKey` is a
  separate concern (see Open Question 8 in the big-picture plan).

---

## Dismissed alternatives

### Single acceptance path for all commands

The current ADR-6 (pre-revision) uses a single acceptance path that
handles both keyed and unkeyed commands, with the idempotency journal as
part of the core acceptance path for keyed commands. This couples
deduplication to durability, making the base system more complex without
benefit to unkeyed commands. Separating the layers keeps the base system
simple and uniform.

### Idempotency check in the base system

An alternative is to have the base system check the idempotency journal
during recovery (dirty flag enumeration). This would let the base system
skip re-execution of commands that the caller has already retried. But
the base system has no reason to know about idempotency keys -- its
recovery is based on factspace state, not caller identity. Adding this
check would violate the layering.

### Per-node idempotency store

Using a per-node store instead of a cluster-wide journal would avoid the
cluster-wide write on the synchronous path. But idempotency keys must
deduplicate across the entire cluster -- the caller may retry against a
different node. Per-node dedup is insufficient.

---

## Open questions

### OQ-1: Idempotency journal cleanup

The idempotency journal entry persists after the command completes. Over
time, completed entries accumulate. Options:

- **TTL-based expiry.** Delete entries older than a retention period.
  Simple but requires the persistence backend to support TTL or a
  background sweeper.
- **Completion-triggered delete.** Delete the journal entry when the
  handler subsystem confirms command completion. Requires a callback
  from the base system to the idempotency layer, which complicates the
  layering.
- **Lazy cleanup on read.** When a retry reads the journal and finds
  the command completed, delete the entry. Only cleans up entries that
  are actually retried.
- **No cleanup.** Accept unbounded growth. Journals are append-only
  and each entry is small. May be acceptable depending on the
  persistence backend.

### OQ-2: Retry path latency

The retry path reads the idempotency journal and then checks the target
factspace. This is two reads on the synchronous path (vs one write for
first submission). Is this acceptable, or should the retry path return
immediately after finding the journal entry and assume the base system
will handle it?

Returning immediately is simpler but means the caller gets `nil` without
knowing whether the command actually reached the factspace. If the
original submission crashed between the journal write and factspace
write, and the retry returns immediately, no one re-dispatches the
command until the next restart (dirty flag recovery). This may be
acceptable given that the base system's restart recovery will find and
handle it -- but only if the node restarts. If the node is still
running, the command is lost until idle unload triggers dirty flag
cleanup, which would find nothing and clear the flag.

The safe default is: the retry path always checks the factspace and
re-dispatches if absent.

### OQ-3: Concurrent retries

If two retries for the same idempotency key arrive simultaneously on
different nodes, both will read the journal entry and both may attempt
to re-dispatch. The base system's factspace OCC prevents duplicate
execution, but the double dispatch is wasted work. Is this a concern
worth addressing, or is it rare enough to ignore?

<!-- references -->

[command-acceptance-path-rev2.md]: command-acceptance-path-rev2.md
[Dogma ADR-31]: https://github.com/dogmatiq/dogma/blob/main/docs/adr/0031-require-retries-for-idempotency-keyed-commands.md
