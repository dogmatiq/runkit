# 5. Durable idempotent command handoff

Date: 2026-04-06

## Status

Proposed

- References [3. Optimistic conflict resolution][ADR-3]
- References [4. Ranked instruction routing][ADR-4]
- References [5. Homogeneous cluster nodes][ADR-5]

> [!NOTE]
> This decision has not yet been accepted and is subject to change.

## Context

The Dogma API describes [`CommandExecutor.ExecuteCommand()`] as returning once
the engine has taken ownership of the command, not necessarily when handling is
complete.

Runkit always handles commands asynchronously, as established by [ADR-5].
Ownership is confirmed to the caller before execution begins, and execution may
happen on a different node entirely. That means a crash or node loss between
ownership confirmation and completion will silently drop the command unless the
engine takes explicit steps to prevent it. The design must therefore guarantee
three properties:

- **Durability.** The command must be persisted so that it survives a node
  restart or permanent failure.

- **Recovery.** If execution does not complete, it must be resumed. The
  recovery contract differs depending on whether the caller supplied an
  idempotency key using [`WithIdempotencyKey()`].

- **Idempotency.** Because recovery may re-submit a command, repeated
  submission must not produce duplicate execution. Commands submitted with
  [`WithIdempotencyKey()`] supply a caller-controlled stable identifier the
  engine can use for deduplication. Commands submitted without a key carry no
  such identifier; the engine must provide its own mechanism instead.

## Decision

We will satisfy the durable handoff requirement using two distinct paths. We
call a command that is executed using `WithIdempotencyKey()` a [keyed command],
and one without an [unkeyed command]. Each path addresses durability, recovery,
and idempotency differently.

### Unkeyed commands

For an [unkeyed command], the engine is responsible for recovery.

The source node routes the command to a destination node using [ranked
instruction routing][ADR-4]. The destination node resolves the handler and any
handler-specific routing information needed to execute the command later. For an
aggregate, that includes the instance ID from `RouteCommandToInstance()`. For
an integration, it includes the handler identity and any routing choice implied
by the handler's concurrency preference.

To accept the command durably, the destination node writes an [unkeyed command
scratchspace] entry that contains the command envelope and the resolved routing
information needed to resume execution after restart. The keyspace is private to
the destination node so that restart recovery scans only that node's accepted
commands. After the write succeeds, the command is dispatched to the handler
subsystem in memory and acceptance is confirmed to the caller.

Recovery for unkeyed commands is engine-managed. On restart, a node enumerates
its own [unkeyed command scratchspace] and resumes any command whose handler
factspace does not already show completion. If a node dies permanently, another
node adopts its scratchspace entries and repeats the same completion check
before resuming work.

### Keyed commands

For a [keyed command], the caller provides the stable identity used at the
handoff boundary, namely the idempotency key supplied to `WithIdempotencyKey()`.

To accept the command durably, the destination node appends the command
envelope to the [keyed command factspace] for that idempotency key at position
`0`. The [optimistic conflict resolution][ADR-3] mechanism makes this naturally
idempotent: a conflicting append means the command was already accepted under
the same key, and the new submission is treated as a no-op success. After the
append succeeds, the command is dispatched to the handler subsystem in memory
and acceptance is confirmed to the caller.

Keyed commands do not use [unkeyed command scratchspace]. Recovery is
caller-managed: if submission fails, the caller retries with the same
idempotency key and the [keyed command factspace] deduplicates the retry.

### Handler factspaces

The handoff contract depends on the keying of each handler's authoritative
[factspace], because recovery must be able to tell whether a handed-off command
already completed.

For aggregates, the factspace is keyed per instance. Multiple commands may
target the same aggregate instance, so completion and conflict detection are
properties of the instance lifecycle.

For integrations, the factspace is keyed per command. This avoids creating OCC
contention between unrelated commands and makes completion checks independent of
which node executes the command.

The internal record layout of those factspaces is not decided here.

### Routing validation before execution

The durable handoff records the routing decision that was valid at submission
time. Execution may happen later, after deployment changes. Before loading a
handler cold and executing the command, the destination node must validate that
the recorded handler can still accept the command under the current application
configuration.

If the command is still routable, execution continues. If the command is no
longer routable, it is moved to the poison backlog. The exact rerouting logic
for aggregates and the exact execution policy for integrations are deferred to
their subsystem ADRs.

### Dismissed alternatives

We considered several alternatives:

- **A single durable path for all commands.** Writing the same cluster-wide
  acceptance record for keyed and unkeyed commands would give one uniform
  mechanism. However, it would ignore the key distinction in where stable
  identity comes from. Keyed commands already have a caller-supplied identifier
  and a caller retry contract, so forcing them through the same recovery path as
  unkeyed commands adds synchronous work without adding correctness.

- **Two synchronous writes for every command.** An earlier design wrote both a
  per-node scratchspace entry and a cluster-wide per-command factspace entry for
  unkeyed commands. This gives a very direct recovery model, but once the
  scratchspace entry contains the full envelope and routing data, the extra
  per-command factspace no longer carries its weight. It adds latency on the
  caller-facing path without providing a distinct correctness property.

- **Set-backed scratchspace.** A set is attractive because the recovery use case
  begins with enumeration. However, the durable handoff record must store the
  command envelope and routing data, not just membership. A key-value store fits
  the actual data shape; a set does not.

- **Cluster-wide scratchspace.** One shared scratchspace would allow any node to
  enumerate every unkeyed command directly. However, restart scans would then be
  proportional to the whole cluster's accepted workload instead of a single
  node's share. Per-node scratchspace keeps recovery naturally partitioned while
  still allowing dead-node adoption.

## Consequences

Keyed and unkeyed commands pay different synchronous costs because they rely on
different recovery contracts. This is intentional. Caller-keyed commands reuse
caller-supplied identity and caller retry. Unkeyed commands pay for
engine-managed recovery instead.

The common synchronous path is short. Each command requires one durable write,
followed by an in-memory dispatch. This is a consequence of the chosen split
handoff model, not an independently optimised goal.

Unkeyed recovery is naturally partitioned by node. A node restart scans only
its own [unkeyed command scratchspace], and dead-node adoption scans only the
failed node's share.

Keyed commands can leave behind orphaned [keyed command factspace] entries if a
caller provides an idempotency key and never retries after failure. We accept
that tradeoff because the caller chose the keyed recovery contract.

This ADR does not decide how handlers execute commands after handoff, how the
poison backlog is implemented, or how `WithEventObserver()` detects completion.
It only defines the durable, idempotent boundary between submission and
asynchronous execution.

<!-- references -->

[ADR-3]: 0003-optimistic-conflict-resolution.md
[ADR-4]: 0004-ranked-instruction-routing.md
[ADR-5]: 0005-homogeneous-cluster-nodes.md
[`CommandExecutor.ExecuteCommand()`]: https://pkg.go.dev/github.com/dogmatiq/dogma#CommandExecutor.ExecuteCommand
[`WithIdempotencyKey()`]: https://pkg.go.dev/github.com/dogmatiq/dogma#WithIdempotencyKey
[factspace]: ../glossary.md#factspace
[keyed command]: ../glossary.md#keyed-command
[keyed command factspace]: ../glossary.md#keyed-command-factspace
[unkeyed command]: ../glossary.md#unkeyed-command
[unkeyed command scratchspace]: ../glossary.md#unkeyed-command-scratchspace
