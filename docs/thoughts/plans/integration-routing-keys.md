# Plan: Integration Routing Key Derivation

This document describes the planned implementation of integration handler routing
key derivation. The formulas are fully specified by [ADR-0006]. No design
decisions remain; this is an implementation plan.

## What it is

When `ExecuteCommand()` is called for a command routed to an integration handler,
the engine must determine which cluster node should execute the command. [ADR-0006]
specifies three cases based on the handler's concurrency preference:

| Preference            | Routing key                   | Effect                                     |
| --------------------- | ----------------------------- | ------------------------------------------ |
| None declared         | — (no routing)                | Source node handles locally; no rendezvous |
| `MinimizeConcurrency` | `uuid5(app_key, handler_key)` | All commands funnel to one stable node     |
| `MaximizeConcurrency` | `command_uuid`                | Commands spread across nodes               |

The routing key is then passed to rendezvous hashing ([ADR-0002]) and ranked
instruction routing ([ADR-0004]) to select a destination node.

## Package

`internal/subsystem/integration`

The derivation function is a pure function with no I/O. It belongs inside the
integration subsystem package rather than a shared utility -- it encodes
integration-specific semantics, not general routing logic.

## Implementation

```go
// routingKey derives the rendezvous routing key for an integration command.
// Returns nil if no routing applies (no preference declared), indicating the
// command should be handled locally on the source node without rendezvous.
func routingKey(
    appKey     *uuidpb.UUID,
    handlerKey *uuidpb.UUID,
    pref       dogma.ConcurrencyPreference,
    commandID  *uuidpb.UUID,
) *uuidpb.UUID {
    switch pref {
    case dogma.MinimizeConcurrency:
        return uuidv5(appKey, handlerKey)
    case dogma.MaximizeConcurrency:
        return commandID
    default: // no preference
        return nil
    }
}
```

The `uuid5` derivation uses the standard UUID v5 algorithm (SHA-1 namespace
hash). The namespace is `app_key` and the name is `handler_key` serialized to
bytes. Confirm the byte representation of `handler_key` when implementing --
RFC 9562 UUID v5 requires a stable byte encoding of the name; using the raw
16-byte UUID representation is unambiguous and matches the ADR's notation.

> ADR-0006 writes `uuid5(app_key, handler_key)` -- treating `app_key` as the
> namespace UUID and `handler_key` as the name. This is the natural reading of
> RFC 9562 UUID v5 where the namespace is itself a UUID. Verify that
> `enginekit/uuidpb` or `google/uuid` has a suitable `NewSHA1` / `NewV5` helper
> before implementing.

## Where it fits in the acceptance path

The routing key is one input to the broader integration command acceptance path
described for Phase 4 in the big-picture plan. The acceptance path is not being
implemented here -- only the routing key derivation function that it will call.

Concurrency preference changes do not trigger rerouting (stated explicitly in
ADR-0006). The routing key derivation function has no memory of past decisions;
it is always computed fresh from current configuration.

## Out of scope

- Full command acceptance path (Phase 4)
- Rendezvous hashing (already implemented in `internal/rendezvous`)
- Ranked instruction routing (separate plan)
- Integration handler execution

## Dependencies

- `github.com/dogmatiq/dogma` -- `ConcurrencyPreference` constants
- `github.com/dogmatiq/enginekit/protobuf/uuidpb` -- UUID type
- Standard UUID v5 implementation (confirm package when implementing)

<!-- references -->

[ADR-0002]: ../../adr/0002-rendezvous-hashing-for-workload-assignment.md
[ADR-0004]: ../../adr/0004-ranked-instruction-routing.md
[ADR-0006]: ../../adr/0006-durable-command-executor.md
