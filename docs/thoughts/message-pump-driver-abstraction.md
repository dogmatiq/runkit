# Abstract common code in message pump drivers

Each handler type (`aggregate`, `integration`, `process` × 2, `projection`)
implements its own `messagepump.Driver`. Across those five implementations
there is substantial duplication that could be factored into shared helpers,
either in `internal/messagepump` or a new sub-package.

## Duplicated concerns

### `AcquireDelivery` for the command queue

`aggregate.CommandPump` and `integration.CommandPump` have an identical
implementation: a `SELECT ... FOR UPDATE SKIP LOCKED` from
`commandqueue.commands` filtered by `message_type_id`, then scanning into a
`messagepump.Delivery`. This could be a single `commandqueue.Acquire()` (or
similar) helper.

### `AcquireDelivery` for event streams

`process.EventPump` and `projection.EventPump` share almost all of the
"acquire next event on a stream, else advance checkpoint" logic. The
projection variant additionally deals with the "no checkpoint yet →
initialize from handler" case, but the underlying query and the
end-of-stream advancement pattern are the same.

### `PostponeDelivery`

Two shapes exist:

- Queue-based (`aggregate`, `integration`, `process.DeadlinePump`): update
  `deliver_at` + `failures` for the row identified by `message_id`.
- Stream-based (`process.EventPump`, `projection.EventPump`): update
  `resume_at` + `failures` on `eventstream.handler_checkpoints` for the
  handler/stream pair.

Both shapes are identical between their respective consumers, save for the
table name in the queue-based variant.

### `advanceCheckpoint`

`process.EventPump.advanceCheckpoint` and
`projection.EventPump.advanceCheckpoint` are the same query wrapped the same
way.

### Envelope unmarshalling

Every driver starts `HandleDelivery` with the same 3–4 lines: call
`xmessage.UnpackMessage`, log via `xslog.Error` on failure, return
`messagepump.ErrFailed`. A helper like `unpackForHandling(logger, envelope,
&msg)` would replace the boilerplate.

### Panic-to-error wrapping around handler calls

Every call into user code follows the same shape:

```go
if err := xerrors.ConvertPanicToError(func() error { ... }); err != nil {
    logger.ErrorContext(ctx, "unable to ...", xslog.Error(err))
    return messagepump.ErrFailed
}
```

A `callHandler(ctx, logger, msg, fn)` helper (or a small typed wrapper per
callback shape) would consolidate this.

## Possible shape

- Split the driver interface into two orthogonal concerns:
  - **Source** — how a delivery is acquired and postponed (commandqueue vs.
    eventstream). Two concrete implementations, injected into each pump.
  - **Sink** — what to do with the acquired delivery (handler-specific).
- Move `xmessage.UnpackMessage` + `xerrors.ConvertPanicToError` wrappers
  into `messagepump` so pumps don't need to import them directly.

## Trade-offs to consider

- The projection event pump has extra behaviours (checkpoint initialisation
  from the handler, OCC conflict warnings) that a naive shared helper would
  need to accommodate without pushing every pump toward the union of all
  features.
- The `process.DeadlinePump` acquire query intentionally joins
  `process.instances` for lock ordering — a shared "queue acquire" would
  need to remain flexible enough to allow driver-specific joins.
- Consolidation reduces per-driver code but adds indirection, which may
  make individual drivers harder to read in isolation.

## Possibly related

- [`internal/messagepump/driver.go`](../../internal/messagepump/driver.go) — driver contract
- [`internal/aggregate/commandpump.go`](../../internal/aggregate/commandpump.go)
- [`internal/integration/commandpump.go`](../../internal/integration/commandpump.go)
- [`internal/process/eventpump.go`](../../internal/process/eventpump.go)
- [`internal/process/deadlinepump.go`](../../internal/process/deadlinepump.go)
- [`internal/projection/eventpump.go`](../../internal/projection/eventpump.go)
