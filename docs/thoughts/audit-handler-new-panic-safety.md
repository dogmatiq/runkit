# Audit `Handler.New()` calls for panic safety

All call sites that invoke a user-supplied `Handler.New()` should be wrapped in
`xerrors.ConvertPanicToError` so that a panicking constructor is converted into
an `ErrFailed`-style outcome rather than tearing down a worker goroutine.

`aggregate.CommandPump.newRoot` already does this. The two process pumps don't:

- `internal/process/eventpump.go` — `root := p.Handler.New()`
- `internal/process/deadlinepump.go` — `root := p.Handler.New()`

These will be revisited during the process pump migration to `messagepump.Driver`,
but the audit should be a deliberate step, not a side effect.

Worth checking projection too if/when it grows a `New()`-like hook.

## Possibly related

- [internal/aggregate/commandpump.go](../../internal/aggregate/commandpump.go)
- [internal/process/eventpump.go](../../internal/process/eventpump.go)
- [internal/process/deadlinepump.go](../../internal/process/deadlinepump.go)
- [internal/x/xerrors/recover.go](../../internal/x/xerrors/recover.go)
