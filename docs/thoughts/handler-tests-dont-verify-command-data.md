# Handler tests don't verify command data

The aggregate and integration controller tests verify that commands are
dispatched and that events appear on the stream, but none of them assert that
the correct command message was actually passed to the handler's
`HandleCommand` method.

A handler stub could receive a zero-value command (or the wrong command
entirely) and the tests would still pass as long as any event is recorded.

We should add assertions that confirm the handler receives the exact command
that was enqueued.

## Possibly related

- `internal/aggregate/controller_test.go`
- `internal/integration/controller_test.go`
