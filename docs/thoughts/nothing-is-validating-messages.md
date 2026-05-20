# Nothing is validating messages

Neither the aggregate nor the integration controller calls `dogma.ValidateMessage`
(or any equivalent) before dispatching a command to a handler. The engine accepts
whatever the packer successfully unmarshals, even if the message violates its own
validation rules.

We should decide whether validation belongs in the engine (fail fast, nack
invalid messages) or whether it's the handler's responsibility to reject bad
input via its return value or a panic.

## Possibly related

- `internal/aggregate/worker.go` — `handleCommand`
- `internal/integration/worker.go` — `handleCommand`
- `engine.go` — `ExecuteCommand` (could validate before enqueuing)
