# Scopes Must Validate Messages and Their Types

Scopes (command scopes, event scopes, deadline scopes) should validate that the
messages passed to them are of the correct type according to the handler's route
configuration.

For example, if a process handler is configured with
`dogma.ExecutesCommand[*CommandStub[TypeX]]()`, calling `s.ExecuteCommand()` with
a `*CommandStub[TypeY]` should panic with `dogma.UnexpectedMessage` (or similar)
rather than silently accepting an unrouted message type.

This applies to:

- `AggregateCommandScope.RecordEvent()` — must be a routed event type
- `ProcessEventScope.ExecuteCommand()` — must be a routed command type
- `ProcessEventScope.ScheduleDeadline()` — must be a routed deadline type
- `ProcessDeadlineScope.ExecuteCommand()` — must be a routed command type
- `ProcessDeadlineScope.ScheduleDeadline()` — must be a routed deadline type
- `IntegrationCommandScope.RecordEvent()` — must be a routed event type

## Possibly Related

- `docs/thoughts/scope-panic-on-unrouted-message.md`
