# Audit all slog attributes

Do a pass over every `slog` attribute emitted in the codebase to check for
consistency:

- Key naming — snake_case, singular vs plural, grouping (e.g. `delivery.*`,
  `process_instance.*`).
- Attribute types — prefer `xslog.UUID` over `slog.String`, `xslog.Envelope`
  over ad-hoc fields, `slog.Duration` over stringified durations, etc.
- Levels — info vs warn vs error usage for similar events across pumps.
- Message text — verb tense, capitalization, sentence structure.
- Redundancy — attributes that duplicate information already in a parent group
  (e.g. logging `stream_id` after the logger is already scoped to a delivery).

Particularly: confirm the four pumps (aggregate, integration, process,
projection) emit symmetric attributes for symmetric events (acquire, handle,
postpone, OCC conflict, panic).

## Possibly related

- [internal/x/xslog/attr.go](../../internal/x/xslog/attr.go)
- [internal/messagepump/pump.go](../../internal/messagepump/pump.go)
- [internal/aggregate/commandpump.go](../../internal/aggregate/commandpump.go)
- [internal/integration/commandpump.go](../../internal/integration/commandpump.go)
- [internal/process/eventpump.go](../../internal/process/eventpump.go)
- [internal/process/deadlinepump.go](../../internal/process/deadlinepump.go)
- [internal/projection/eventpump.go](../../internal/projection/eventpump.go)
