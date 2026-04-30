# Observability

This document captures thought-through design decisions for OpenTelemetry
observability in runkit. It covers all three OTel signals: traces, metrics, and
logs.

## Providers

One `TracerProvider`, one `MeterProvider`, one `LoggerProvider` -- one per
signal type. The `enginekit/telemetry.Provider` struct bundles all three and is
the type used in the public API.

Public API options:

```go
WithTelemetry(p *telemetry.Provider)
WithGlobalTelemetry()  // captures OTel globals at call time, wraps in *telemetry.Provider
```

`*telemetry.Provider` zero value is safe -- all three underlying providers fall
back to their respective OTel noop implementations.

There are no separate per-signal options. Callers who want to configure
providers individually construct a `*telemetry.Provider` directly (three field
assignments).

Runkit uses `enginekit/telemetry` directly throughout, with no intermediate
abstraction layer.

## Traces

Standard context-parented OTel spans from the caller's ctx and gRPC middleware.

Persistence operations (`persistencekit`-instrumented journal/kv/set calls)
appear as child spans of the enclosing handler span.

All spans carry a standard base attribute set:

- `dogma.site_name` -- from `WithSiteIdentity()` name
- `dogma.application_name` -- from Dogma application config (where applicable)
- `dogma.handler_name` -- from Dogma handler config (where applicable)

All are human-readable names, not UUIDs or keys. Names are the right dimension
for dashboards and alerts; IDs are for program-to-program references.

OTel resource attributes (`service.name`, `service.version`, etc.) are set by
the operator via standard OTel SDK resource configuration -- runkit neither sets
nor documents them.

OCC retries and other high-frequency internal mechanics are recorded as span
attributes (e.g. `occ.attempts = 3`), not as log records or span events.

## Metrics

### Application event counters

One counter per event type, named `dogma.event.<TypeName>` (e.g.
`dogma.event.OrderPlaced`). Fires only on durably committed events -- not
commands, not timeout scheduling, not timeout delivery. Events are the canonical
record of things the application did; commands are requests and timeouts are
scheduling mechanics.

Cardinality is bounded by the number of event types in the application, which
is fixed at compile time. Aggregate instance IDs and other unbounded identifiers
are never used as metric attributes.

Each counter carries: `dogma.site_name`, `dogma.application_name`,
`dogma.handler_name`.

### Infrastructure metrics

Message volume metrics (commands routed, timeouts scheduled, timeouts fired,
etc.) for SRE/ops use. May carry `dogma.message_type` where useful.

Metric names and units per subsystem (heartbeat, aggregate, integration,
process, projection, event stream) are TBD at implementation time.

## Logs

Two severity levels only: `Info` and `Error`. There is no `Warn`, `Debug`, or
`Trace`. The rule: if a function returns an error, log it at `Error`; otherwise
`Info`. There is no escalation logic for high-frequency mechanics.

- `HandlerScope.Log(format, args)` -- buffered in the scope during handler
  execution, emitted at `Info` at commit time under the active span context.
  Discarded if the execution does not commit. Because OTel log-trace correlation
  is ID-based, the log records are correlated to the commit span without the
  span needing to be active at emit time.
- Engine-internal records (heartbeat writes, routing events, etc.) -- emitted
  at `Info` or `Error`.

OTel log records automatically carry `trace_id` and `span_id` from the active
span context, giving log-trace correlation in any compliant backend. This is the
only log-trace coupling -- logs do NOT additionally emit span events.

Errors are both logged and marked on the active span (`span.RecordError` +
`span.SetStatus(codes.Error)`). These always happen together.

## Open questions

- Dynamic instrument creation for `dogma.event.<TypeName>` counters: event type
  names are not known until handlers are registered (or until a new type is
  first committed). OTel allows lazy instrument creation but the interaction
  with some exporters' cardinality management needs verification.
- Control-plane metric names and units per subsystem (heartbeat, aggregate,
  integration, process, projection, event stream).
- Sampling strategy documentation for operators.
