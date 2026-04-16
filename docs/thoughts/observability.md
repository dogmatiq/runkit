# Observability

This document captures thought-through design decisions for OpenTelemetry
observability in runkit. It covers all three OTel signals: traces, metrics, and
logs.

## Two-plane model

All observability signals belong to one of two planes, using the glossary terms:

- **Application plane** (`dogma.plane = "application"`) -- business-logic
  signals for **application developers**: Dogma message handling, causality,
  handler decisions, committed events. A faithful record of what the application
  did and why.
- **Control plane** (`dogma.plane = "control"`) -- infrastructure signals for
  **SRE/ops**: heartbeat writes, ranked instruction routing, OCC retries, node
  membership, persistence mechanics.

Every span, metric, and log record carries `dogma.plane` so an OTel Collector
can route them to different backends (e.g., product APM vs. infrastructure
monitoring). The engine provides no per-plane provider options; routing is
delegated to the Collector or to a custom `TracerProvider` wrapping.

## Providers

One `TracerProvider`, one `MeterProvider`, one `LoggerProvider` -- one per
signal type, shared by both planes. The `enginekit/telemetry.Provider` struct
bundles all three and is the type used in the public API.

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

## Application plane traces

### Causal DAG model

The application plane is primarily a tool for **application developers** debugging
business logic -- understanding why a specific causal chain did or did not
produce an expected outcome. It is not a control-plane SRE tool.

The plane boundary: **application code vs. engine infrastructure**. Handler code
is application code; OCC retries, journal reads, and routing are infrastructure.

The DAG has two node types, each represented by its own OTel span in its own
trace:

- **Message spans** -- one per durably committed message (`PlaceOrder`,
  `OrderPlaced`, etc.). Span name is the message type name. Emitted at commit
  time on the producing node.
- **Handler spans** -- one per successful handler execution (e.g. `OrderProcess
handled OrderPlaced`). Emitted at commit time on the handling node. Carries
  `scope.Log()` output as log records.

Span links encode causality as backward-pointing edges (effect → cause). Each
hop alternates between node types: handler span links to the inbound message
span; outbound message spans link to the handler span that produced them.

```
PlaceOrder ← [Agg handled PlaceOrder] ← OrderPlaced ← [ProcessA handled OrderPlaced] ← ProcessA_Cmd
(unlinked                                             ← [ProcessB handled OrderPlaced]
 root)
```

Backends that reverse-index links (Jaeger, Tempo, Honeycomb, Datadog) present
this as a forward causal graph in the UI.

Externally submitted commands (via `CommandExecutor`) are unlinked roots -- the
submission is infrastructure and does not appear in the app-plane DAG.

The Dogma `CorrelationId` UUID is NOT used as the OTel trace ID -- causal chains
can span hours or days, and backends handle very large traces poorly.

### Span context lifecycle

Each envelope carries exactly one OTel span context -- the message's own
(`traceID`, `spanID`, `traceFlags`). It is generated at envelope construction
time (`scope.RecordEvent`, `scope.ExecuteCommand`, etc.), stored in the
envelope, and is stable across OCC retries: the envelope is constructed once
and carried through unchanged.

The producing handler's span context does NOT travel in the envelope. The
handler span and the outbound message spans it produces are always emitted
together on the same node at commit time, so the handler span context is
available locally when the message spans are constructed.

At commit time the engine emits:

1. **Handler span** -- fresh trace root. Links to the inbound message's span
   context (from the inbound envelope) via `trace.WithLinks()`. Start time =
   handler invocation time. End time = durable commit time. Buffered
   `scope.Log()` records emitted under this span's context.
2. **Outbound message spans** -- one per committed message, each a fresh trace
   root. Links to the handler span (available locally) via `trace.WithLinks()`.
   Start time = `envelope.createdAt`. End time = durable commit time.

For externally submitted commands there is no inbound handler, so the handler
span is omitted. The message span is an unlinked root.

Nothing is emitted for executions that do not result in a durable commit.

### Timeouts

Timeouts have equal footing with commands and events: one message span per
durably committed timeout. The years-long gap between scheduling and firing is
visible in the causal edge timestamps, not a span duration. If the original
timeout span has been TTL'd from the backend before it fires, `correlation_id`
query-based reconstruction (see below) applies.

### ProcessScope.ExecuteCommand (no ctx)

`ProcessScope.ExecuteCommand(cmd)` takes no context. The new command envelope
carries the command's own pre-generated span context. At commit time the
command message span links to the handler span, which is available locally.
No external context injection is needed.

### Cross-service propagation

Deferred. If ever added, it would be an explicit `ExecuteCommandOption` (e.g.,
`WithCausalContext(spanCtx)`) -- not implicit ctx propagation.

### Span attributes

All signals on both planes carry a standard base attribute set:

- `dogma.site_name` -- from `WithSite()` name
- `dogma.application_name` -- from Dogma application config (where applicable)
- `dogma.handler_name` -- from Dogma handler config (where applicable)

All are human-readable names, not UUIDs or keys. Names are the right dimension
for metrics and dashboards; IDs are for program-to-program references.
OTel resource attributes (`service.name`, `service.version`, etc.) are set by
the operator via standard OTel SDK resource configuration -- runkit neither sets
nor documents them.

Every application-plane span additionally carries:

- `dogma.plane = "application"`
- `dogma.correlation_id` -- the `Header.CorrelationId` UUID

Message spans additionally carry:

- `dogma.message_id` -- the `Body.MessageId` UUID
- `dogma.causation_id` -- the `Header.CausationId` UUID (if set)

The `CorrelationId` attribute is the primary resilience mechanism for long
causal chains. When span links are broken by TTL or backend limitations,
operators can reconstruct the DAG by querying `dogma.correlation_id = <X>`
across all retained spans.

## Control plane traces

Standard context-parented OTel spans from the caller's ctx and gRPC middleware.
Not parented to application-plane spans.

Persistence operations (`persistencekit`-instrumented journal/kv/set calls)
appear as child spans of the control-plane handling span. The control-plane ctx
is active during persistence calls; the application-plane ctx is not.

### Cross-plane navigation

Application-plane spans (emitted at commit time) carry a link to the
control-plane commit span via `trace.WithLinks()`. All links in OTel are
unidirectional in the data model; this gives app-to-control navigation in all
backends. Control-to-app navigation is available in backends that reverse-index
links (Jaeger, Tempo, Honeycomb, Datadog).

## Metrics

Single meter, both planes, `dogma.plane` attribute distinguishes them. No split
by plane at the meter level.

### Application-plane event counters

One counter per event type, named `dogma.event.<TypeName>` (e.g.
`dogma.event.OrderPlaced`). Fires only on durably committed events -- not
commands, not timeout scheduling, not timeout delivery. Events are the canonical
record of things the application did; commands are requests and timeouts are
scheduling mechanics.

Cardinality is bounded by the number of event types in the application, which
is fixed at compile time. Aggregate instance IDs and other unbounded identifiers
are never used as metric attributes.

Each counter carries the standard base attributes:
`dogma.site_name`, `dogma.application_name`, `dogma.handler_name`.

### Control-plane metrics

Message volume metrics (commands routed, timeouts scheduled, timeouts fired,
etc.) for SRE/ops use. May carry `dogma.message_type` where useful, but serve
a different purpose to the application-plane event counters -- they measure
system throughput and health, not domain facts.

Metric names and units per subsystem (heartbeat, aggregate, integration,
process, projection, event stream) are TBD at implementation time.

## Logs

Two severity levels only: `Info` and `Error`. There is no `Warn`, `Debug`, or
`Trace`. The rule: if a function returns an error, log it at `Error`; otherwise
`Info`. There is no escalation logic for high-frequency mechanics.

- `HandlerScope.Log(format, args)` -- buffered in the scope during handler
  execution, emitted at `Info` with `dogma.plane = "application"` under the
  handler span's context at commit time. Discarded if the execution does not
  commit. Log records emitted after the handler span has ended -- OTel
  log-trace correlation is ID-based and does not require the span to be active.
- Engine-internal records (heartbeat writes, routing events, etc.) -- emitted
  at `Info` or `Error` with `dogma.plane = "control"`.

OTel log records automatically carry `trace_id` and `span_id` from the active
span context, giving log-trace correlation in any compliant backend. This is the
only log-trace coupling -- logs do NOT additionally emit span events.

Errors are both logged and marked on the active span (`span.RecordError` +
`span.SetStatus(codes.Error)`). These always happen together.

OCC retries and other high-frequency internal mechanics are recorded as span
attributes (e.g. `occ.attempts = 3`), not as log records or span events.

## internal/telemetry package

Runkit's `internal/telemetry` package provides a plane-aware abstraction on top
of `enginekit/telemetry`. It enforces the application/control split at compile
time via two distinct types with different method sets:

- **Application-plane recorder** -- has `StartRootSpan` (accepts
  `trace.WithLinks(...)` options for causal predecessors), `Info`, `Error`.
  `dogma.plane = "application"` is baked in; callers never pass it.
- **Control-plane recorder** -- has `StartSpan`, `Info`, `Error`.
  `dogma.plane = "control"` is baked in.

Using an application-plane method on a control-plane recorder (or vice versa)
is a compile error. The `dogma.plane` attribute value is never a loose string at
any call site.

The plane concept is runkit-specific and does not belong in `enginekit/telemetry`,
which is used by `persistencekit` and other infrastructure kits with no knowledge
of the application/control split.

## Envelope change needed

Each envelope stores one span context -- the message's own (`traceID`, `spanID`,
`traceFlags`) -- in `Envelope.Body.Extensions`. This allows the receiving node's
handler span to link to the message span via `trace.WithLinks()`. The producing
handler's span context does not travel in the envelope (see Span context
lifecycle above).

Requirements:

- Survives the network hop to the destination node.
- Stable across OCC retries: generated once at envelope construction, unchanged.
- Does not propagate as OTel context (not baggage, not the active span) -- it
  is a data field used to construct spans at commit time on the receiving node.

`Body.Extensions` scope (not `Header.Baggage`) ensures it applies only to this
message and is not inherited by downstream effects.

The exact proto representation -- a dedicated type in
`enginekit/protobuf/telemetrypb` vs. a W3C traceparent string -- is TBD at
implementation time.

## Open questions

- Exact proto representation for span context in `Envelope.Body.Extensions`:
  dedicated type in `enginekit/protobuf/telemetrypb` vs. W3C traceparent string.
- Span name conventions: exact format for handler spans (e.g. `OrderProcess
handled OrderPlaced` vs. `handled OrderPlaced` vs. something else).
- Dynamic instrument creation for `dogma.event.<TypeName>` counters: event type
  names are not known until handlers are registered (or until a new type is
  first committed). OTel allows lazy instrument creation but the interaction
  with some exporters' cardinality management needs verification.
- Control-plane metric names and units per subsystem (heartbeat, aggregate,
  integration, process, projection, event stream).
- Sampling strategy documentation for operators.
