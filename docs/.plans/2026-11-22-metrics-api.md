# Plan: Serve the protobuf MetricsAPI (#154)

## Where the work stands

Planned, and the plan was approved in planning. The protobuf bump, the cluster ID,
the move of the event-stream service and the schema changes for the bindings are
done, as four commits on the branch `bump-protobuf-v0.9.0`, which was made from
`main` at `f585398`. The service, which reports `EventStreamLength` and ends
calls with `UNAVAILABLE` when a measurement fails, is written and awaits review. The next task is to report the aggregate instance bindings.

## Context

[#154](https://github.com/dogmatiq/runkit/issues/154) serves
`MetricsAPI.WatchMetrics`, which dogmatiq/protobuf v0.9.0 defines in its
`metricsgrpc` package, from every RunKit node. A client such as Stories can then
read the metrics that a node supports without consuming event payloads. It is
part of [#140](https://github.com/dogmatiq/runkit/issues/140).

This issue chooses the set of metrics that RunKit supports.
[#156](https://github.com/dogmatiq/runkit/issues/156), the ProjectionAPI, needs
the set to include the compaction state now, and the reset state once #156
builds reset. [#153](https://github.com/dogmatiq/runkit/issues/153), the
EngineAPI, needs the same node ID, cluster ID and application identity in its
preamble. The OpenTelemetry metrics are
[#120](https://github.com/dogmatiq/runkit/issues/120), and stay out of this
plan.

What the code shows today:

- [go.mod](../../go.mod) pins dogmatiq/protobuf v0.8.0, which has neither
  `metricsgrpc` nor `configpb`.
- The engine serves one gRPC service, `EventStreamConsumerAPI`, through
  [internal/grpcserver/server.go](../../internal/grpcserver/server.go). A
  service is a plain struct with exported fields, such as `source.Server`, and
  is registered in `serve()`. The engine builds the server in
  [engine.go](../../engine.go) only if `WithListenAddress` was given, so
  without it no metrics are served. Authentication is whatever
  `grpc.ServerOption` values the application passes.
- RunKit has no cluster ID. `Engine.nodeID` is random for each process start,
  and the only database-wide identity is the application's key and name in
  `runkit.identity`. Neither RunKit nor enginekit v0.32.0 converts an
  application's configuration to `configpb.Application`.
- RunKit has no counters, and it holds no aggregate or process instance in
  memory between deliveries.
- RunKit never calls `ProjectionMessageHandler.Reset`.
- The `inspector` branch is far behind `main` and uses the old table names. It
  is inspiration only, and nothing in this plan follows it as a specification.
  It shows one way to read the same state: a read-only transaction with a
  statement timeout, caps on counts, and queries for the queues and for pending
  events. Each task decides for itself whether any of that suits an engine that
  serves a public API.

## Decided in planning

- **The supported set.** `EventStreamLength`; `AggregateInstanceEventStreamBindings`;
  `AggregateInstancesInMemory`; `ProcessInstancesInMemory`;
  `QueueCommands{Pending,Due,Failing,Removed}` and
  `QueueDeadlines{Pending,Due,Failing,Removed}`;
  `HandlerCommands{Pending,Due,Failing,Handled}`;
  `HandlerDeadlines{Pending,Due,Failing,Handled,Canceled}`;
  `HandlerEvents{Recorded,Handled,Pending,Failing}`; `ProcessInstancesEnded`;
  `ProjectionCompactionState` and
  `ProjectionCompactions{Completed,Failed,DeadlineExceeded,Canceled}`.
  `ProcessInstanceQueueBindings` is in the set, because its index measured well,
  as the task for the schema changes says. `ProjectionResetState` is not in the
  set, because RunKit cannot reset a projection yet, and advertising a metric
  that a node can't measure fails the whole call. #156 adds it when reset
  exists.
- **The two in-memory metrics count the instances that are loaded for a
  delivery in progress.** The protocol's definition of `value`, "the
  instances that the engine holds in memory", is met. Its purpose clause,
  "without loading them from storage", is not, because RunKit keeps no instance
  between deliveries. The docs say so. The protocol is expected to gain separate
  counts for "warm" and "active" instances, so these two metrics may move to the
  new "active" one later.
- **Reach.** The gauges of queues, handlers and pending events are
  `REACH_NODE`. A node reports one of them for a queue, stream or handler only
  while it owns the workload with that key (`cluster.Node.OwnsWorkload`, the rule
  that the consumers use). When the node stops owning that workload, or the count
  falls to nothing, the node sends a zero. The running totals, the in-flight
  instance counts and the compaction state are `REACH_NODE`.
  `EventStreamLength` and `AggregateInstanceEventStreamBindings` are
  `REACH_CLUSTER`, because every node can read them exactly from the database.
  `ProcessInstanceQueueBindings` is `REACH_NODE`, reported only by the node that
  owns the workload with the handler's key, for every queue and for the
  instances bound to none. The
  samples of all nodes then add up to the cluster's value, and the scan runs
  once for each handler, not once for each node.
- **The bindings counter table.** A table keyed by `(handler_key, stream_id)`,
  with a count of instances, goes into the unreleased v2.0.0 schema, so it needs
  no handling of mixed versions. The v2.0.0 migration fills it from the
  instances that a database at the earlier schema version already has bound.
  The table is named `aggregate_stream_binding_counts`, and it needs no striped
  rows, because the stream's lock already serializes its updates.
  `runkit.save_aggregate_instance` increments it at the one place where an instance's `stream_id` changes from
  NULL to a stream. The transaction already holds the stream's lock there, so
  the increment adds no wait. An index on `aggregate_instances` would still
  count every entry of the handler on each refresh, and aggregate instances are
  never deleted, so the table is better. A column on `event_streams` can't hold
  the count, because the metric reports a separate count for each handler.
- **The process bindings use a partial index** on
  `process_instances (handler_key, deadline_queue_id) WHERE NOT ended`, because
  process instances end, and a counter would need decrements and would have one
  hot row for each handler and queue. On a scratch database with 1M in-progress
  and 2M ended instances, the exact count took 45 ms with the index and 87 ms
  without it, which scans the whole 658 MB table, and inserts cost about 1 µs
  more for each row. The metric is read on a slower interval than the other
  gauges. If 45 ms proves too slow on a real workload, the next step is
  counters in several rows for each handler and queue, which are exact, and not
  a sample, because a client adds the queue values to get the handler's total.
- **The cluster ID is stored in the database.** The table `runkit.identity`
  becomes `runkit.cluster`. Its one row holds the identity of the cluster and of
  what the cluster runs: `id`, a random UUID made when the database is first
  bound; `application_key`;
  `application_name`; and, when RunKit supports a site, `site_key` and
  `site_name`, which are nullable and added with `ADD COLUMN IF NOT EXISTS`. A
  cluster is the set of nodes that share one database, which is one installation
  of one application. `cluster.Join` creates the ID on first use, and keeps refusing a
  database that is bound to a different application key. `runkit.identity`
  exists only in the unreleased v2.0.0 schema, so the change goes into the
  v2.0.0 migration and needs no new schema version. #153 uses the same ID.
- **One sampler for each node, not for each client.** Each `WatchMetrics` call
  holds only a map of the latest sample that it sent for each metric and
  subject, and sends the differences. A sample replaces the earlier one of the
  same metric and subject, so nothing is lost, and a slow client never builds an
  unbounded queue.
- **Revisions** are the time of the measurement, as a fixed-width big-endian
  integer that the node forces to increase. All samples of one measurement share
  one revision.
- **A failed measurement** ends the calls with `UNAVAILABLE` and a fixed message
  that has no database error text, and the next call starts the sampler again.
  The sampler runs only while at least one call is watching.
- **Measurements are pulled, with signals to wake the sampler.** One sampler for
  each node reads the engine's state and keeps the latest snapshot, and each
  `WatchMetrics` call sends the differences from that snapshot. The sampler
  asks the engine's other packages for as little as possible. It prefers the
  signals that the engine already has, such as "messages_enqueued" and the
  signals for events appended to a stream, and database state that is very
  cheap to poll. Anything added to another package only for the metrics is
  called out to the user when it comes up in review, and the user decides how it
  is marked. For each metric,
  the task that adds it says whether a client would gain from seeing a change
  at once, and if the cost is small, uses a signal to send it at once, not
  only on the timer.
- **The sampler's interval is the same for every metric, for now.** The user
  isn't convinced that one interval suits all of them, and asked that it not be
  treated as fixed. Each metric task says whether it gives friction, such as a
  query that is too costly at that rate or a count that a client wants sooner,
  and if so proposes a separate interval or a signal for that metric.
- **`grpcserver.Server` registers the service only if its field is set.** The
  existing test leaves unset fields nil.

- **The engine's gRPC services live in `grpcserver`.** Each service's
  implementation is one exported type, in one file of
  [internal/grpcserver](../../internal/grpcserver), which the engine constructs
  and gives to `Server`. It stays a thin layer over the
  engine's other packages. A package under `internal/grpcserver/internal/` holds
  only what several services share, such as the conversion of the application's
  configuration to `configpb.Application`. This replaces the earlier plan for a
  package `internal/metrics` that serves the API. The event-stream service
  moves there too, from `source.Server`, in its own task. The conversion of the
  application's configuration to `configpb.Application` is an exported function
  of `grpcserver`, with no tests of its own, and is written as part of the
  metrics service, because the preamble is its first user. The engine converts
  once, and passes the result to each service that needs it.
## Words

The protocol's own terms are used exactly: metric, sample, subject, reach,
revision, preamble. `grpcserver` builds each metric's protobuf message itself,
from what the engine's packages expose, so there are no types of its own that
mirror the protocol's.

- **`snapshot`**, an unexported type of `grpcserver`: "snapshot holds the latest
  sample of every metric and subject that the node has measured." The sampler
  keeps it in a `cosyne.Observable`, so each call that watches can read the
  snapshot and wait for the next one.
- **`Total`**, for a running total, in the package that counts. Recommended:
  "A Total counts the occurrences of one event since the node started counting,
  and only ever increases." Alternatives: `Counter`: "A Counter counts the
  occurrences of one event since the node started."; `Tally`. A total of each
  kind is a field named for what it counts, such as `Handled` and `Removed`.
  Chosen when the counters task comes.

## Tasks

### [x] Bump dogmatiq/protobuf to v0.9.0

Do it in its own change, because #153 and #156 need the same release, and
whichever lands first makes the bump. Change [go.mod](../../go.mod) and
[go.sum](../../go.sum), and whatever the build needs. The build and the existing
tests must still pass. No changelog entry unless the build changes in a way that
a user can see.

v0.9.0 adds `ListEventStreams` and `ReadEventSegment` to
`EventStreamConsumerAPI`, so `source.Server` no longer satisfied the interface.
The change embeds `messaginggrpc.UnimplementedEventStreamConsumerAPIServer` in
it, so those two methods answer UNIMPLEMENTED, as they would from a server
that lacks them. Nothing a client can see has changed, so there is no
changelog entry.

### [x] Rename the identity table to `cluster`, and add the cluster ID

Shared with #153. Rename `runkit.identity` to `runkit.cluster`, with the columns
`id`, `application_key` and `application_name`, in
[internal/schema/internal/current/00-application.sql](../../internal/schema/internal/current/00-application.sql).
The table has no `singleton` column. A unique index on `((TRUE))` limits it to
one row, and `Join` conflicts on that index. Because a database at schema
version v1.1.0 has `application.identity`, the v2.0.0 migration, which is not
yet released, creates `runkit.cluster`, copies the row with a new random ID, and
drops the old table.

Replace `application.Bind` in
[internal/application/bind.go](../../internal/application/bind.go) with
`cluster.Join` in [internal/cluster](../../internal/cluster), and delete the
then-empty `internal/application` package. `Join` returns the cluster ID: "Join
returns the ID of the cluster that uses the database, and creates the cluster if
the database has none. It updates the stored name of the application, and
returns an error if the cluster runs a different application." The code that
creates the ID generates it in Go, and the ID is a plain `uuid.UUID`, like the
node ID. Update every document that says "identity" for this table. The engine
reads the cluster ID when it starts.

Tests, against a real database: two engines on one database report the same
cluster ID; two databases report different IDs; joining a cluster of a
different application key is still refused.

### [x] Move the event-stream service into `grpcserver`

Move `source.Server` from
[internal/eventstream/source/server.go](../../internal/eventstream/source/server.go)
into a file of [internal/grpcserver](../../internal/grpcserver), with its tests,
as the exported type `EventStreamConsumerService`, so that every gRPC service of
the engine is in one package, one file each. `Server` keeps its field that holds
the service, and the engine constructs the service.
Nothing a client can see changes. Done before the metrics service, so that the
metrics service lands beside it.

### [x] Make the schema changes for the bindings

Add the partial index to
[internal/schema/internal/current/20-process.sql](../../internal/schema/internal/current/20-process.sql).
It was measured on a scratch database already, as "Decided in planning" says.

Then add the bindings counter table, `aggregate_stream_binding_counts`, and
change `save_aggregate_instance` in
[internal/schema/internal/current/20-aggregate.sql](../../internal/schema/internal/current/20-aggregate.sql).

Tests, against a real database: the count for each handler and stream after
several binds; no double count when an instance records more events; a
rolled-back bind leaves the count unchanged.

The index is created only if it doesn't exist. `CREATE INDEX` locks its table
even when the index exists, and the engine's transactions lock
`process_instances` and `queued_messages` in both orders, so the lock-ordering
block at the top of
[internal/schema/internal/current/00-application.sql](../../internal/schema/internal/current/00-application.sql)
can't make an upgrade safe for this table. The v2.0.0 migration has a step for
each change: one creates the counter table and fills it from the instances that
are already bound, and one builds the index concurrently. The changelog entry
for the schema change says that the upgrade takes longer the more instances
there are.

### [x] Serve `WatchMetrics`, reporting `EventStreamLength`

Add `MetricsService` to [internal/grpcserver](../../internal/grpcserver), with
the preamble, the samples that follow it, revisions, and cleanup of each call
when it is canceled, the client disconnects or the engine shuts down. The engine
constructs it, and `Server` serves it only if it has one. While a call watches,
a sampler measures every second, and the calls send the differences.

The conversion of the application's configuration to `configpb.Application` is
an exported function of `grpcserver`, `ApplicationToProto`, so that the engine
converts once and passes the result to each service that needs it. Neither
RunKit nor enginekit v0.32.0 had one, and the `configpb` package that enginekit
has is an older, thinner schema that nothing in enginekit uses. It covers the
application's identity and Go type, its handlers with their types, message
usage and concurrency preference, and its message kinds. It leaves the version
empty, and leaves support for compaction and reset absent, because RunKit can't
tell a real `Compact` or `Reset` from a no-op. The preamble test covers it.
[#153](https://github.com/dogmatiq/runkit/issues/153) uses it later.

`EventStreamLength` is the first metric, because it is the simplest one that
exercises the sampler. It is `REACH_CLUSTER`, and every node reports the same
lengths, read from the event streams table on each measurement, which is a cheap
read of one row for each stream. It reads the table and not the streams that
`source.Local` follows, so it adds nothing to that package, and it excludes
remote streams because they aren't in the table. Real-time delivery would gain
a client little here: the sampler's one-second interval is already close to it.

A failed measurement ends the calls that were watching with `UNAVAILABLE` and a
fixed message, and the error goes to the engine's log. The next call starts the
measuring again.

Tests, against a real cluster: the preamble, the lengths and their revisions, a
second call that begins with the latest lengths, and a new call after the last
one ended. A test with a closed database checks the `UNAVAILABLE` ending.

### [ ] Report `AggregateInstanceEventStreamBindings`

Note from the review of the previous task: `samples()` in
[internal/grpcserver/metrics.go](../../internal/grpcserver/metrics.go) will
probably need to split apart as metrics are added, for example into one
function for each group of metrics. This task is the first to add a second
group, so it is where to decide how.

`REACH_CLUSTER`. The counts come from the counter table, read on the sampler's
interval.

### [ ] Report the gauges of queues and handlers for commands and deadlines

Pending, due and failing, for each queue and each handler, and
`ProcessInstanceQueueBindings`, on a slower interval. The node reports a queue
only while it owns the workload with the queue's key. "Due" uses the Go clock,
as delivery does. Measure the cost of each query on a large engine before
relying on it, and bound it, for example with a read-only transaction and a
statement timeout.

### [ ] Add counters, the running totals and the in-flight instance counts

Handled, canceled, recorded, removed and ended, and the instances that are
loaded for a delivery in progress. The count of those goes up when a delivery
loads an instance, and down when the delivery's transaction ends, in
[internal/aggregate/commandsink.go](../../internal/aggregate/commandsink.go) and
[internal/process/instance.go](../../internal/process/instance.go).

This is the hardest design. `Sink.Deliver` returns only an error, so it can't
say whether a message was handled or canceled, and counts must increase only
after the transaction commits. Show the design in words, before any code.

### [ ] Report the events that handlers have pending, handled and failing

For process and projection handlers, from the consumer's tracked checkpoint, and
never by calling application code from the metrics path. Add a read-only
accessor to the failure tracker. Measure the exact count of pending events on
about a million events, and decide on a timeout, before relying on it.

### [ ] Report the compaction state and totals

In [internal/projection/compactor.go](../../internal/projection/compactor.go):
the compaction that is running, the previous one with its outcome and interval,
the planned next one, and the four totals. Check how the compactor treats a
deadline: the supervisor retries it as a failure today, and the protocol says it
isn't one.

### [ ] Write the documentation and the changelog

The supported set, each omission, and each protocol gap. File an issue in
dogmatiq/protobuf, after the maintainer approves its wording because that
repository is public. The issue makes three points about
`AggregateInstancesInMemory` and `ProcessInstancesInMemory`:

- Each defines `value` as the instances that the engine holds in memory. An
  engine that loads an instance for each delivery can report that.
- Each also gives a purpose, "without loading them from storage". An engine
  that loads an instance for each delivery can't meet that purpose.
- RunKit is an engine of that kind, because it keeps no instance between
  deliveries.

It asks which of the two the protocol requires, or whether to add the "warm" and
"active" counts that are expected.

Also update the `WithListenAddress` doc comment, which must say that metrics are
served to any client that can connect, as events are; the README; and
[CHANGELOG.md](../../CHANGELOG.md).

### [ ] Verify against a real cluster

With three nodes: the reach of each metric, revisions, resets and zeros,
ownership moving between nodes, the unsupported and unavailable cases,
consistent IDs, cancellation and reconnecting.

## Verification

Use `make` targets only: `make test` and `make precommit`. Precommit can fail
with port exhaustion in `internal/eventstream/source`; rerun it with `-p 1`.
Tests use a real PostgreSQL through `xtesting.RunEngines` and
`xtesting.StartCluster`, and `xtesting.DialGRPC` for the client. Don't write
tests for the xtesting helpers themselves.

## Risks

- Running totals restart from zero with the process. The protocol allows it,
  and the docs must say so.
- Per-queue subjects include the IDs of old nodes, so zero subjects need
  pruning, or the maps grow.
- An exact count of pending events may be slow on a large stream.
- Metrics are served only where `WithListenAddress` is set, and with whatever
  authentication the application configured.

## Not in this plan

- The OpenTelemetry metrics and traces
  ([#120](https://github.com/dogmatiq/runkit/issues/120)).
- `ProjectionResetState`, which #156 adds when reset exists.
- The engine API ([#153](https://github.com/dogmatiq/runkit/issues/153)) and the
  projection API ([#156](https://github.com/dogmatiq/runkit/issues/156)), apart
  from the groundwork that they share.
