# Plan: Serve the protobuf MetricsAPI (#154)

## Where the work stands

Planned, and the plan was approved in planning. The protobuf bump, the cluster
ID, the move of the event-stream service, the schema changes for the bindings,
the service with `EventStreamLength`, and the aggregate instance bindings are
done, as six commits on the branch `bump-protobuf-v0.9.0`, which was made from
`main` at `f585398`.

Before the gauges of queues and handlers were started, every supported metric
was evaluated for its cost, and the design of the measuring changed. "Decided
while designing the gauges" records the result. The measuring of each group of
metrics on its own timer, the move of the measuring into its own package, and
the bump to dogmatiq/protobuf v0.10.0, which replaced `WatchMetrics` with
`WatchNodeMetrics` and `WatchClusterMetrics` and removed `Reach`, are written
and await review.

## Context

[#154](https://github.com/dogmatiq/runkit/issues/154) serves the `MetricsAPI`
service, which dogmatiq/protobuf defines in its `metricsgrpc` package, from
every RunKit node. v0.9.0 of the protocol had one call, `WatchMetrics`. v0.10.0
replaced it with `WatchNodeMetrics` and `WatchClusterMetrics`. A client such as
Stories can then read the metrics that a node supports without consuming event
payloads. It is
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
  `HandlerEvents{Recorded,Handled,Failing}`; `HandlerEventStreamLag`;
  `ProcessInstancesEnded`;
  `ProjectionCompactionState` and
  `ProjectionCompactions{Completed,Failed,DeadlineExceeded,Canceled}`.
  `ProcessInstanceQueueBindings` is in the set, because its index measured well,
  as the task for the schema changes says. `ProjectionResetState` is not in the
  set, because RunKit cannot reset a projection yet, and advertising a metric
  that a node can't measure fails the whole call. #156 adds it when reset
  exists. `HandlerEventsPending` no longer exists: v0.10.0 replaced it with
  `HandlerEventStreamLag`.
- **The two in-memory metrics count the instances that are loaded for a
  delivery in progress.** The protocol's definition of `value`, "the
  instances that the engine holds in memory", is met. Its purpose clause,
  "without loading them from storage", is not, because RunKit keeps no instance
  between deliveries. The docs say so. The protocol is expected to gain separate
  counts for "warm" and "active" instances, so these two metrics may move to the
  new "active" one later.
- **Reach became two calls.** v0.9.0 gave each metric a reach, and this plan
  chose the reach of each metric. v0.10.0 removed reach, and put the metrics in
  two calls instead. "Decided while designing the gauges" says which metrics
  each call reports. A node still reports a queue, stream or handler in the
  node call only while it owns the workload with that key
  (`cluster.Node.OwnsWorkload`, the rule that the consumers use), and sends a
  zero when it stops owning that workload or the count falls to nothing.
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
  more for each row. If 45 ms proves too slow on a real workload, the next step is
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
- **One sampler for each node and call, not for each client.** Each call to
  `WatchNodeMetrics` or `WatchClusterMetrics` holds only a map of the latest sample that it sent for each metric and
  subject, and sends the differences. A sample replaces the earlier one of the
  same metric and subject, so nothing is lost, and a slow client never builds an
  unbounded queue.
- **Revisions** are the time of the measurement, as a fixed-width big-endian
  integer that the node forces to increase. All samples of one measurement share
  one revision. v0.10.0 makes revisions comparable only when they come from the
  same node, so a node has to order only its own.
- **A failed measurement** ends the calls with `UNAVAILABLE` and a fixed message
  that has no database error text, and the next call starts the sampler again.
  The sampler runs only while at least one call is watching.
- **Measurements are pulled, with signals to wake the sampler.** One sampler for
  each node reads the engine's state and keeps the latest snapshot, and each
  call sends the differences from that snapshot. The sampler
  asks the engine's other packages for as little as possible. It prefers the
  signals that the engine already has, such as "messages_enqueued" and the
  signals for events appended to a stream, and database state that is very
  cheap to poll. Anything added to another package only for the metrics is
  called out to the user when it comes up in review, and the user decides how it
  is marked. For each metric,
  the task that adds it says whether a client would gain from seeing a change
  at once, and if the cost is small, uses a signal to send it at once, not
  only on the timer.
- **The sampler's interval is not the same for every metric.** The plan began
  with one interval for all of them. "Decided while designing the gauges"
  replaces it with one pacing rule that every group of metrics follows.
- **`grpcserver.Server` registers the service only if its field is set.** The
  existing test leaves unset fields nil.

- **The engine's gRPC services live in `grpcserver`.** Each service's
  implementation is one exported type, in one file of
  [internal/grpcserver](../../internal/grpcserver), which the engine constructs
  and gives to `Server`. It stays a thin layer over the engine's other packages.
  A package under `internal/grpcserver/internal/` holds what a service needs
  that is too large for its file, such as the measuring of the metrics service
  in
  [internal/grpcserver/internal/metrics](../../internal/grpcserver/internal/metrics),
  or what several services share. The earlier plan for a package
  `internal/metrics` that serves the API is replaced by this. The event-stream
  service moves there too, from `source.Server`, in its own task. The conversion
  of the application's configuration to `configpb.Application` is an exported
  function of `grpcserver`, with no tests of its own, and is written as part of
  the metrics service, because the preamble is its first user. The engine
  converts once, and passes the result to each service that needs it.

## Decided while designing the gauges

Every supported metric was evaluated for where its value comes from and what
reading it costs, before any gauge was built. These decisions came from that
evaluation.

- **Groups.** A group is the metrics that one measurement produces: one
  statement, or one pass over counters in memory. A group of one metric is a
  group. Metrics that need separate statements are separate groups, even if they
  are cheap, so that each group's cost grows with one thing and its timer
  stretches on its own. Each group is measured on its own timer, so a slow group
  never delays another. No group has a cadence of its own.

  | Group | Metrics | Cost grows with |
  | --- | --- | --- |
  | In memory | The 12 running totals, `AggregateInstancesInMemory`, `ProcessInstancesInMemory`, `HandlerEventsFailing`, `ProjectionCompactionState` | nothing |
  | Event stream lengths | `EventStreamLength` | the number of streams |
  | Aggregate instance bindings | `AggregateInstanceEventStreamBindings` | the aggregate handlers times the streams |
  | Due and failing | Due and Failing, for queues and handlers, for commands and deadlines | the messages that are due or failing now |
  | Pending | Pending, for queues and handlers, for commands and deadlines | every queued message in the queues that are counted |
  | Process bindings | `ProcessInstanceQueueBindings` | the instances in progress |

- **One pacing rule for every group.** A group starts a measurement no sooner
  than `MinInterval` after the last one started, and later when needed to keep
  its duty cycle at or below `MaxDutyCycle`. A duty cycle is the fraction of
  the time during which a group is measuring, counted from the start of one
  measurement to the start of the next. `MinInterval` is one second and
  `MaxDutyCycle` is one tenth, so the next measurement starts after the later
  of one second and ten times the last measurement's elapsed time, both
  counted from the last start. After a measurement of 100 ms or less the next
  starts one second after the last, after a 250 ms measurement it starts 2.5 s
  after, and after a 2.5 s measurement it starts 25 s after. Both are fields
  of `MetricsService` that the engine sets from constants, with no option for
  an application, and they replace the service's `Interval`.
- **Parallel workers are off for the metrics queries.** The pacing rule uses the
  elapsed time of each measurement, because PostgreSQL doesn't return a
  query's processor time to its client. Each measurement that queries the
  database runs in a transaction that sets `max_parallel_workers_per_gather`
  to 0 with `SET LOCAL`, so that the elapsed time is never less than the
  processor time.
- **The queue index gains the handler key, and a second index covers the failing
  messages.** `queued_messages_by_queue` includes `handler_key`, so the counts
  read the index alone. A partial index on `(queue_id, message_type_id)`, which
  also includes `handler_key` and covers only the messages with `failures > 0`,
  makes the failing count read only the failing messages. Both go into the
  unreleased v2.0.0 schema. The step of the migration that creates the table
  also creates the queue index with the handler key. The failing index is new,
  so `current/` creates it with `CREATE INDEX IF NOT EXISTS`. That is safe for
  `queued_messages`, because the lock block at the top of `00-application.sql`
  already locks that table in the engine's order.
- **A deadline's handler comes from its row, and a command's from its type.**
  Dogma lets two process handlers schedule the same deadline type, so a
  deadline's type doesn't identify its handler. Its row's `handler_key` does.
  A command's row has no handler key, and the application's configuration
  allows one handler for each command type. A message is a deadline if its row
  has a handler key.
- **Due and failing are read in one statement, and pending in another.** The
  due count reads only the part of the queue index that is due, and the
  failing count reads only the failing index. One `UNION ALL` of the two, with
  a column that tells them apart, feeds 8 metrics from one snapshot. The
  pending count reads every queued message, so it is its own group, and a
  client can briefly see more messages due than pending for one queue.
- **Zeros.** The protocol requires a zero for a subject whose count falls to
  nothing. The measuring remembers the subjects of its last measurement, and
  sends a zero for each one that the next measurement no longer contains,
  whether the queue emptied or its workload moved to another node.
- **The counts of queued messages take the set of queues to count.** The due
  and failing counts and the pending count are each given that set. In the node
  call it is the queues for which the node owns the workload, and in the
  cluster call it is every queue that has messages.
- **The protocol has two calls, and no `Reach`.** The cluster call reports
  `EventStreamLength`, `AggregateInstanceEventStreamBindings`,
  `ProcessInstanceQueueBindings`, and the 12 gauges of queues and handlers,
  counted over every queue that has messages. The node call reports the 12
  gauges counted over the queues for which the node owns the workload, the 16
  metrics that are read from memory, and `HandlerEventStreamLag`.
  `ProcessInstanceQueueBindings` was to be reported by the node that owns the
  workload with the handler's key, and now no node owns it. No query feeds both
  calls, because a scan of the whole cluster for each watched node costs more
  than each node's scan of its own share.
  The doc of `EventStreamLength` says that while ownership of a stream moves
  from one node to another, both may report it, and that the value for the
  cluster is the greatest value that any node reports. RunKit has no owner of an
  event stream, because each handler's consumers own the workload with their
  own keys, so it reports the length in the cluster call only.
  Each call has its own list of metrics in its preamble, and a sample names its
  metric by the index in that list. So the service keeps a list, a set of
  groups and a sampler for each call, and a call's groups measure only while a
  call to that method watches. No parameters for the calls are needed.
- **`HandlerEventStreamLag` replaces `HandlerEventsPending`.** RunKit could
  count the events of a handler's own types only on a local stream, and not at
  all on a remote one. [#158](https://github.com/dogmatiq/runkit/issues/158)
  asked for a metric of the lag in offsets, and v0.10.0 of the protocol has
  one: the number of events on one stream, of any type, after the offset that
  the handler has reached, which is the offset after the last event that the
  handler handled or skipped. For a remote stream it counts the events that the
  engine knows are on the stream. The node call reports it for each stream that
  the node consumes, so a client adds the nodes' values for the cluster.

Measured on scratch PostgreSQL 18 containers on a laptop, so the times are
rough:

- The first measurement used the real schema of the message queue and 1M queued
  messages, half of them deadlines. The queue index is 86 MB without the handler
  key and 106 MB with it. 100,000 single-row inserts took 2.39 to 2.56 s without
  it and 2.50 to 2.68 s with it. The pending count took 150 to 188 ms without it
  and 173 to 214 ms with it. The catalog's query, the consumer's search for the
  next messages and its postponements took the same time with both.
- With a cut-down copy of the table, 668,000 queued messages in the queues
  counted, 1,333 of them due and 67 failing, the due count took 0.16 ms and
  the failing count 0.03 ms.
- The count for the process bindings took 45 ms with 1M instances in
  progress, as "Decided in planning" says.

Chosen by the agent, and not yet confirmed by the maintainer:

- No metric is given a signal. Each one either changes on every delivery, so
  a signal would arrive more often than the timer, or costs nothing to read.
- The duty cycle is the only bound on the cost of a group. A statement timeout
  is not used, because a failed measurement ends every call with
  `UNAVAILABLE`.
- `EventStreamLength` and `AggregateInstanceEventStreamBindings` stay two
  queries, one for each metric.
- Zeros are not pruned from the snapshot yet.
- `ProcessInstanceQueueBindings` moves to the cluster call when the calls are
  split. Until then it stays as "Decided in planning" says.

Left for the tasks that they belong to:

- `HandlerEventsFailing` would count one event for each stream on which the
  handler's consumer is stopped at a failing event, because the events behind
  it haven't been attempted.
- Every node runs a compactor for each projection, so the
  `ProjectionCompactionState` of one node doesn't describe the cluster, and a
  client gets one state from each node.

Found on the way, and not part of this plan: the engine keeps one queue sink
for each message type, so two process handlers that schedule the same deadline
type share a sink.
[#157](https://github.com/dogmatiq/runkit/issues/157) tracks it.

## Words

The protocol's own terms are used exactly: metric, sample, subject, revision,
preamble. The groups in
[internal/grpcserver/internal/metrics](../../internal/grpcserver/internal/metrics)
build each metric's protobuf message, from what the engine's packages expose, so
there are no types of their own that mirror the protocol's.

- **`Snapshot`**, an exported type of that package: "Snapshot is what a node has
  measured of the engine's metrics, as of one point in time." The reporter keeps
  it in a `cosyne.Observable`, so each call that watches can read the snapshot
  and wait for the next one.
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

### [x] Report `AggregateInstanceEventStreamBindings`

`samples()` in
[internal/grpcserver/metrics.go](../../internal/grpcserver/metrics.go) is split
into one method for each metric, which it concatenates, as the review of the
previous task expected. Each later group of metrics adds a method.

`REACH_CLUSTER`. The counts come from the counter table, read on the sampler's
interval.

### [x] Measure each group of metrics on its own timer

Change the measuring in
[internal/grpcserver/metrics.go](../../internal/grpcserver/metrics.go) so that
each group measures and waits by itself, under the pacing rule in "Decided while
designing the gauges". `MinInterval` and `MaxDutyCycle` replace the service's
`Interval`. The rule is a small package,
[internal/dutycycle](../../internal/dutycycle), so that its spacing can be
tested. Each measurement that queries the database runs in a transaction with
parallel workers off. The task adds no metric: each of the two existing metrics
becomes a group.

### [x] Move the measuring into its own package, and serve v0.10.0's two calls

[internal/grpcserver/metrics.go](../../internal/grpcserver/metrics.go) does two
jobs: it serves the stream to a client, and it measures. Every task after this
one adds to the second, so the measuring moves to
`internal/grpcserver/internal/metrics`: the snapshot, `Manifest` (the list of
metrics that one call reports), the groups with their queries, each listing the
metrics that it measures, and `Reporter` (one call's groups and latest samples).
It is under `grpcserver/internal`, not `internal`, because only `MetricsService`
uses it and it produces `metricsgrpc` samples. `grpcserver` keeps the service:
the calls, the preamble, and the loop that sends changes to a client.

The move and the bump to v0.10.0 are one task, so that the moved code is
written once, in its final form, and reviewed once. The tree doesn't build
between the bump and the service, which uses types that v0.10.0 removes. The
chunks are reviewed in this order: the bump, `Manifest`, the group of stored
counts, `Reporter`, then the service and its tests.

`MetricsService` serves `WatchNodeMetrics` and `WatchClusterMetrics` in place of
`WatchMetrics`, which v0.10.0 removes along with `WatchMetricsRequest`,
`SupportedMetric` and `Reach`. Each call has its own `metrics.Reporter`, which
holds that call's groups and latest samples, and builds the call's `Manifest`
from the metrics that its groups list. It measures while a call to that method
is watching. A group that needs inputs of its own, such as the queues to count,
is built by a function that takes them. The cluster call reports
`EventStreamLength` and `AggregateInstanceEventStreamBindings`.
`WatchNodeMetrics` answers UNIMPLEMENTED until the first node metric exists,
which is the in-memory counters. The tests use the cluster call. "Decided while
designing the gauges" says which metrics each call reports as they are added.

### [x] Add the handler key to the queue index, and add the failing index

Change `queued_messages_by_queue` and add the failing index in
[internal/schema/internal/current/10-messagequeue.sql](../../internal/schema/internal/current/10-messagequeue.sql).
The v2.0.0 migration creates the queue index the same way, in
[internal/schema/internal/migrations/v2.0.0/10-merge-command-and-deadline-queues.tx.sql](../../internal/schema/internal/migrations/v2.0.0/10-merge-command-and-deadline-queues.tx.sql).
The test that compares a migrated database with a new one covers both. The
changelog entry for the schema change covers them.

### [ ] Report the due and failing gauges of queues and handlers

`QueueCommands{Due,Failing}`, `QueueDeadlines{Due,Failing}`,
`HandlerCommands{Due,Failing}` and `HandlerDeadlines{Due,Failing}`, from one
statement, for the queues for which the node owns the workload. "Due" uses the Go
clock, as delivery does. The measuring sends a zero for a subject that a
measurement no longer contains.

### [ ] Report the pending gauges and `ProcessInstanceQueueBindings`

`QueueCommandsPending`, `QueueDeadlinesPending`, `HandlerCommandsPending` and
`HandlerDeadlinesPending`, as their own group, and
`ProcessInstanceQueueBindings` as another. These are the counts whose cost
grows with the size of the database, so they rely on the duty cycle.

### [ ] Add counters, the running totals and the in-flight instance counts

Handled, canceled, recorded, removed and ended, and the instances that are
loaded for a delivery in progress. The count of those goes up when a delivery
loads an instance, and down when the delivery's transaction ends, in
[internal/aggregate/commandsink.go](../../internal/aggregate/commandsink.go) and
[internal/process/instance.go](../../internal/process/instance.go).

This is the hardest design. `Sink.Deliver` returns only an error, so it can't
say whether a message was handled or canceled, and counts must increase only
after the transaction commits. Show the design in words, before any code.

### [ ] Report the events that handlers have handled and failing

For process and projection handlers. `HandlerEventsFailing` comes from the
failure streak of each stream's consumer, through a read-only accessor on the
failure tracker, and never by calling application code from the metrics path.

### [ ] Report the lag of handlers on event streams

`HandlerEventStreamLag`, in the node call, for each stream that the node
consumes. Both offsets are in the node's memory: the end of the stream, as the
node last saw it, and the offset that the handler has reached. For a
projection, the handler's checkpoint offset stays behind the end of the stream
while the newest events are of types that the handler skips, so the offset that
it has reached is the one that the consumer has read up to. This needs a
read-only accessor on the event-stream consumer, which is an addition to
another package only for the metrics, and the user decides how it is marked.

### [ ] Report the compaction state and totals

In [internal/projection/compactor.go](../../internal/projection/compactor.go):
the compaction that is running, the previous one with its outcome and interval,
the planned next one, and the four totals. Check how the compactor treats a
deadline: the supervisor retries it as a failure today, and the protocol says it
isn't one. Decide what a client should make of one state from each node, as
"Decided while designing the gauges" notes.

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

With three nodes: the metrics that each call reports, revisions, resets and zeros,
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
- The due and failing counts cost the most when a queue has a large backlog,
  which is when an operator watches them. The duty cycle makes them repeat less
  often then.
- Once the calls are split, a client that makes the cluster call to every node
  multiplies the cluster's scans by the number of nodes.
- Metrics are served only where `WithListenAddress` is set, and with whatever
  authentication the application configured.

## Not in this plan

- The OpenTelemetry metrics and traces
  ([#120](https://github.com/dogmatiq/runkit/issues/120)).
- `ProjectionResetState`, which #156 adds when reset exists.
- The engine API ([#153](https://github.com/dogmatiq/runkit/issues/153)) and the
  projection API ([#156](https://github.com/dogmatiq/runkit/issues/156)), apart
  from the groundwork that they share.
