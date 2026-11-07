# Logging and Retries Implementation Plan

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Don't stage or commit code. The
> maintainer stages and commits each task as they review it. Build and test
> exclusively through `make` (`make test`, `make precommit`), never by invoking
> the Go toolchain directly. Tests start PostgreSQL through testcontainers, so
> Docker must be running.

**Goal:** Bring the engine in line with the [logging guide], and start
replacing the supervisor's retry logic with a retry streak that the code
deciding about a failure owns.

**Architecture:** Tasks 1 to 3 are behavior fixes to event stream consumption
that stand on their own. A new `supervisor.Streak` type then tracks consecutive
failures of one piece of work. It decides the backoff and writes every failure
and recovery record by the guide's streak rules. `supervisor.Run` uses it for
now and reports a time-only success. The engine builds an engine logger and an
application logger, and each scope writes its application records through a
logger whose handler buffers them until its attempt commits.

**Tech stack:** Go, `log/slog`, PostgreSQL, `testing/synctest`.

**Spec:** the [logging guide], plus the decisions below.

This covers the parts of #58, #73 and #91 that don't depend on the message pump
redesign in #100.

## Decisions that shape the work

These were made with the maintainer, and aren't in the logging guide.

- **`ErrConsumerTooSlow` becomes `ErrNotContiguous`.** It means the source can
  no longer deliver a stream contiguously, so the caller should watch again.
  It still maps to ABORTED. Don't change the `dogmatiq/protobuf` docs.
- **`source.Local` resumes small gaps.** When it follows a stream again after a
  failure, it resumes from the offset it last delivered if the gap fits in a
  watcher's buffer, `BufferSize` × `SegmentSize` events. Otherwise it ends
  every watch call with `ErrNotContiguous` and resumes from the stream's end.
  It no longer ends watch calls just because a stream failed.
- **A stream that leaves the catalog** isn't a failure. The stream consumer
  waits for the stream to return, without logging above DEBUG, and then
  resumes from the handler's checkpoint.
- **The retry streak.** One type, `supervisor.Streak`, with a recovery policy.
  The policy is a quiet period. With a quiet period of zero, a reported success
  is recovery at once, which suits discrete work. With a positive quiet period,
  a success starts the period, and recovery comes when it passes without a
  failure, which suits long-running work. Work with no success signal reports a
  success when each attempt starts, so it falls back to time alone.
- **Deterministic failures.** The code that decides about a failure reports a
  deterministic failure to the streak directly. While `supervisor.Run` remains,
  it learns about one through a `supervisor.Deterministic` wrapper, which
  replaces `supervisor.Persistent`. Only the feed and catalog use it, for
  source errors. The stream consumer and the compactor stop wrapping every
  error.
- **The supervisor** stays for now and is factored away later. How that
  happens isn't decided. `supervisor.Fatal` is unchanged.
- **Message pumps** change only as far as renaming and passing loggers. #100
  redesigns them and adapts to this work. Buffering their application records,
  per-delivery streaks and a persisted first-failure time follow #100.

## Related work

- **#101** changes how process handlers persist their effects, in
  `internal/process/sink.go`, `deadlinepump.go`, `instancestate.go` and a new
  `effects.go`. It's independent of #100 and is expected to land first. Tasks
  6 and 7 build on its version of those files.

## Global constraints

- Tests never assert on log records. Logging isn't tested.
- Tests are in `package foo_test`, and test unexported types through the public
  API.
- Struct literals have one named field per line, in definition order.
- Method receivers are one letter.
- Exported methods come first in a file, and unexported helpers after them.
- Internal packages take required fields. The engine passes constants. Add no
  `EngineOption`.
- Don't add constructors for unexported types. Build the literal where it's
  used.
- Comments describe the current code. They never mention past designs, and
  never restate what the code shows.
- Engine log messages are constant strings. Values go in attributes.
- Fields that hold a logger are named `EngineLogger` and `AppLogger`, or
  `engineLogger` and `appLogger` if unexported. Never use `Logger` or `logger`.

## Review focus

These conditions are easy to get wrong, and no existing test exercises them.
Each one has a test in the task that owns the code.

1. `source.Local` failing on a stream before it delivered the stream's opening
   segment has no offset to resume from. Task 2 has the test.
2. A stream consumer waiting for its stream to return must stop when the
   `Consumer` stops it. Task 3 has the test.
3. A streak's quiet period ending after shutdown must not write a recovery
   record or touch a stopped streak. Task 4 has the test.
4. A success and a failure reported from different goroutines while the quiet
   timer fires. `make test` runs with `-race`. Task 4 has the test.
5. An attempt whose commit fails after its handler succeeded must write no
   application records. Task 7 covers it by construction, because flushing
   happens only on the commit's success path.

---

### Task 1: Rename `ErrConsumerTooSlow` to `ErrNotContiguous`

**Files:**
- Modify: `internal/eventstream/source/errors.go:6-9`, `source.go:31`,
  `local.go:45,120,323`, `remote.go:174`, `server.go:61-62`
- Modify: `internal/eventstream/catalog.go:130,197`, `engine.go:63-66`
- Test: `internal/eventstream/source/local_test.go`, `remote_test.go`,
  `internal/eventstream/catalog_test.go`

**Interfaces:**
- Produces: `source.ErrNotContiguous = errors.New("event stream can not be
  delivered contiguously")`, with the doc comment "ErrNotContiguous is the
  error that [Source.Watch] returns when it can no longer deliver a stream
  contiguously. The caller should watch again, and read any events it missed."

- [ ] Rename the variable, and every use in code, tests and doc comments.
  Comments that say the caller fell behind should say the call can no longer
  deliver a stream contiguously, giving falling behind as one cause.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 2: `source.Local` resumes small gaps

**Files:**
- Modify: `internal/eventstream/source/local.go` (`fail`, `watchStream`,
  `deliverStream`, the `BufferSize` and `Watch` doc comments)
- Test: `internal/eventstream/source/local_test.go`

**Behavior:**
- `watchStream` no longer calls `fail`, and keeps the stream's entry in
  `s.offsets`, so a retry knows where delivery stopped.
- When `deliverStream` opens a stream that has an entry in `s.offsets`, it
  loads the stream's end and compares the two.
  - If `end - offset <= BufferSize * SegmentSize`, it resumes delivery from
    `offset`, with no opening segment.
  - Otherwise, it removes every watcher with
    `fmt.Errorf("unable to deliver event stream %s contiguously: %w", id, ErrNotContiguous)`,
    then follows the stream from its end, as for a new stream.
- A stream with no entry in `s.offsets` is opened as a new stream (review
  focus 1).
- The `Watch` doc comment says it returns an error that wraps
  `ErrNotContiguous` when a stream can't be delivered contiguously, and no
  longer mentions other delivery errors.

- [ ] Write the tests:
  - "it resumes a stream from where delivery stopped after a failure". Use
    `xtesting.NewDatabaseWithFailingQueries` with an `atomic.Bool` that fails
    the segment query for one stream. While it's set, append 3 events. Clear
    it, append 1 event, and expect the watcher to receive all 4 events,
    contiguously, with no error.
  - "it ends watch calls when the gap after a failure exceeds the buffer". Set
    `SegmentSize` 1 and `BufferSize` 2. While failing, append 5 events. Clear
    the failure, and expect the watch call to end with an error that wraps
    `ErrNotContiguous`.
  - "it opens a stream that failed before its first delivery as a new stream".
    Fail `loadWatchOffset`'s query from the start, then clear it, and expect the
    opening segment at the stream's end.
  - Replace "it returns an error when it can not continue delivering a stream"
    (:370) with "it keeps a watch call open while it can not continue
    delivering a stream". Corrupt the envelope as before, and expect no segment
    and no error within 1 second.
- [ ] Run `make test`. Expect the new tests to fail.
- [ ] Implement the behavior.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 3: Stream consumers wait for a stream that left the catalog (#73)

**Files:**
- Modify: `internal/eventstream/consumer.go:112-176`
- Test: `internal/eventstream/consumer_test.go`, `sinkstub_test.go`

**Behavior:**
- Inside the supervised function, when `c.EventStreams.Consume` returns
  `ErrStreamNotFound` and the context is still live, the stream consumer logs
  DEBUG `event stream left the catalog, waiting for it to return`. It then
  waits on `c.EventStreams.StreamIDs()` until the set contains the stream or
  the context ends. When the stream returns, it loads the checkpoint again and
  continues. It doesn't return an error, so the supervisor sees no failure.

- [ ] Write the tests:
  - "it waits without retrying while its stream is absent from the catalog".
    Count `CheckpointOffset` calls in the stub sink. Remove the stream, wait
    1 second, and expect the count unchanged.
  - "it stops while waiting for its stream to return" (review focus 2).
    Remove the stream, cancel the context passed to `Consumer.Run`, and expect
    `Run` to return within 1 second.
  - The existing "it consumes a stream again when it returns to the catalog"
    (:146) still passes.
- [ ] Run `make test`. Expect the first new test to fail.
- [ ] Implement the behavior.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 4: The retry streak

**Files:**
- Create: `internal/supervisor/streak.go`
- Test: `internal/supervisor/streak_test.go`, using `testing/synctest`

**Interfaces:**
- Produces:

  ```go
  // Streak tracks a run of consecutive failures of one piece of work. It
  // decides how long to wait before each retry, and writes each failure and
  // recovery record by the rules in docs/maintainers/logging.md.
  type Streak struct {
      // EngineLogger is the target for the streak's records. Its attributes
      // must identify the work.
      EngineLogger *slog.Logger

      // Description describes the work as a verb phrase, such as "consume
      // event stream". It's used in the records' log messages.
      Description string

      // QuietPeriod is how long the work must go without failing after a
      // success before the streak ends. Zero ends it at the success itself.
      QuietPeriod time.Duration
      // unexported state, guarded by a mutex
  }

  func (s *Streak) Failed(ctx context.Context, err error) time.Duration
  func (s *Streak) FailedDeterministically(ctx context.Context, err error) time.Duration
  func (s *Streak) Succeeded(ctx context.Context)
  func (s *Streak) Stop()
  ```

  Package constants: `baseDelay = 100 * time.Millisecond`,
  `transientMaxDelay = 15 * time.Second`,
  `deterministicMaxDelay = 5 * time.Minute`,
  `errorAfter = 30 * time.Second`. Use the package's existing `Jitter`.

**Behavior:**
- `Failed` and `FailedDeterministically` return the delay before the next
  attempt. The first failure of a streak records the time it started and uses
  `baseDelay`. Each later failure doubles the backoff up to the limit for its
  kind, and the returned delay is `Jitter` of the backoff. A failure cancels any
  quiet period in progress, so the streak continues.
- `Succeeded` does nothing if no streak is in progress. With a zero
  `QuietPeriod`, it ends the streak at once. Otherwise, it starts the quiet
  period if one isn't running. A `time.AfterFunc` timer ends the streak when
  it passes.
- Ending a streak writes the recovery record and resets all state, so the next
  failure starts a new streak at `baseDelay`.
- `Stop` cancels the quiet period and writes nothing. Call it when the work
  stops for good, such as at shutdown.

**Records**, with the exact log messages. `{description}` is `Description`.

| When | Level | Log message |
| --- | --- | --- |
| A deterministic failure, if the streak hasn't logged an ERROR | ERROR | `unable to {description}, further failures are logged at DEBUG until it succeeds again` |
| A failure once the streak is `errorAfter` old, if it hasn't logged an ERROR | ERROR | same as above |
| Any other failure | DEBUG | `unable to {description}, retrying` |
| Recovery after an ERROR | INFO | `able to {description} again` |
| Recovery without an ERROR | DEBUG | `able to {description} again` |

Every record has `failing_since`, the time of the streak's first failure, and
`failures`. Failure records also have `xslog.Error(err)` and `delay`. ERROR
and recovery records also have `failing_for`, the time since the first
failure.

- [ ] Write the tests in `streak_test.go`, each inside `synctest.Test`:
  - "it starts each streak at the base delay": the first `Failed` returns a
    delay from 50ms to 100ms, inclusive.
  - "it doubles the delay up to the transient limit": 10 consecutive `Failed`
    calls return delays whose upper bounds go 100ms, 200ms … and stop at 15s.
  - "it limits a deterministic failure's delay to 5 minutes": the same with
    `FailedDeterministically`, stopping at 5m.
  - "it ends a streak at a success when the quiet period is zero": after
    `Failed`, `Failed`, `Succeeded`, the next `Failed` returns at most 100ms.
  - "it continues a streak when a failure comes within the quiet period":
    `QuietPeriod` 30s. After `Failed`, `Succeeded`, sleep 10s, then `Failed`
    returns at most 200ms.
  - "it ends a streak when the quiet period passes without a failure": the
    same, but sleep 31s. Then `Failed` returns at most 100ms.
  - "it writes nothing after it is stopped" (review focus 3): `Failed`,
    `Succeeded`, `Stop`, then sleep past the quiet period inside
    `synctest.Test`. Expect no panic or race. The test makes no log assertions.
  - "it tolerates successes and failures from different goroutines" (review
    focus 4): 100 goroutines each call `Failed` and `Succeeded`, then wait.
- [ ] Run `make test`. Expect the new tests to fail, because `Streak` doesn't
  exist yet.
- [ ] Implement `Streak` in `streak.go`, with the behavior and records above.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 5: The supervisor uses the streak

**Files:**
- Modify: `internal/supervisor/supervisor.go`, `group.go`, `error.go`
- Modify: every caller listed below
- Test: `internal/supervisor/supervisor_test.go`, `group_test.go`,
  `error_test.go`

**Interfaces:**
- Consumes: `Streak` from Task 4.
- Produces:
  - `func Run(ctx context.Context, engineLogger *slog.Logger, description string, fn func(context.Context) error) error`
  - `func (g *Group) Go(engineLogger *slog.Logger, description string, fn func(context.Context) error)`
  - `func Deterministic(err error) error` and `func Deterministicf(format string, args ...any) error` replace `Persistent` and `Persistentf`.

**Behavior:**
- `Run` builds a `Streak` with a `QuietPeriod` of 30 seconds, and reports
  `Succeeded` at the start of each attempt, because it can't see inside `fn`.
- It reports a failure that wraps `Deterministic` with
  `FailedDeterministically`, and any other failure with `Failed`.
- It calls `Stop` when it returns. `Fatal`, the panic on a SQL deadlock, and
  returning the context's error are unchanged.
- Remove `resetThreshold` and the "component failed" record.

**Callers and descriptions:**

| Caller | Description |
| --- | --- |
| `catalog.go:180` | `watch event stream source` |
| `consumer.go:129` | `consume event stream` |
| `source/local.go:87` | `discover event streams` |
| `source/local.go:95` (`group.Go`) | `follow event stream` |
| `messagepump/pump.go:55` | `deliver messages` |
| `remoteengine/connector.go:48` | `discover remote engines` |
| `grpcserver/server.go:47` | `serve gRPC requests` |
| `signal/bus.go:122` | `listen for signals` |
| `projection/compactor.go:55` | `compact projection` |

- [ ] Change `catalog.go:209` and `feed.go:275` from `Persistent` to
  `Deterministic`.
- [ ] Remove the `Persistent` wrapping at `consumer.go:135`, `consumer.go:147`
  and the `Persistentf` call at `compactor.go:112`. They return the plain
  `error`. Keep the message `unable to compact projection: %w` with
  `fmt.Errorf`.
- [ ] Update the supervisor tests for the new signatures. Replace the 3-minute
  reset subtests with the 30-second quiet period. Rename `TestPersistentf` to
  `TestDeterministicf`.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 6: Engine and application loggers

**Files:**
- Modify: `engine.go`, `engineoption.go`, `execute.go`
- Modify: every struct named below

**Interfaces:**
- Produces:
  - `Engine.engineLogger` and `Engine.appLogger`. Both are built in `Run` from
    the logger given to `WithLogger`, with `xslog.UUID("node_id", e.nodeID)`
    and `xslog.Identity("application", e.appConfig.Identity())`.
    `engineLogger` adds `slog.String("layer", "engine")`, and `appLogger` adds
    `slog.String("layer", "application")`.
  - `newLoggerForHandler` becomes
    `newLoggersForHandler(handlerConfig) (engineLogger, appLogger *slog.Logger)`.
    Both get the same `handler` identity attribute.

- [ ] Remove every `slog.String("component", ...)`.
- [ ] Rename each `Logger *slog.Logger` field to `EngineLogger`. That's
  `signal.Bus`, `cluster.Node`, `eventstream.Catalog`, `eventstream.Consumer`,
  `source.Local`, `grpcserver.Server`, `remoteengine.Connector`,
  `messagepump.MessagePump`, `process.Sink`, `projection.Sink` and
  `projection.Compactor`. Rename `deliveryTask.Logger` to `EngineLogger`.
- [ ] Rename the `logger` parameter of `messagepump.Driver.HandleDelivery`, and
  of its implementations, to `engineLogger`.
- [ ] For now, the scopes keep logging through a renamed `engineLogger` field.
  Task 7 moves their application records to `appLogger`.
- [ ] Add a CHANGELOG entry under `[Unreleased]` → `Changed`: "Log records
  have a `layer` attribute, `application` or `engine`, and carry the
  application's identity. The `component` attribute has been removed."
- [ ] Run `make precommit`. Expect it to pass.
- [ ] Stop for review.

### Task 7: Application records written after commit

This task stays out of the message pumps, which #100 redesigns. The aggregate
and integration command pumps and the process deadline pump give their scopes
an application logger that writes at once, as today. Buffering those paths
until commit waits for #100.

**Files:**
- Create: `internal/x/xslog/buffer.go`
- Modify: `internal/aggregate/commandscope.go`, `commandpump.go`
- Modify: `internal/integration/commandscope.go`, `commandpump.go`
- Modify: `internal/process/messagescope.go`, `sink.go`, `deadlinepump.go`
- Modify: `internal/projection/eventscope.go`, `sink.go`, `compactor.go`
- Modify: `engine.go`, `execute.go`

**Interfaces:**
- Consumes: the loggers from Task 6.
- Produces:

  ```go
  // Buffer is an [slog.Handler] that holds records until they are flushed,
  // keeping each record's time and the attributes and groups added to its
  // logger.
  type Buffer struct {
      // Target is the handler that flushed records are written to.
      Target slog.Handler
      // unexported state
  }

  func (b *Buffer) Enabled(ctx context.Context, level slog.Level) bool
  func (b *Buffer) Handle(ctx context.Context, r slog.Record) error
  func (b *Buffer) WithAttrs(attrs []slog.Attr) slog.Handler
  func (b *Buffer) WithGroup(name string) slog.Handler
  func (b *Buffer) Flush(ctx context.Context)
  ```

  `Enabled` defers to `Target`. A handler returned by `WithAttrs` or
  `WithGroup` shares the buffer and pairs each record with the matching
  derived `Target` handler, so `Flush` writes every record through the handler
  its logger had. A scope's application logger is `slog.New(buffer)`, built
  from the application logger's handler, and the scope logs through it as
  usual.
  - `aggregate.CommandPump`, `integration.CommandPump` and
    `process.DeadlinePump` gain an `AppLogger` field, set in `engine.go`. The
    `messagepump.Driver` interface and `MessagePump` are unchanged.
  - Each scope gains an `appLogger` field, next to its `engineLogger`.

**Records each scope writes:**

| Scope call | Engine record, written at once at DEBUG | Application record, at INFO |
| --- | --- | --- |
| `RecordEvent` | `handler recorded an event` + `xslog.Envelope("event", …)` | the event's `MessageDescription()` + `xslog.Envelope("event", …)` |
| `ExecuteCommand` | `handler executed a command` + `xslog.Envelope("command", …)` | the command's description + `xslog.Envelope("command", …)` |
| `ScheduleDeadline` | `handler scheduled a deadline` + `xslog.Envelope("deadline", …)` | the deadline's description + `xslog.Envelope("deadline", …)` |
| `End` | `handler ended the process instance` | `process instance ended` |
| `Log` | `handler logged a message` + `slog.String("text", …)` | the formatted text |

**When application records are written:**
- Process events: buffered, and flushed in `Sink.consume` after
  `xsql.Transact` returns nil.
- Projection events: buffered, and flushed in `projection.Sink.dispatch` after
  `HandleEvent` returns nil.
- `compactScope.Log` writes straight to the application logger, with no
  engine record and no buffer.
- `execute.go` writes the command's INFO record to `e.appLogger` after
  `xsql.Transact` returns nil, and only when the command was newly enqueued.
- Aggregate, integration and process deadline scopes: at once, through the
  pump's `AppLogger`, until #100.

Every application logger for a handler carries the message being handled as
`xslog.Envelope("message", …)`, the handler identity, and the instance ID
where there is one. It carries no `delivery`, stream or offset attributes.

- [ ] Implement `Buffer`. It's untested, like all logging.
- [ ] Change each scope to write its engine record at once and its application
  record through its application logger, as in the table.
- [ ] Wire the buffers and flush points for process events and projection
  events.
- [ ] Run `make test`. Expect it to pass. Existing behavior tests cover the
  handlers, and review focus 5 holds because flushing happens only on the
  success path.
- [ ] Stop for review.

### Task 8: Heartbeat records

**Files:**
- Modify: `internal/cluster/node.go:176-241`

**Behavior:**
- `sendHeartbeat` logs a failed send at DEBUG (`unable to send heartbeat`) and
  keeps the `error` in an unexported `lastSendErr` field. It clears the field
  after a successful send. Sends happen only on the node's own goroutine, so no
  lock is needed.
- `expireNodes` removing this node logs ERROR `this node's heartbeat was not
  heard within the timeout, failed sends are logged at DEBUG`, with
  `slog.Duration("timeout", n.Timeout)` and `xslog.Error(lastSendErr)` if
  there is one.
- `recordHeartbeat` for this node, after it had been removed, logs INFO `this
  node's heartbeat is heard again`. Track this with an unexported
  `wasLive bool`. The first time this node becomes live stays a DEBUG record.

- [ ] Implement the above. It's untested, like all logging. The existing test
  "it keeps running when a heartbeat can not be sent" (node_test.go:92) still
  covers the behavior.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 9: Levels for the remaining engine records

**Files:**
- Modify: `internal/eventstream/catalog.go:197-210`
- Modify: `internal/grpcserver/server.go:72`
- Modify: `internal/remoteengine/connector.go` (`add` and its returned remove
  function)
- Modify: `engine.go` (`Run`)

**Behavior:**
- **Catalog.** `ErrNotContiguous` is logged at DEBUG, `watching event stream
  source again`, and watching resumes at once. `ErrInvalidSegment` also logs
  WARN `received an invalid segment from an event stream source`, with
  `xslog.Error(err)`, before it's reported as deterministic.
- **gRPC server.** `listening for gRPC requests` moves to INFO.
- **Remote engine connector.** It logs INFO `remote engine added` when `add`
  starts watching, and `remote engine removed` when the returned function stops
  it.
- **Engine.** It logs INFO `engine started` once its components are running,
  and `engine stopping` when `ctx` is canceled.

- [ ] Implement the above. It's untested, like all logging.
- [ ] Run `make precommit`. Expect it to pass.
- [ ] Stop for review.

### Task 10: Sinks don't log the failures they return

The process and projection sinks run under the stream consumer, whose streak
decides about their failures and logs them. Today the sinks log some failures
at ERROR and then return them, so each one is logged twice.

**Files:**
- Modify: `internal/process/sink.go:201,282,351`

**Behavior:**
- Remove each `ErrorContext` call in `process/sink.go` that is followed by
  returning the same failure. They are "unable to unmarshal event", "unable to
  handle event", "unable to route event to instance", and, once #101 has
  landed, "unable to prepare process effects". The projection sink has no such
  calls.
- Each returned `error` carries the context the removed record had, which is
  the event's message ID and, where there is one, the instance ID. For
  example: `fmt.Errorf("unable to handle event %s for instance %q: %w",
  messageID, instanceID, err)`.
- Wrap with `%w` everywhere, so that a recovered panic keeps its stack for
  `xslog.Error`.
- `internal/process/instancestate.go` keeps its records. The process deadline
  pump also calls it, and that pump returns only `messagepump.ErrFailed`, so
  the records are where its failures are logged until #100.

- [ ] Implement the above. It's untested, like all logging.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

### Task 11: Instance descriptions reflect the instance when each record is written

An instance's description is computed once, after the instance loads, and
added to its logger with `With`. A handler can change the instance with
`scope.Mutate()` or by recording events, so later records can carry a stale
description.

**Files:**
- Create: `internal/x/xslog/late.go`
- Modify: `internal/process/sink.go:246-252`, `internal/process/deadlinepump.go:177-183`
- Modify: `internal/aggregate/commandpump.go:93-100`

**Interfaces:**
- Produces:

  ```go
  // Late is an [slog.Handler] that adds attributes to each record as it is
  // handled, so that their values reflect the time each record is written.
  type Late struct {
      // Target is the handler that records are written to.
      Target slog.Handler

      // Attrs returns the attributes to add to a record.
      Attrs func() []slog.Attr
  }
  ```

  It implements `Enabled`, `Handle`, `WithAttrs` and `WithGroup`, deferring
  to `Target` and keeping `Attrs` on the handlers it derives.

**Behavior:**
- The instance's group, `process_instance` or `aggregate_instance`, moves from
  `With` to a `Late` handler wrapped around the logger's handler. Its
  `Attrs` returns the whole group, with the ID, the stream ID for aggregates,
  and the description computed at that moment, using
  `root.ProcessInstanceDescription(scope.ended)` or
  `root.AggregateInstanceDescription()`.
- Use a handler, not a `slog.LogValuer` given to `With`. The built-in `slog`
  handlers resolve a `LogValuer` when `With` is called, so it would still be
  computed only once.
- The whole group is added late, never half of it, because `slog` keeps
  duplicate keys and would write two `process_instance` groups.
- For buffered application records from Task 7, the `Buffer`'s `Target` is
  the `Late` handler, not the other way around. The description is then
  computed when the records are flushed after the commit, which describes the
  instance as committed.
- The process deadline pump is #100's area. Change only its instance logger
  there.

- [ ] Implement `Late` and use it for both kinds of instance. It's untested,
  like all logging.
- [ ] Run `make test`. Expect it to pass.
- [ ] Stop for review.

## After #100

#100 adapts to these changes and to the [logging guide]. Once it is
implemented, this work finishes with:

- Buffering the application records of message pump deliveries until commit,
  for commands and deadlines.
- Per-delivery streaks, including a persisted first-failure time.

## Out of scope

- Factoring away `supervisor.Run`, `Fatal` and `Deterministic`.
- Moving each supervised function to its real success signal and quiet period.
- Logging a coordinator's single ERROR for many failing stream consumers.

<!-- references -->

[logging guide]: ../maintainers/logging.md
