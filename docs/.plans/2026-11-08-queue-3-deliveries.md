# Queue Plan 3: Deliveries

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Before each task, confirm its
> assumptions with the maintainer, as "Reviewability" describes. Don't stage
> or commit code. The maintainer stages and commits each task as they review
> it. Build and test exclusively through `make` (`make test`,
> `make precommit`), never by invoking the Go toolchain directly. Tests start
> PostgreSQL through testcontainers, so Docker must be running.

**Goal:** Make each command and deadline delivery commit all of its effects or
none, write its application records only after it commits, and log its
failures as one streak per message according to the [logging guide].

**Architecture:** The consumer owns every delivery's loggers. It buffers the
application records of each attempt and flushes them after the commit, as the
process sink does for events, so the aggregate and integration scopes can
write engine DEBUG records again. Process handling is split into a preparation
step, which runs all application code and serialization, and a write step, so
a preparation failure writes nothing. A failure streak is a value,
`failure.Streak`, whose methods apply the [logging guide]'s rules.
`failure.Tracker` keeps one in memory for ongoing work, and the message queue
consumer stores a delivery's streak in its message's row.

**Tech stack:** Go, `log/slog`, PostgreSQL.

**Spec:** the [logging guide], the "After #100" section of the
[logging plan][logging-plan], the "Delivery correctness and contention"
section of [#100][100], and [#99][99].

This is the last of three plans for [#100][100]. It depends on [plan 2][plan-2]
for `messagequeue.Consumer`, `messagequeue.Sink` and the claim-first deadline
delivery. Finishing it closes [#99][99], and lets [#102][102] close as
superseded. [#101][101], the hotfix for [#99][99] on `main`, is already closed
as superseded.

## Reviewability

The maintainer reviews every task as one commit on `main`, by hand. The plan
is written to make that possible, and it is a draft: between tasks, the
maintainer and the implementer check the next task's assumptions against what
the last one taught them, and change the plan before changing the code.

- **One task, one commit, one idea.** A task that grows past about 400 lines
  of non-test diff is split before it is reviewed, not after.
- **Renames and moves are their own commits**, with no behavior change in
  them, so the behavior change that follows is a small diff. The implementer
  says which commit is which.
- **Each review stop opens with a summary** of what the diff does, what it
  deliberately doesn't do, which decisions in this plan it relies on, and
  anything it found that the plan got wrong.
- **Tests read as a specification.** Their names say the behavior, and each
  tests one thing, so the maintainer can review the tests first and the code
  second.
- **Before starting a task**, the implementer restates the task's
  assumptions in one message and waits for the maintainer to confirm or
  correct them. An assumption that changes is written back into the plan.

## Decisions that shape the work

These were made with the maintainer, and aren't in [#100][100].

- **Loggers carry only what's true of every record, and the code that knows
  more adds it.** The message queue consumer holds the engine's base loggers
  and passes both to the sink on each call, so the queue sinks hold no loggers.
  Each sink adds its handler's identity, using a shared helper, so the
  `handler` group is the same wherever it's added. The consumer adds the
  identity that the sink reports to its own records about a delivery.
- **The code that decides whether handling counts buffers the application's
  records.** The message queue consumer commits each delivery, so it buffers
  each attempt's application records, and writes them only if the delivery
  completed and the transaction committed. A failed or contended attempt's
  records are discarded, as the event sinks already do.
- **A failed attempt commits nothing that the sink wrote.** On any outcome
  other than delivery, the consumer rolls back the attempt's transaction, then
  postpones the delivery with one statement outside it. Committing a failed
  attempt's writes was the [#99][99] bug, and this makes it impossible
  whatever error a sink returns, instead of relying on each sink to fail
  before it writes. A savepoint would have kept the row's lock throughout, but
  it cost about 9% of throughput with the integration sink, and 19% with a
  sink that does nothing, so successful deliveries pay nothing extra instead.
- **The postponement may find the message taken over.** Once the attempt rolls
  back, the row's lock is released. The postponement locks the row with
  `SKIP LOCKED` and updates it only if `failures` is unchanged. If another
  transaction holds the lock, the row is gone, or `failures` has changed,
  another attempt has taken over the message, and the postponement writes
  nothing. That isn't an error. This node can't attempt the message in
  between, because the attempt is still in progress until the postponement
  returns.
- **Any error from a sink is a failure.** `messagequeue.ErrBusy` is the only
  other signal, for contention. There's no failure sentinel and no `Fail`.
  Sinks return errors with context and don't log them, and the consumer logs
  each failure once. Because a failed attempt is rolled back, an error's
  classification affects only backoff and logging, never what's committed.
- **A failure is deterministic if its error says so.** `xerrors.IsDeterministic`
  returns true for an error that is, or wraps, one created by
  `xerrors.Deterministic`, or a `xerrors.PanicError`, so every panic in
  application code is deterministic. It lives in `xerrors`, not `supervisor`, so
  the code where a failure happens can mark it without depending on the code
  that retries it. It recognizes only `xerrors`' own types, like
  `xerrors.IsFatal`, so an application's error type can't change how the engine
  retries it by accident. `xmessage` wraps an unpack failure with
  `xerrors.Deterministic`, and the process package marks a missing instance or a
  state that can't be unmarshaled the same way. Two consequences are accepted: a
  handler that panics on a transient error gets the longer backoff, and a panic
  in a remote engine discoverer is logged as an ERROR immediately, and
  rediscovery waits up to five minutes.
- **A streak is a value that its holder stores, in memory or in a row.**
  `failure.Streak` holds the number of failures, the time of the first failure,
  and whether the streak has logged its ERROR. Its methods apply the logging
  guide's rules to that state, write the records to the engine logger that the
  holder passes in, and return the time of the next attempt. `failure.Tracker`
  keeps one in memory, with its quiet period. The message queue consumer stores
  one in the message's row, and passes a buffered engine logger that it flushes
  only once the postponement commits. Both follow the guide: a streak writes at
  most one ERROR, its recovery is INFO exactly when it wrote one, and
  `failing_for` is the time from the first failure to the record, because a
  streak lasts until it recovers.
- **A stored streak may duplicate its ERROR, but never loses it.** On the
  attempt where the streak writes its ERROR, the consumer flushes the buffered
  records before the postponement, which then stores that the ERROR was
  written. If the engine stops in between, or the postponement doesn't happen,
  a later attempt writes the ERROR again. Every other attempt flushes only once
  the postponement commits. A comment in the consumer says so.
- **`supervisor` keeps only what runs functions.** `Run` and `Group` stay.
  `Work`, which becomes `failure.Tracker`, `Jitter` and the delay constants move
  to a new `failure` package.
- **The delay is worked out from the number of failures.** It's
  `min(100 ms << n, limit)`, jittered, where `n` is the number of earlier
  failures in the streak, and the limit is 15 seconds for a transient failure
  and five minutes for a deterministic one. A tracker's
  deterministic failure after many transient ones used to double from the
  transient limit, and now gets the delay that its number of failures gives.
  Each kind of work gets an engine option for these values, in the failure
  policy options task.
- **A delivery's times come from Go's clock.** The consumer stores the `Streak`
  that `Extend` updates, and sets `next_attempt_at` from Go's clock, as it
  already compares `next_attempt_at` with Go's clock.
- **Process preparation is one function** used by the event sink and the
  deadline handler. It runs the handler, marshals the state, and seals the
  packers, and returns a value the write step persists. The [#99][99] bug is
  fixed by construction: nothing is written until preparation has finished.
- **Instance attributes are built per record**, with `instanceLogAttr`, as
  the process code does. No `instanceLogger` fixes the instance's ID when it
  is created.

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
- Engine log messages are constant strings. Values go in attributes. Follow
  the [logging guide].
- Fields that hold a logger are named `EngineLogger` and `AppLogger`, or
  `engineLogger` and `appLogger` if unexported. Never use `Logger` or `logger`.
- At most one `&&` or `||` per expression.
- A failure is logged once, by the code that decides what to do about it.

## Review focus

These conditions are easy to get wrong, and no existing test exercises them.
Each one has a test in the task that owns the code.

1. A deadline whose handler emits a command and then fails to marshal its
   state must commit no command, no successor deadline and no state change,
   and must stay queued with one more failure. Task 2 has the tests, from
   [#99][99], for both a returned error and a panic.
2. An attempt whose commit fails after its handler succeeded must write no
   application records. Task 1 covers it by construction, because the flush
   runs only on the commit's success path.
3. A delivery whose streak began before this attempt must keep the stored
   start of the streak, not restart it at the attempt's time. The
   per-delivery streaks task has the test.
4. A migrated row with `failures > 0` and `first_failed_at` equal to its old
   `deliver_at` must be treated as a streak in progress, not a fresh one.
   The per-delivery streaks task's "it keeps the start of a streak in
   progress" covers it, with a row of that shape.

---

### Task 1: Buffered application records

**Decisions** (agreed before starting):

- `messagequeue.Sink.Deliver` gains `engineLogger` and `appLogger` parameters.
  The consumer builds `appLogger` for each attempt with `xslog.Buffered`, from
  its base `AppLogger` with `handled_message` added, and flushes it only if the
  delivery completed and the commit succeeded.
- `Consumer.Sinks` stays a map of sinks. The consumer gains `AppLogger`. The
  aggregate, integration and deadline sinks lose their `EngineLogger` and
  `AppLogger` fields.
- `messagequeue.Sink` gains `LogAttr() slog.Attr`: "LogAttr returns an
  attribute that describes the sink in log records. The consumer adds it to its
  records about deliveries to the sink." The consumer adds it from the point
  where it looks up the sink. It doesn't add it to the loggers it passes to the
  sink.
- Each queue sink's `LogAttr` returns its handler's `handler` group, built with
  `xslog.HandlerIdentity`, which replaces `newLoggerForHandler`'s body, so the
  engine and the sinks build the group the same way. Each sink adds its own
  `LogAttr` to the loggers it's passed, so its records and the consumer's
  records about it carry the same attribute.
- The aggregate and integration scopes gain an `engineLogger`, and write the
  engine DEBUG records that the process scopes write, such as "handler recorded
  an event".
- The [logging guide] gains two passages and a section:
  - in "Layers", that a logger carries only what's true of every record
    written through it, that the code that knows more adds it, and that code
    whose application records depend on an outcome that something else decides
    is passed its application logger by that code;
  - in "Application records", that the code that decides whether the handling
    of a message counts buffers its application records, replacing "Each scope
    buffers…";
  - a section, "How the engine applies these rules", which says it lists some of
    the decisions that follow from the rules, not all of them, with parts for
    the message queue and for projection compaction. Its part for event streams
    is added by the follow-up below, when the code matches it.
- The engine's records about a delivery carry a `delivery` group holding only
  `message_id`, replacing the top-level `message_id`. It identifies the
  delivery, including on the records written before the envelope is unpacked,
  which have no `handled_message`. A delivery can fail in only one streak,
  because nothing resets a message's failure count and completing removes the
  message, so `delivery.message_id` also pairs a streak's ERROR with its
  recovery.
- Failure and recovery records about a delivery carry the same attributes as
  `failure.Tracker`'s, at the top level: `error`, `failures`, `failing_since`,
  `failing_for`, `next_attempt_at` and `next_attempt_in` on each failure, and
  `failures`, `failing_since` and `failing_for` on recovery, without
  `succeeding_since` or `succeeding_for`. The per-delivery streaks task writes
  them through `Streak`. Until then, the other records about a delivery stop
  carrying `failures`.
- The guide's "Streaks" section gains: "When the work can only fail in one
  streak, such as delivering one message, the attributes that identify the work
  also identify the streak." Its `delivery` row describes the group above.

**Follow-up, in the [logging plan][logging-plan]:** the event stream consumers
and their sinks follow the same rule. Each consumer holds the base loggers and
passes them to its sink, which adds its handler's identity. `eventstream.Sink`
gains the same `LogAttr` method, for the consumer's own records. The projection
compactor adds its projection's identity to base loggers. The process and
projection event sinks keep buffering their own application records, because
each decides whether the handling of an event counts. The guide's "How the
engine applies these rules" section gains its part for event streams then.

- [x] **Step 1: Agree the decisions above**
- [x] **Step 2: Write the failing tests**

  Logging isn't tested. The tests are structural: the consumer's test sink
  receives the loggers, and the handler suites compile against the new
  signature.

- [x] **Step 3: Run `make test` to verify they fail**
- [x] **Step 4: Implement it**, and update the [logging guide]
- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Update the [logging plan][logging-plan]:** remove the first and
  third items of its "After #100" section, which this task finishes, and add the
  follow-up above.
- [x] **Step 7: Stop for review**

---

### Task 2: Marshal process state before writing anything

**The bug ([#99][99]).** After a deadline's handler returns, the deadline sink
adds the commands it executed to their queues, then saves the instance's
state. If marshaling the root fails, `Save` returns `ErrFailed`, and the
consumer commits the attempt's transaction to postpone the delivery, which
commits those commands too. The deadline is attempted again from the old state,
and executes the same commands again. The event sink rolls back its own
transaction on any error, so it isn't affected.

**Decisions** (agreed before starting):

- Marshaling the root is the only step after the handler that can fail because
  of the application's code, so it happens before anything is written. There is
  no `effects` type or `prepare` function: the scope already holds the commands,
  the deadlines, `ended` and `mutated`.
- `instance.Save` saves the instance's own changes: it ends the instance, or
  stores its state, if mutated, and adds the new deadlines to its deadline
  queue. It marshals the root before it writes anything, so if it returns
  `messagequeue.ErrFailed`, it has written nothing.
- `writeEffects` encodes the order once: it calls `instance.Save`, then
  `addCommandsToQueues`. Both sinks call only `writeEffects`. The commands
  aren't the instance's, so they aren't written by an `instance` method.
- Until the per-delivery streaks task, a marshaling failure is
  `messagequeue.ErrFailed`.

**Files:**
- Modify: `internal/process/instance.go`, `internal/process/deadlinesink.go`,
  `internal/process/eventsink.go`, `CHANGELOG.md`
- Test: `internal/process/instance_test.go`, through the engine API

- [x] **Step 1: Write the failing tests**

  Through the engine API, with a process handler whose root's
  `MarshalBinary` can be made to fail:

  ```go
  t.Run("it commits none of a deadline's effects when the state can not be marshaled", ...)   // error
  t.Run("it commits none of a deadline's effects when marshaling the state panics", ...)
  t.Run("it commits the effects once marshaling succeeds on a later attempt", ...)
  t.Run("it commits none of an event's effects when the state can not be marshaled", ...)
  ```

  Each checks that no command is queued, no successor deadline is queued,
  the stored state is unchanged, and the original deadline remains with
  `failures = 1`, using the `xtesting` queue helpers.

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement `instance.Save` and `writeEffects`, and use them in both sinks**
- [x] **Step 4: Run `make test` to verify they pass**
- [x] **Step 5: Update `CHANGELOG.md`**

  Under `### Fixed`: a deadline whose handler's state couldn't be
  serialized no longer commits the commands it produced.

- [x] **Step 6: Stop for review**

---

### Task 3: Roll back a failed attempt's writes

Nothing that a sink writes is committed unless the delivery completes. See "A
failed attempt commits nothing that the sink wrote" and "The postponement may
find the message taken over" above. The postponement finding the message locked
or changed isn't tested. Nothing controls whether the postponement or another
transaction reaches the row first after the rollback, so a test would sometimes
pass with broken code. The statement is covered by review instead. The two rare
consequences of not waiting for the lock and comparing only the failure count, a
failure that goes uncounted and a contention postponement that's replaced during
a handoff, are accepted and documented in the code.

**Files:**
- Modify: `internal/messagequeue/delivery.go`, `internal/messagequeue/sink.go`
  (the sink's changes are discarded after an error, so tx needn't stay
  usable), `internal/process/instance.go` (the comment in `writeEffects`
  that relies on `Save` failing before any write)
- Test: `internal/messagequeue/consumer_test.go`

- [x] **Step 1: Measure the savepoint's cost.** Rebuild plan 2's throughput
  load, without committing it, and compare deliveries per second with and
  without the savepoint. Record the result on [#100][100]. If it costs more
  than a few percent, stop and revisit rolling back and postponing in a new
  transaction instead. It cost about 9% with the integration sink, so the
  postponement uses a new transaction.
- [x] **Step 2: Write the failing tests**

  ```go
  t.Run("it discards the sink's changes after a failed attempt", ...)
  t.Run("it discards the sink's changes when the sink is busy", ...)
  ```

- [x] **Step 3: Run `make test` to verify they fail**
- [x] **Step 4: Implement it**
- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Stop for review**

---

### Task 4: Move failure handling to the `failure` package

A move commit, with no renames and no change in behavior. It starts from the
committed code. The earlier, uncommitted `supervisor.Streak` work is set aside
first, and isn't part of this commit.

**Files:**
- Move: `internal/supervisor/work.go`, `work_test.go`, `jitter.go` and
  `jitter_test.go` to `internal/failure/`
- Modify: `internal/supervisor/supervisor.go` (`baseDelay`,
  `transientMaxDelay` and `deterministicMaxDelay` move to
  `internal/failure/`, and `quietPeriod` stays), and the other callers of
  `Work` and `Jitter`, namely `internal/messagequeue/consumer.go` and
  `internal/messagequeue/delivery.go`
- Create: the `failure` package's doc comment

- [x] **Step 1: Restate the assumptions**, including the first sentence of the
  `failure` package's doc comment
- [x] **Step 2: Move the files and update the callers**
- [x] **Step 3: Run `make precommit`**
- [ ] **Step 4: Stop for review**

---

### Task 5: Rename `Work` to `Tracker`

A rename commit, using `gopls rename`. Its only change that isn't mechanical
is rewriting the doc comments and other comments that the rename makes false.

**Files:**
- Move: `internal/failure/work.go` and `work_test.go` to `tracker.go` and
  `tracker_test.go`
- Modify: every caller of `failure.Work`, and the comments that describe it,
  such as `Tracker`'s own doc, the consumer's field comment, and
  `supervisor.Run`'s doc

`failure.Work` becomes `failure.Tracker`, and its receiver becomes `t`.
Callers' fields and locals that hold one are named `failures`, such as the
consumer's `c.work`, which becomes `c.failures`. Check that this reads well in
`internal/messagequeue/consumer.go`, beside the counts that `failures` also
names there.

- [x] **Step 1: Restate the assumptions**, including the first sentence of
  `Tracker`'s doc comment
- [x] **Step 2: Rename the type, its files, and the callers' fields and
  locals, and rewrite the comments**
- [x] **Step 3: Run `make precommit`**
- [ ] **Step 4: Stop for review**

---

### Task 6: The streak value

**Files:**
- Create: `internal/failure/streak.go`, `internal/failure/streak_test.go`
- Modify: `internal/failure/tracker.go`, `internal/failure/tracker_test.go`,
  `internal/supervisor/supervisor.go`, `internal/supervisor/supervisor_test.go`,
  and every other caller of `Tracker.FailedDeterministically`

**Interfaces:**
- Consumes: `xerrors.IsDeterministic`, from a prep commit that's already made.
- Produces: `failure.Streak`, a value whose zero value means no streak is in
  progress. Its exported fields are `Failures`, `FirstFailedAt` and
  `FailureLoggedAsError`. `Extend`, which has a pointer receiver, classifies the
  error with `xerrors.IsDeterministic`, updates the streak, writes the failure
  record, and returns the time of the next attempt. `End` writes the recovery
  record. The description and the engine logger aren't part of a streak's state,
  so each holder passes them in. `Tracker` adds `succeeding_for` to the engine
  logger that it passes to `End`, because only a tracker has a quiet period.

  The records' attributes, which the logging guide's "Streaks" section
  documents:

  | Record | Times | Durations |
  | --- | --- | --- |
  | Failure | `failing_since`, `next_attempt_at` | `failing_for`, `next_attempt_in` |
  | Recovery | `failing_since` | `failing_for` |
  | Recovery, tracker only | `succeeding_since` | `succeeding_for` |

  Every duration starts or ends at the record's own time. `failing_for` is on
  every failure record, not only the ERROR, and `delay` becomes
  `next_attempt_in`.

  `Tracker` holds a `Streak` behind its mutex, and adds only the quiet period,
  the time of the success that began it, and completion. Once the tracker is
  complete, `Failed` returns zero at once, because no more attempts are made.
  Its own backoff, `FailedDeterministically`, and its
  separate fields for the streak's state go. `supervisor.Run` calls `Failed`
  for every failure that isn't fatal.

  The comments in the `failure` package say "logged as a DEBUG record" and so
  on, not "at <level>".

- [x] **Step 1: Agree the shape of `Streak` with the maintainer**
- [x] **Step 2: Write the failing tests**

  `streak_test.go`, with `synctest`:

  ```go
  func TestStreak(t *testing.T) {
      t.Run("it returns the base delay for the first failure", ...)
      t.Run("it doubles the delay for each failure up to the transient limit", ...)
      t.Run("it uses the deterministic limit for a deterministic failure", ...)
      t.Run("it bases a deterministic failure's delay on the number of failures", ...)
      t.Run("it doesn't overflow after many failures", ...)
      t.Run("it starts the streak at the first failure", ...)
      t.Run("it keeps the start of a streak in progress", ...)
      t.Run("it marks that it logged its ERROR once it qualifies for one", ...)
      t.Run("it ends the streak when the work succeeds", ...)
  }
  ```

  The tracker's delay tests move from `tracker_test.go` to `streak_test.go`.
  `supervisor_test.go` keeps its delay tests, as tests of `Run` from
  end to end, except "it grows the delay from the transient cap after a
  deterministic failure", which goes, because that behavior goes.

- [x] **Step 3: Run `make test` to verify they fail**
- [x] **Step 4: Implement `Streak`, and make `Tracker` use it**
- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Update the logging guide's attributes for streaks**
- [ ] **Step 7: Stop for review**

---

### Task 7: Rename the tracker's methods

A rename commit, using `gopls rename`. `Tracker.Failed` and
`Tracker.Succeeded` become `Tracker.WorkFailed` and `Tracker.WorkSucceeded`,
because the work fails or succeeds, not the tracker.

- [x] **Step 1: Rename the methods, and the comments that mention them**
- [x] **Step 2: Run `make precommit`**
- [ ] **Step 3: Stop for review**

---

### Task 8: Per-delivery streaks

**Files:**
- Modify: `internal/schema/ddl/10-messagequeue/00-tables.sql` (the new
  column, and `first_failed_at` renamed) and
  `internal/schema/ddl/10-messagequeue/90-migrate.sql` (fill it for migrated
  rows)
- Modify: `internal/messagequeue/delivery.go`,
  `internal/messagequeue/sink.go`
- Modify: `internal/aggregate/commandsink.go`, `internal/aggregate/instance.go`,
  `internal/integration/commandsink.go`, `internal/process/deadlinesink.go`,
  `internal/process/instance.go` (return errors with context, mark the
  deterministic ones with `xerrors.Deterministic`, and stop logging them)
- Modify: `internal/failure/jitter.go` and `jitter_test.go` (`Jitter` becomes
  unexported once nothing outside the package calls it, so its test goes or
  moves inside the package)
- Test: `internal/messagequeue/consumer_test.go`,
  `internal/schema/schema_test.go`

**Interfaces:**
- Consumes: the rollback from the rollback task, `failure.Streak` from the
  streak value task, and `xerrors.IsDeterministic`.

  The `failing_since` column is renamed `first_failed_at`. The message row gains
  `failure_logged_as_error`, which is `NOT NULL DEFAULT false`, and is false for
  a migrated row.

  `Delivery` replaces `Failures` and `FailingSince` with one field, `Streak
  failure.Streak`. A process root that can't be marshaled is deterministic. The
  contention record's `delay` attribute becomes `next_attempt_in`, beside a new
  `next_attempt_at`. `jitter_test.go` is deleted once `Jitter` is unexported,
  because `Streak`'s delay tests already check the jitter's range.

  `ErrFailed` goes, and any error from a sink except `ErrBusy` is a failure.
  A missing process instance, or a state that can't be unmarshaled, is
  wrapped with `xerrors.Deterministic`, and so is the invalid envelope that
  the attempt itself rejects.

  The attempt builds a `Streak` from the row, and passes the description
  "deliver message" when it calls `Extend` and `End`. It calls `Extend` on any
  error except `ErrBusy`, and stores the returned streak and `next_attempt_at`,
  which is the time that `Extend` returns, in the statement that postpones the
  message. The engine logger that it passes to `Extend` is buffered, and flushed
  only once the postponement commits, so that an outage doesn't log a failure
  for every message attempted. If the postponement finds the message taken over,
  it stores nothing and discards the record. The exception is the attempt where
  the streak writes its ERROR, which flushes before the postponement. The
  streak's failure record replaces "message delivery postponed" for failures.
  `ErrBusy` neither fails nor succeeds the streak. Contention keeps the streak
  as it is, and keeps its "message delivery postponed" record. An attempt that
  fails because the consumer is stopping writes nothing. After a successful
  commit, the attempt calls `End`.

  The delivery's own backoff, `failureBackoffBase` and `failureBackoffMax`,
  goes.

- [x] **Step 1: Restate the assumptions**, including what `Delivery` holds
- [x] **Step 2: Write the failing tests**

  `consumer_test.go`:

  ```go
  t.Run("it stores the start of the streak on the first failure", ...)
  t.Run("it keeps the start of a streak in progress", ...)
  t.Run("it stores that the streak logged its ERROR", ...)
  t.Run("it keeps the streak through contention", ...)
  t.Run("it postpones a deterministic failure by up to the deterministic limit", ...)
  t.Run("it postpones any other failure by up to the transient limit", ...)
  ```

  `schema_test.go`'s `TestCreate_migratesQueues` checks `first_failed_at`
  and `failure_logged_as_error`.

  The sinks' tests check that these failures are deterministic: an empty
  instance ID from an aggregate or process handler's routing, a missing
  deadline instance, a process state that can't be unmarshaled, and a process
  root that can't be marshaled. The process event sink's empty instance ID is
  marked deterministic, to match the aggregate sink.

- [x] **Step 3: Run `make test` to verify they fail**
- [x] **Step 4: Implement it**
- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Update the logging plan.** Remove the per-delivery streaks
  from "After #100". The section's other items stay.
- [x] **Step 7: Stop for review**

---

### Task 9: A separate streak for dispatching events

The event stream consumer runs all of its work under one `supervisor.Run`, so
one streak counts both failures to read events from the stream and failures
of its sink to handle them. The consumer keeps its sink's failures in a streak
of its own, with a `failure.Tracker`, so that the failure policy options task
can give the dispatching of events its own policy. Only failures to read
events reach `supervisor.Run`.

Each stream consumer has a second `failure.Tracker`, described as "dispatch
events", with no quiet period. Errors from the sink's `CheckpointOffset` and
`ConsumeSegment` are its failures, because both run the handler's code, and
its streak ends when the sink next handles a segment. Errors from reading the
stream, and checkpoint conflicts, stay with `supervisor.Run`. As today, a
failure unwinds out of `Catalog.Consume`, and the consumer waits with nothing
open, then consumes again from the checkpoint. No new tests: both trackers use
the same values until the failure policy options task, which tests the split.

- [x] **Step 1: Restate the assumptions**
- [x] **Step 2: Implement it**
- [x] **Step 3: Run `make precommit`**
- [ ] **Step 4: Stop for review**

---

### Task 10: Check every failure for an engine bug

In v0.2.0, the message pump ran under `supervisor.Run`, which passes every
error through `xsql.PanicOnDeadlock`, so a deadlock fails the tests.
`messagequeue.Consumer` doesn't run under `supervisor.Run`, so its own errors
and its sinks' errors aren't checked. Every failure that the engine counts
passes through `failure.Streak.Extend`, so the check moves there, as the
unexported `failure.panicOnEngineBug`, whose doc says only that it panics on
an error that shows a bug in the engine, under `go test`. `xsql.PanicOnDeadlock`
and the calls in `supervisor.Run` and `eventstream.Consumer` go.

One commit, because the callers can't call an unexported function, so a pure
move isn't possible.

- [x] **Step 1: Implement it**
- [x] **Step 2: Run `make precommit`**
- [x] **Step 3: Stop for review**

---

### Task 11: Engine options for failure policies

A failure policy is how the engine responds to failures of a kind of work: how
long it waits before each retry, and when it logs a streak of failures as an
ERROR. Each kind of work gets an engine option that takes a whole policy, so
an operator can tune them separately.

The public type is `FailurePolicy`, with these fields and defaults:

- `RetryDelay`, the delay before the first retry, 100 milliseconds
- `TransientRetryDelayLimit`, the longest delay after a transient failure, 15
  seconds
- `DeterministicRetryDelayLimit`, the longest delay after a deterministic
  failure, 5 minutes
- `ErrorLogThreshold`, how old a streak of transient failures must be before a
  failure is logged as an ERROR, 30 seconds

Every field must be positive, and each option panics otherwise. Internal
packages don't check their policies, because `engine.go` sets them.

| Option | Covers | Field it sets |
| --- | --- | --- |
| `WithEngineFailurePolicy` | the engine's own work, including reading event streams | `EngineFailurePolicy` |
| `WithMessageQueueDeliveryFailurePolicy` | command and deadline deliveries | `DeliveryFailurePolicy` |
| `WithEventStreamDispatchFailurePolicy` | the process and projection sinks handling events | `DispatchFailurePolicy` |
| `WithProjectionCompactionFailurePolicy` | projection compaction | `CompactionFailurePolicy` |

`messagequeue.Consumer` and `eventstream.Consumer` each take two policies. The
other components take `EngineFailurePolicy`, and `projection.Compactor` takes
`CompactionFailurePolicy`.

`FailurePolicy` is an alias for `failure.Policy`. Because godoc doesn't show
the fields of an aliased type from another package, its doc comment describes
each field. The default is one function, `DefaultFailurePolicy()`, which every
option uses, so that adding a field to the policy later doesn't break callers
who start from it. It's a function because a struct can't be a constant.
Every other option's default is a function too, for symmetry and so that a
default can change without breaking callers: `DefaultMessageQueueConcurrency()`
is new, and the five default constants become functions. One of them,
`DefaultProjectionCompactInterval`, was released as a constant, so it has a
**[BC]** changelog entry. Each function returns a value that the caller owns.
`Streak.Extend`, `Streak.End`, `supervisor.Run` and `supervisor.Group.Go` take
the description first, then the policy, then the engine logger, and
`failure.Tracker` has a `Policy` field. The `failure` package keeps no
defaults.

The quiet period after which `supervisor.Run` ends a streak has no option for
now. The message queue's contention delay already has an option,
`WithMessageQueueContentionDelay()`. The logging guide states 30 seconds as
the default ERROR threshold, and names the options.

- [x] **Step 1: Restate the assumptions**
- [x] **Step 2: Write the failing tests**
- [x] **Step 3: Implement it, and add the options to `CHANGELOG.md`**
- [x] **Step 4: Stop for review**

---

### Task 12: Fix "at <level>" in the logging guide

A small commit of its own, at any point. Replace "at <level>" with "as an
ERROR", "as a DEBUG record" and so on in `docs/maintainers/logging.md` and
`internal/cluster/node.go`.

Also reword the docs in `internal/failure/tracker.go` that use "records" as a
verb, because "record" is a Dogma term, and the process event sink's
`dispatch` doc, which says "applies its effects", because "apply" is Dogma's
word for aggregate roots. It writes its effects. For example, "WorkFailed adds a
failure of the work to the streak in progress, or starts a streak".

- [x] **Step 1: Fix the wording**
- [ ] **Step 2: Stop for review**

---

### Task 13: Unify field naming and order

The queue work adds many structs beside older ones, and their fields drifted
apart along the way. This task makes the whole codebase consistent, not just
the code the queue plans touched.

`internal/cluster/rendezvous.go` is the reference for how the code should
read. Above all, it models a concept from the domain as a value object
(`scoredNode`) with methods named for what they mean there (`Outranks`).
Each early return also has a comment giving its reason, names pair up by
role, and zero values mean something. Where this pass touches a file, bring
it closer to that standard.

**Files:**
- Modify: any Go file whose struct fields or struct literals change, and any
  DDL file whose columns or parameters change.

Rename the bare `logger` names in the event stream package to name their
layer, as the logging guide requires.

- [ ] **Step 1: Take an inventory.** List every exported and unexported struct
  that holds dependencies or configuration, with its fields in order, and every
  table and SQL function with its columns or parameters in order. Group them by
  role, such as handler identity, database, cluster, signals, timeouts and
  loggers, so the differences are easy to see.
- [ ] **Step 2: Agree the conventions with the maintainer, one at a time.** For
  each difference the inventory shows, propose one rule, with the examples that
  break it. Expect rules for field names, field order, which fields are
  documented, and the order of fields in struct literals. Record each agreed
  rule below. The rules are specific to this project, so they don't go in the
  shared skills in [dogmatiq/agent-config][agent-config].
- [ ] **Step 3: Apply the rules in rename-only commits**, one rule per commit,
  so each diff shows a single mechanical change. Run `make test` after each.
- [ ] **Step 4: Stop for review** after each commit.

The rules agreed so far, each applied in its own commit:

1. **Field order.** The exported fields of a struct that a caller fills in
   come in this order: the domain dependencies, from least specific to most
   specific, then configuration, then cross-cutting dependencies, which are the
   loggers. A failure policy is configuration. Unexported fields follow all of
   the exported ones, after a blank line. Fields that are the same thing, such
   as a handler and its identity, or a listener and its address, sit together
   and share one comment, even if the rule would otherwise put them in
   different groups. Struct literals follow the struct's order.
2. **A field is named for what it is.** A field never shares a name with a
   field of another meaning: the handler's `dogma.ConcurrencyPreference` is
   `ConcurrencyPreference`, and the consumer's limit is `ConcurrencyLimit`.
3. **A set of IDs is named for its IDs.** A field that holds a set of message
   type IDs, or a method that returns one, is named `…MessageTypeIDs`, or
   `…EventTypeIDs`, `…CommandTypeIDs` or `…DeadlineTypeIDs` if the set holds
   only one kind. It is never a bare `…TypeIDs`. `…Types` is for collections of
   Go types. A name that would collide with the other, such
   as a sink's field and its `EventTypeIDs()` method, gets a qualifier on the
   field: `InboundEventTypeIDs`. Parameters keep `types`.
4. **A struct that does one thing may use bare names.** When a struct's own
   name says what its fields apply to, and it holds no other field of the same
   kind, the fields need no qualifier: the `Compactor`'s `Interval`, `Timeout`
   and `FailurePolicy`. A struct that holds several fields of one kind, such as
   a consumer's `EngineFailurePolicy` and `DeliveryFailurePolicy`, qualifies
   each.
5. **A logger is named for its role.** A logger field, parameter or local is
   `engineLogger` or `appLogger`, or is named for its job if it is neither: a
   helper's `target`, a function's `base`, a test's `testLogger`. It is never a
   bare `logger`. The one exception is the public `WithLogger(logger)`, which
   sets both loggers.

---

### Task 14: Close out

**Files:**
- Modify: `CHANGELOG.md`, `docs/.plans/` (delete the queue plans and the
  logging plan)

- [ ] **Step 1: Add one entry for the net change since v0.2.0 in retrying and
  logging failures.** No commit on this branch has recorded it. An ordinary
  projection failure now waits at most 15 seconds instead of five minutes, and
  its streak logs one ERROR instead of one per failure. A panic, or a message
  that can't be unpacked, is deterministic, so it's logged as an ERROR
  immediately and waits up to five minutes.
- [ ] **Step 2: Read the `[Unreleased]` section as a whole** and make its
  entries for the queue read as one change, with the stop-migrate-restart
  note once, and check that it agrees with the [migration guide].
- [ ] **Step 3: Delete the completed plans**
- [ ] **Step 4: Stop for review**, then comment on [#100][100], [#99][99],
  [#102][102], [#58][58] and [#91][91] with what landed and the follow-ups from
  plan 1.

---

## Out of scope

- The follow-ups in [plan 1][plan-1].
- Factoring `supervisor.Run` away.
- Instance placement.

<!-- references -->

[58]: https://github.com/dogmatiq/runkit/issues/58
[91]: https://github.com/dogmatiq/runkit/issues/91
[99]: https://github.com/dogmatiq/runkit/issues/99
[100]: https://github.com/dogmatiq/runkit/issues/100
[101]: https://github.com/dogmatiq/runkit/pull/101
[102]: https://github.com/dogmatiq/runkit/pull/102
[agent-config]: https://github.com/dogmatiq/agent-config
[logging guide]: ../maintainers/logging.md
[logging-plan]: 2026-11-08-logging-and-retries.md
[migration guide]: ../MIGRATING-v0.3.0.md
[plan-1]: 2026-11-08-queue-1-storage.md
[plan-2]: 2026-11-08-queue-2-consumer.md
