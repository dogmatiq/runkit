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
a preparation failure writes nothing. A delivery's failure streak is stored on
its row, in `failures` and `failing_since`, and `supervisor.Streak` decides
each failure's level and delay from those columns alone.

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
- **The streak lives on the row.** `supervisor.Streak` is a value built from
  `Failures` and `FailingSince`. It writes the failure record as an ERROR when
  the failure is deterministic or the streak is older than `maxDebugStreak`, and
  otherwise as a DEBUG record, and it returns the delay. There is no in-memory
  state, so it can't know whether an earlier attempt wrote an ERROR. Once a
  streak qualifies for an ERROR, every failed attempt writes one, which is at
  most once per five minutes for a deterministic failure and once per 15 seconds
  for any other. The logging guide states this as an exception for streaks
  stored outside the engine's memory. Recovery is INFO if the streak is older
  than `maxDebugStreak`, and DEBUG otherwise. Storing whether a streak has
  written its ERROR, in a new column, is tried in its own task after the rest of
  this plan.
- **A delivery's backoff is `Work`'s.** It doubles from 100 ms, up to 15
  seconds for a transient failure and five minutes for a deterministic one.
  It will be made configurable later, outside this plan.
- **A delivery's times come from Go's clock.** The consumer stores the start
  of the streak that `Streak` returns, and sets `next_attempt_at` from Go's
  clock, as it already compares `next_attempt_at` with Go's clock.
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
   start of the streak, not restart it at the attempt's time. Task 4 has the
   test.
4. A migrated row with `failures > 0` and `failing_since` equal to its old
   `deliver_at` must be treated as a streak in progress, not a fresh one.
   Task 4 has the test.

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
  `supervisor.Work`'s, at the top level: `error`, `failing_since`, `failures`
  and `delay` on each failure, plus `failing_for` on the ERROR, and
  `failing_since`, `failures` and `failing_for` on recovery, without
  `succeeding_for`. Task 4 writes them through `Streak`. Until then, the other
  records about a delivery stop carrying `failures`.
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
- Until task 4, a marshaling failure is `messagequeue.ErrFailed`.

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

### Task 4: Per-delivery streaks

**Files:**
- Create: `internal/supervisor/streak.go`, `internal/supervisor/failure.go`
- Modify: `internal/supervisor/work.go`
- Modify: `internal/messagequeue/delivery.go`,
  `internal/messagequeue/sink.go`
- Modify: `internal/aggregate/commandsink.go`, `internal/aggregate/instance.go`,
  `internal/integration/commandsink.go`, `internal/process/deadlinesink.go`,
  `internal/process/instance.go` (return errors with context, mark the
  deterministic ones with `xerrors.Deterministic`, and stop logging them)
- Test: `internal/supervisor/streak_test.go`,
  `internal/messagequeue/consumer_test.go`

**Interfaces:**
- Produces:

  ```go
  // Streak is a streak of consecutive failures of one piece of work whose
  // failures are stored outside the engine's memory, such as in a queued
  // message's row.
  type Streak struct {
      Description  string
      EngineLogger *slog.Logger
      Failures     uint64    // before this attempt
      Since        time.Time // zero if Failures is zero
  }

  // Failed writes the record for a failure of the work, and returns the
  // delay before the next attempt and the time of the first failure in the
  // streak. The failure is deterministic if xerrors.IsDeterministic returns
  // true for err.
  func (s Streak) Failed(
      ctx context.Context,
      err error,
  ) (delay time.Duration, since time.Time)

  // Succeeded writes the streak's recovery record, if a streak is in
  // progress.
  func (s Streak) Succeeded(ctx context.Context)
  ```

  `ErrFailed` goes, and any error from a sink except `ErrBusy` is a failure.
  A missing process instance, or a state that can't be unmarshaled, is
  wrapped with `xerrors.Deterministic`.

  The level rules are the ones in `Work.fail`, with `loggedError` replaced
  by "`Since` is older than `maxDebugStreak`": ERROR if deterministic or
  the streak is older than that; else DEBUG. Recovery is INFO if the streak
  is older than `maxDebugStreak`, else DEBUG. The attributes match `Work`'s.
  `Streak`'s backoff is `min(baseDelay << Failures, limit)`, with `limit` by
  determinism, jittered. `Work` keeps its own backoff, which doubles from the
  previous delay, because a deterministic failure after transient ones grows
  from the transient limit. `Work` and `Streak` share the level rule and the
  failure record through unexported functions.

  The attempt builds a `Streak` from `Delivery.Failures` and
  `Delivery.FailingSince` with the description "deliver message", calls
  `Failed` on any error except `ErrBusy`, stores `failures + 1` and `since`,
  sets `next_attempt_at` to Go's time plus the delay, and calls `Succeeded`
  after a successful commit. It writes the failure record only once the
  postponement commits, so that an outage doesn't log a failure for every
  message attempted. The streak's failure record replaces "message delivery
  postponed" for failures. `ErrBusy` neither fails nor succeeds the streak.
  Contention keeps `failing_since` as it is, and keeps its "message delivery
  postponed" record. An attempt that fails because the consumer is stopping
  writes nothing.

  Before this task, two commits prepare for it. A rename-only commit gives
  the bare `logger` parameters in the aggregate and process packages the name
  `engineLogger`, as the logging guide requires. Another moves `Fatal` and
  the classification of deterministic failures from `supervisor` to
  `xerrors`, as `xerrors.Fatal`, `xerrors.IsFatal`, `xerrors.Deterministic`
  and `xerrors.IsDeterministic`, and wraps unpack failures in `xmessage` with
  `xerrors.Deterministic`.

  Also replace "at <level>" with "as an ERROR", "as a DEBUG record" and so
  on in `docs/maintainers/logging.md`, `internal/supervisor/work.go` and
  `internal/cluster/node.go`.

- Consumes: the rollback from the previous task, and `xerrors.IsDeterministic`.

- [ ] **Step 1: Write the failing tests**

  `streak_test.go`, with `synctest`:

  ```go
  func TestStreak(t *testing.T) {
      t.Run("it returns the base delay for the first failure", ...)
      t.Run("it doubles the delay for each failure up to the limit", ...)
      t.Run("it uses the deterministic limit for a deterministic failure", ...)
      t.Run("it returns the given start of the streak", ...)          // since == Since when Failures > 0
      t.Run("it starts the streak at the first failure", ...)        // since == now when Failures == 0
  }
  ```

  `consumer_test.go`:

  ```go
  t.Run("it stores the start of the streak on the first failure", ...)
  t.Run("it keeps the start of the streak on later failures", ...)
  t.Run("it keeps the streak through contention", ...)
  t.Run("it postpones a deterministic failure by up to the deterministic limit", ...)
  t.Run("it postpones any other failure by up to the transient limit", ...)
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement `Streak`, the deterministic errors, and the sink
  changes**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Update the logging guide and the logging plan**

  In the guide, state the exception for streaks stored outside the engine's
  memory: each failed attempt writes an ERROR once the streak qualifies for
  one. In the logging plan, remove the per-delivery streaks from "After
  #100", and note in "Out of scope" that `Work` and `Streak` could later
  share a type. The section's other items stay.

- [ ] **Step 6: Stop for review**

---

### Task 5: Unify field naming and order

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
  rule in the shared skills in [dogmatiq/agent-config][agent-config], not only
  in the code.
- [ ] **Step 3: Apply the rules in rename-only commits**, one rule per commit,
  so each diff shows a single mechanical change. Run `make test` after each.
- [ ] **Step 4: Stop for review** after each commit.

---

### Task 6: Store whether a delivery's streak has written its ERROR

Tried after the rest of this plan, so that a long or deterministic streak
writes one ERROR, as the logging guide requires, instead of one per attempt.
Its assumptions are restated before it starts.

- [ ] **Step 1: Restate the assumptions**, including the column's name and
  whether it joins the 0.3.0 migration.
- [ ] **Step 2: Implement, removing the exception from the logging guide**
- [ ] **Step 3: Stop for review**

---

### Task 7: Close out

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
- Factoring `supervisor.Run` away, and merging `Work` and `Streak`.
- Making the delivery backoff configurable.
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
