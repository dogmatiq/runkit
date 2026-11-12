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
- **A failure is logged once, by the consumer.** Handlers return errors with
  context and don't log them. `messagequeue.Fail(err)` marks an application
  failure to postpone with a failure counted. `messagequeue.ErrBusy` marks
  contention. `supervisor.Deterministic` marks one that retrying can't clear.
  Any other error rolls the attempt back and leaves the row for another attempt.
- **The streak lives on the row.** `supervisor.Streak` is a value built from
  `Failures` and `FailingSince`. It writes the failure record at ERROR when
  the failure is deterministic or the streak is older than `maxDebugStreak`,
  and otherwise at DEBUG, and it returns the delay. There is no in-memory
  state, so it can't know whether an earlier attempt wrote an ERROR; it
  infers it from the streak's age, and writes the recovery at INFO when the
  streak was old enough to have, and at DEBUG otherwise. A deterministic
  failure's ERROR repeats on each attempt, which is at most once per five
  minutes, because nothing can carry "already logged" across attempts
  without another column. That is accepted for now.
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
3. A message whose first failure was more than `maxDebugStreak` ago must get
   exactly one ERROR per attempt, not one per retry of the same attempt.
   Task 3 has the test.
4. A migrated row with `failures > 0` and `failing_since` equal to its old
   `deliver_at` must be treated as a streak in progress, not a fresh one.
   Task 3 has the test.

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
  `succeeding_for`. Task 3 writes them through `Streak`. Until then, the other
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
- Until task 3 adds `messagequeue.Fail`, a marshaling failure is
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

### Task 3: Per-delivery streaks

**Files:**
- Create: `internal/supervisor/streak.go`, `internal/messagequeue/fail.go`
- Modify: `internal/messagequeue/delivery.go`,
  `internal/messagequeue/sink.go`
- Modify: `internal/aggregate/commandsink.go`,
  `internal/integration/commandsink.go`,
  `internal/process/deadlinesink.go` (return errors with context; stop
  logging them; `messagequeue.Fail` for application failures;
  `supervisor.Deterministic` for unmarshal failures)
- Test: `internal/supervisor/streak_test.go`,
  `internal/messagequeue/consumer_test.go`

**Interfaces:**
- Produces:

  ```go
  // Streak is a streak of consecutive failures of one piece of work whose
  // state is stored elsewhere, such as a queued message's row.
  //
  // Unlike Work, it holds no state between calls. The caller stores
  // Failures and Since, and builds a new Streak from them for each attempt.
  type Streak struct {
      Description  string
      EngineLogger *slog.Logger
      Failures     uint64      // before this attempt
      Since        time.Time   // zero if Failures is zero
  }

  // Failed writes the record for a failure of the work and returns the delay
  // before the next attempt, and the time the streak began.
  func (s Streak) Failed(
      ctx context.Context,
      err error,
  ) (delay time.Duration, since time.Time)

  // FailedDeterministically does the same for a deterministic failure.
  func (s Streak) FailedDeterministically(
      ctx context.Context,
      err error,
  ) (delay time.Duration, since time.Time)

  // Succeeded writes the recovery record if a streak was in progress.
  func (s Streak) Succeeded(ctx context.Context)

  // In messagequeue:
  // Fail marks err as a failure of the delivery itself, to be postponed with
  // its failure count incremented.
  func Fail(err error) error
  var ErrFailed  // what Fail's result wraps; replaces the bare sentinel use
  ```

  The level rules are the ones in `Work.fail`, with `loggedError` replaced
  by "`Since` is older than `maxDebugStreak`": ERROR if deterministic or
  the streak is older than that; else DEBUG. Recovery is INFO if the streak
  was older than `maxDebugStreak`, else DEBUG. The attributes match `Work`'s.
  The backoff is `Work`'s, computed from `Failures` without a stored
  `backoff`: `min(baseDelay << Failures, limit)`, with `limit` by
  determinism, jittered. `Work` and `Streak` share these through unexported
  functions.

  The attempt builds a `Streak` from `Delivery.Failures` and
  `Delivery.FailingSince` with the description "deliver message", calls `Failed`
  or `FailedDeterministically` on `ErrFailed` and deterministic errors, stores
  `failures + 1` and `since` with the delay, and calls `Succeeded` after a
  successful commit. `ErrBusy` neither fails nor succeeds the streak. Contention
  keeps `failing_since` as it is.

  Handlers stop logging "unable to handle command", "unable to unmarshal
  command" and the like, and return `messagequeue.Fail(fmt.Errorf("unable to
  handle command %s: %w", id, err))`, or `supervisor.Deterministic(...)` for
  an envelope or message that can't be unmarshaled. The consumer's attempt
  logs each failure once through the streak, with `handled_message`.

- Consumes: `supervisor.Work`'s unexported helpers, `maxDebugStreak`.

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
  t.Run("it keeps the start of the streak through contention", ...)
  t.Run("it clears the streak when the message is removed", ...)    // trivially, by removal
  t.Run("it postpones a deterministic failure by the longer limit", ...)
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement `Streak`, `Fail`, and the handler changes**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Update the logging plan**

  Remove its "After #100" section, now empty, and note in "Out of scope"
  that `Work` and `Streak` could later share a type.

- [ ] **Step 6: Stop for review**

---

### Task 4: Unify field naming and order

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

### Task 5: Close out

**Files:**
- Modify: `CHANGELOG.md`, `docs/.plans/` (delete the queue plans and the
  logging plan)

- [ ] **Step 1: Read the `[Unreleased]` section as a whole** and make its
  entries for the queue read as one change, with the stop-migrate-restart
  note once, and check that it agrees with the [migration guide].
- [ ] **Step 2: Delete the completed plans**
- [ ] **Step 3: Stop for review**, then comment on [#100][100], [#99][99],
  [#102][102], [#58][58] and [#91][91] with what landed and the follow-ups from
  plan 1.

---

## Out of scope

- The follow-ups in [plan 1][plan-1].
- Factoring `supervisor.Run` away, and merging `Work` and `Streak`.
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
