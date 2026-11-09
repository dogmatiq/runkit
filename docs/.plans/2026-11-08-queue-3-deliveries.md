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
for `messagequeue.Consumer`, `messagequeue.Handler` and the claim-first deadline
delivery. Finishing it closes [#99][99], and lets [#101][101] and [#102][102]
close as superseded.

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

- **The consumer owns the loggers.** `Consumer.Handlers` maps each message
  type to a `messagequeue.Route`, which pairs the handler with its engine and
  application loggers. The consumer derives the attempt's loggers, buffers
  the application one, and flushes it after the commit. Handlers lose their
  `EngineLogger` and `AppLogger` fields.
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

**Files:**
- Create: `internal/messagequeue/route.go`
- Modify: `internal/messagequeue/consumer.go`,
  `internal/messagequeue/attempt.go`, `internal/messagequeue/handler.go`
- Modify: `internal/aggregate/commandhandler.go`, `commandscope.go`,
  `internal/integration/commandhandler.go`, `commandscope.go`,
  `internal/process/deadlinehandler.go`, `messagescope.go`
- Modify: `engine.go` (`runHandler` builds routes)
- Test: `internal/messagequeue/consumer_test.go`, and the existing handler
  suites

**Interfaces:**
- Produces:

  ```go
  // Route is the handler for some message types, with the loggers that
  // identify it.
  type Route struct {
      Handler      Handler
      EngineLogger *slog.Logger
      AppLogger    *slog.Logger
  }

  // Handler.Deliver gains an application logger:
  Deliver(
      ctx context.Context,
      tx *sql.Tx,
      m Message,
      env *envelopepb.Envelope,
      engineLogger *slog.Logger,
      appLogger *slog.Logger,
  ) error
  ```

  `Consumer.Handlers` becomes `Routes uuidcollection.Map[Route]`. For each
  attempt, the consumer builds `engineLogger` from the route's with the
  `attempt` group and `handled_message`, and `appLogger, flush :=
  xslog.Buffered(route.AppLogger.With(handled_message))`. It calls `flush`
  after a successful commit, and never otherwise. The aggregate and
  integration handlers drop their `AppLogger` fields. Their scopes gain an
  `engineLogger` and write the engine DEBUG records the process scopes
  write: "handler logged a message", "handler recorded an event", with the
  instance attribute for aggregates. The deadline scope stops writing its
  application records at once; it shares `messageScope` with the event
  scope, which already buffers.

- Consumes: `xslog.Buffered`.

- [ ] **Step 1: Write the failing tests**

  Buffering isn't tested through logs. The test is structural: in
  `consumer_test.go`, "it delivers through the route's handler" replaces the
  stub `Handler` registration with a `Route`. The handler suites compile
  against the new signature.

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement it**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Update the logging plan**

  Remove the first and third items of its "After #100" section, which this
  task finishes.

- [ ] **Step 6: Stop for review**

---

### Task 2: Shared process preparation

**Files:**
- Create: `internal/process/effects.go`
- Modify: `internal/process/sink.go` (`dispatch`),
  `internal/process/deadlinehandler.go` (`Deliver`),
  `internal/process/instancestate.go` (`saveInstance` splits into marshal
  and write)
- Test: `internal/process/deadlinerouting_test.go` or a new
  `internal/process/effects_test.go`, through the engine API

**Interfaces:**
- Produces, unexported in `process`:

  ```go
  // effects is everything a handler's call changed, ready to write.
  type effects struct {
      instanceID string
      ended      bool
      state      []byte                    // nil if not mutated or ended
      commands   *envelopepb.MultiEnvelope // nil if none
      deadlines  *envelopepb.MultiEnvelope // nil if none or ended
  }

  // prepare runs fn, which calls the handler with the given scope, and
  // returns its effects.
  //
  // It returns an error, and no effects, if fn fails or the state can't be
  // marshaled.
  func prepare(ctx, scope *messageScope, fn func() error) (effects, error)

  // write persists the effects within tx.
  func write(ctx, tx, handlerKey, binding *uuidpb.UUID, e effects) error
  ```

  `write` adds the commands to the queues the router chooses, ends the instance
  or saves the state, and adds the deadlines to the instance's deadline queue.
  `dispatch` and `Deliver` each become: load, prepare, write. A preparation
  error from the handler or the marshal is returned wrapped with context, as
  `messagequeue.Fail(err)` from the deadline handler and as a plain error from
  the sink, where the stream consumer's streak handles it as today.

- Consumes: `messagequeue.Fail` from task 3, or `messagequeue.ErrFailed` if task
  3 hasn't landed; do task 3 first if that's awkward.

- [ ] **Step 1: Write the failing tests**

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

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement `prepare` and `write`, and use them in both paths**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Update `CHANGELOG.md`**

  Under `### Fixed`: a deadline whose handler's state couldn't be
  serialized no longer commits the commands it produced.

- [ ] **Step 6: Stop for review**

---

### Task 3: Per-delivery streaks

**Files:**
- Create: `internal/supervisor/streak.go`, `internal/messagequeue/fail.go`
- Modify: `internal/messagequeue/attempt.go`, `internal/messagequeue/handler.go`
- Modify: `internal/aggregate/commandhandler.go`,
  `internal/integration/commandhandler.go`,
  `internal/process/deadlinehandler.go` (return errors with context; stop
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

  The attempt builds a `Streak` from `Message.Failures` and
  `Message.FailingSince` with the description "deliver message", calls
  `Failed` or `FailedDeterministically` on `ErrFailed` and deterministic
  errors, stores `failures + 1` and `since` with the delay, and calls
  `Succeeded` after a successful commit. `ErrBusy` neither fails nor
  succeeds the streak. Contention keeps `failing_since` as it is.

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
  [#101][101], [#102][102], [#58][58] and [#91][91] with what landed and the
  follow-ups from plan 1.

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
