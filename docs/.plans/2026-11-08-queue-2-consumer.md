# Queue Plan 2: The Queue Consumer

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Before each task, confirm its
> assumptions with the maintainer, as "Reviewability" describes. Don't stage
> or commit code. The maintainer stages and commits each task as they review
> it. Build and test exclusively through `make` (`make test`,
> `make precommit`), never by invoking the Go toolchain directly. Tests start
> PostgreSQL through testcontainers, so Docker must be running.

**Goal:** Replace the per-handler message pumps with one queue consumer per
node that delivers the commands and deadlines in the queues
this node is responsible for.

**Architecture:** `messagequeue.Consumer` watches the live nodes, the observed
queues and the signal of each queue it's responsible for. It keeps the set of
queues the local node is responsible for, finds the earliest deliverable message
across them with one indexed query, waits for one of its attempt slots, and runs
each attempt in its own goroutine and transaction. An attempt claims its message
with `FOR UPDATE SKIP LOCKED`, falling back to any other due message if the hint
is gone, and hands it to the `messagequeue.Handler` for its message type.
Ownership loss stops new attempts; shutdown cancels them. The `messagepump`
package is removed.

**Tech stack:** Go, PostgreSQL, `cosyne`, `errgroup`, `testing/synctest` for
the timer logic.

**Spec:** the "Self-affinity", "Scheduling, capacity, and lifecycle" and
"Delivery correctness and contention" sections of [#100][100], plus the
decisions below.

This is the second of three plans for [#100][100]. It depends on
[plan 1][plan-1] for the `messagequeue.messages` table, its signal,
`messagequeue.Catalog` and `cluster.FindResponsibleNode`.

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

- **One consumer per node, not per queue or per handler.** It discovers across
  every queue it is responsible for. Active-attempt tracking is per message and
  bounded by the consumer's own concurrency limit, `10 × GOMAXPROCS`, which
  it holds as a semaphore. Nothing is shared with other nodes.
- **The consumer lands in two tasks.** Task 3 is ownership, discovery,
  timers and wake-ups, dispatching to an attempt function the test supplies.
  Task 4 is the real attempt: the concurrency limit, the claim, the handler
  call and postponement. Each is reviewable alone.
- **Discovery is one query** that takes the owned queues, the routed message
  types and the active message IDs, and returns the candidate with the earliest
  `deliver_at`, including a future one. Ties are broken arbitrarily. It looks up
  each `(queue, type)` pair in the index from plan 1, so a retired route's rows
  are never read.
- **The fallback claim is one query**, not the probe-and-savepoint sequence
  from [#102][102]: the same shape as discovery with `deliver_at <= now` and
  `FOR UPDATE SKIP LOCKED`, limited to one row. It locks only the row it
  returns. Measuring its plan under load is listed in the validation.
- **Handlers keep their own lock behavior for now.** The aggregate handler
  keeps `SKIP LOCKED` on its instance and reports `ErrBusy`. The deadline
  handler gains the savepoint and `lock_timeout` sequence from [#100][100] in
  task 6. The deliveries plan may unify them.
- **An attempt's context derives from the consumer's**, not from ownership,
  so losing a queue lets in-flight attempts finish and shutdown cancels them.
- **The consumer's own failures use `supervisor.Work`** with a quiet period,
  under the description "consume queue". A failed discovery or claim is a
  failure of the consumer; a failed delivery is the handler's, reported
  through `ErrFailed`. In-flight attempts aren't stopped by the consumer's
  backoff.
- **Integration concurrency** is a semaphore of one inside the integration
  handler when it minimizes concurrency, since there is no longer one worker
  per handler.
- **"Now" for deadlines is Go's clock**, passed to discovery and the claim,
  for the reason in today's `DeadlinePump.AcquireDelivery` comment.

## Global constraints

- Tests never assert on log records. Logging isn't tested.
- Tests are in `package foo_test`, and test unexported types through the public
  API.
- Struct literals have one named field per line, in definition order.
- Method receivers are one letter.
- Exported methods come first in a file, and unexported helpers after them.
- Internal packages take required fields. The engine passes constants. Add
  only the two `EngineOption`s this plan names.
- Don't add constructors for unexported types. Build the literal where it's
  used.
- Comments describe the current code. They never mention past designs, and
  never restate what the code shows.
- Engine log messages are constant strings. Values go in attributes. Follow
  the [logging guide].
- Fields that hold a logger are named `EngineLogger` and `AppLogger`, or
  `engineLogger` and `appLogger` if unexported. Never use `Logger` or `logger`.
- At most one `&&` or `||` per expression.
- Use a set or map when order means nothing.
- Bare `Lock`/`Unlock` only around one or two straight statements; otherwise
  extract a function with `defer`.

## Review focus

These conditions are easy to get wrong, and no existing test exercises them.
Each one has a test in the task that owns the code.

1. A completion must remove its message from the active set before releasing
   its slot, or the next discovery can re-dispatch it. Task 4 has the test,
   which runs with one slot.
2. A wake-up that arrives while the consumer is blocked waiting for a slot
   must not be lost. Task 4 has the test.
3. A message that moves to a queue owned by another node while this node
   holds a future timer for it must not be delivered here. Task 3 has the
   test.
4. A deadline claimed before its instance lock times out must be postponed
   without a failure, and the transaction's `lock_timeout` restored on both
   paths. Task 6 has the tests.
5. Shutdown with attempts in flight must roll them back and return only when
   every slot is released. Task 4 has the test.

---

### Task 1: `Node.AttachSignal` and `cluster.Observable`

`signal.Bus` drops a signal when a watcher's channel is full, and delivers a
subtopic that only heartbeats read. The consumer in task 3 has to wake for any
of a changing set of owned queues, which doesn't fit one channel per watch. This
task lets `cluster.Node` attach cluster-wide signals to `cosyne.Signal`s, called
targets, adds `cluster.Observable`, a value that every node reads from the
database, and moves every watcher except heartbeats onto them. `signal.Bus`
stays until heartbeats move, in follow-up 1 of [plan 1][plan-1].

**Files:**
- Modify: `internal/cluster/node.go` (own the LISTEN connection), the
  eventstream and messagequeue triggers
- Create: `internal/cluster/signal.go`, `internal/cluster/observable.go`
- Modify: `internal/eventstream/source/local.go`,
  `internal/messagequeue/catalog.go`, `engine.go`
- Test: `internal/cluster/signal_test.go`,
  `internal/cluster/observable_test.go`, and the existing suites of the
  callers that move

**Interfaces:**
- Produces:

  ```go
  // AttachSignal attaches the given cluster-wide signal to the given target,
  // so that the target is notified each time the cluster-wide signal is,
  // until the returned function is called or ctx is canceled. It notifies the
  // target straight away.
  func (n *Node) AttachSignal(ctx context.Context, signal string, target cosyne.SignalW) (detach func())

  // Observable is a value stored in the database that every node keeps a
  // copy of, and reads again each time a particular cluster-wide signal is
  // notified. It's published only when it changes.
  type Observable[T interface{ IsEqual(T) bool }] struct {
      Node   *Node
      Signal string
      Read   func(ctx context.Context) (T, error)
      // unexported: cosyne.Observable[T]
  }

  func (o *Observable[T]) Load() (T, <-chan struct{})
  func (o *Observable[T]) Run(ctx context.Context) error
  ```

  A cluster-wide signal is a string, such as `messagequeue`, and is notified by
  a PostgreSQL notification with that string as its payload. There are no topics
  or hierarchy, and matching is exact, so a node looks up the targets for each
  notification in a map. Each call attaches one cluster-wide signal.

  `cluster.Node` holds the one LISTEN connection on each node, which every
  `AttachSignal` call shares. Notifications that arrive while one is pending
  coalesce, as they do in `cosyne.Signal`, so none is ever dropped.

  `AttachSignal` is exported, and the caller passes in the target rather than
  getting one back. The caller owns it, so it outlives any one call, and many
  cluster-wide signals can be attached to one target. The consumer in task 3
  keeps one wake-up signal for its whole life, and attaches the signal of each
  queue it's responsible for to it.

  The triggers send exact signals. `eventstream.signal_create` sends
  `eventstream.create`, without the stream ID, since discovery reloads every
  stream. The messagequeue triggers send `messagequeue.<queue-id>` for each
  queue they touch, and `messagequeue` once for the catalog.

  `Observable` has the same `Load` method as `cosyne.ObservableR`, rather than
  implementing it. That interface is sealed, so nothing outside `cosyne` can
  implement it. `xtesting.WaitForObservable` accepts any type with that
  method. When `messagequeue.Catalog` moves, `cluster` gains its own
  `ObservableR[T]` interface with just `Load`, for `Catalog.QueueIDs()` to
  return.

  There is no sending `Signal` type yet. Apart from heartbeats, every signal
  comes from a database trigger, so nothing in Go sends one, and the callers
  that move here only wait for them. A way to send one comes with follow-up 1
  of [plan 1][plan-1], when heartbeats move off `signal.Bus`.

- [ ] **Step 1: Agree the interfaces.** Agreed: `AttachSignal` as above,
  `Observable` takes the node and signal directly, and it compares values
  with their own `IsEqual` method rather than a separate function or `==`.
  The values it holds are `*uuidcollection.Set`, where `==` compares
  pointers, and `(*uuidcollection.Set).IsEqual` treats nil as empty.
- [ ] **Step 2: Write the failing tests** for `Node.AttachSignal` and
  `Observable`: a cluster-wide signal notifies the target it is attached to on
  every node, other signals don't, notifications coalesce, the target is
  notified straight away, detaching ends delivery, and an observable
  reloads on each notification and publishes only changes.
- [ ] **Step 3: Implement them**
- [ ] **Step 4: Move stream discovery, event delivery and
  `messagequeue.Catalog`**, one commit each. The catalog's `QueueIDs()`
  becomes a `cluster.Observable[*uuidcollection.Set]`.
- [ ] **Step 5: Run `make test`**
- [ ] **Step 6: Stop for review** after each commit.

---

### Task 2: Queue timing options

**Files:**
- Modify: `engineoption.go`, `engine.go`
- Test: `engineoption_test.go`

**Interfaces:**
- Produces:

  ```go
  // DefaultQueueLockTimeout is the default maximum time a delivery waits for
  // the lock that serializes deliveries to one instance.
  DefaultQueueLockTimeout = 200 * time.Millisecond

  // DefaultQueueContentionDelay is the default time a delivery is postponed
  // after it is unable to acquire that lock.
  DefaultQueueContentionDelay = 200 * time.Millisecond

  func WithQueueLockTimeout(timeout time.Duration) EngineOption
  func WithQueueContentionDelay(delay time.Duration) EngineOption
  ```

  Both panic on a non-positive duration, like `WithHandleTimeout`. The engine
  stores them in `queueLockTimeout` and `queueContentionDelay`.

- [ ] **Step 1: Write the failing tests**, following the existing option
  tests: each option is stored, and each panics on zero.
- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement the options**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Update `CHANGELOG.md`** under `### Added`.
- [ ] **Step 6: Stop for review**

---

### Task 3: `messagequeue.Consumer`: ownership and discovery

**Files:**
- Create: `internal/messagequeue/consumer.go`,
  `internal/messagequeue/discovery.go`
- Test: `internal/messagequeue/consumer_test.go`

**Interfaces:**
- Produces:

  ```go
  // Consumer delivers the queued messages that this node is responsible for.
  type Consumer struct {
      DB           *sql.DB
      Node         *cluster.Node
      Queues       *Catalog
      Signals      *signal.Bus
      EngineLogger *slog.Logger

      // Attempt delivers the message with the given ID. It is called in its
      // own goroutine once the message is due, and the message is not
      // offered again until it returns.
      Attempt func(ctx context.Context, messageID *uuidpb.UUID)
      // unexported: owned queues, active message IDs, a wake channel, the
      // timer, the attempt group and a supervisor.Work
  }

  // Run delivers messages until ctx is canceled. It must be called at most
  // once.
  func (c *Consumer) Run(ctx context.Context) error
  ```

  `Attempt` is a field in this task so the loop can be tested with a stub.
  Task 4 makes it a method and removes the field.

  Discovery, in `discovery.go`, is an unexported function that runs:

  ```sql
  SELECT m.message_id, m.deliver_at
  FROM unnest($1::uuid[]) AS k(queue_id)
  CROSS JOIN unnest($2::uuid[]) AS t(message_type_id)
  CROSS JOIN LATERAL (
      SELECT q.message_id, q.deliver_at
      FROM messagequeue.messages AS q
      WHERE q.queue_id = k.queue_id
        AND q.message_type_id = t.message_type_id
        AND q.message_id <> ALL($3::uuid[])
      ORDER BY q.deliver_at
      LIMIT 1
  ) AS m
  ORDER BY m.deliver_at
  LIMIT 1
  ```

  The routed types are a `MessageTypeIDs *uuidcollection.Set` field in this
  task, which task 4 derives from `Handlers`.

  The loop uses `Node.AttachSignal` to attach the signal of each owned queue,
  `messagequeue.<queue-id>`, to its wake-up signal before discovering, so no
  signal falls between them. It keeps each attachment's detach function, so when
  the owned queues change it attaches only to newly owned queues and detaches
  only from those it no longer owns. It then computes the owned queues from
  `Node.LiveNodes()` and `Catalog.QueueIDs()` and discovers. If the candidate is
  due, it adds its ID to the active set and starts `Attempt`; if it's in the
  future, it sets the timer. Then it waits on the first of the live nodes, the
  queues, its wake-up signal, the timer, a completion, or ctx. A completion
  removes the ID from the active set and wakes the loop. The consumer's own
  failures go through `supervisor.Work` with the description "consume queue".

- Consumes: `Node.IsResponsibleFor`, `Catalog.QueueIDs`,
  `Node.AttachSignal` from task 1.

- [ ] **Step 1: Write the failing tests**

  Use rows inserted by SQL into chosen queues, and an `Attempt` stub that
  records its calls and can block. Tests that need a particular set of live
  nodes run one or two real `cluster.Node`s on the test database.

  ```go
  func TestConsumer(t *testing.T) {
      t.Run("it attempts a due message in a queue it is responsible for", ...)
      t.Run("it ignores a message in a queue another node is responsible for", ...)
      t.Run("it ignores a message of a type it has no handler for", ...)
      t.Run("it attempts a future message when it becomes due", ...)
      t.Run("it attempts an earlier message queued while it waits for a later one", ...)
      t.Run("it does not attempt a message that moves to another node's queue while it waits", ...)
      t.Run("it attempts messages in order of their delivery time", ...)     // distinct deliver_at values
      t.Run("it moves on when the message it waits for is removed", ...)   // no signal; the timer fires and discovery finds the next
      t.Run("it does not attempt a message that is already being attempted", ...)
      t.Run("it attempts a message again once its attempt returns", ...)       // the stub leaves the row in place
      t.Run("it keeps running when discovery fails", ...)                     // drop the table mid-run; recreate; attempts resume
      t.Run("it waits for attempts in flight when it stops", ...)
  }
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement the loop and discovery**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Stop for review**

---

### Task 4: `messagequeue.Consumer`: the attempt

**Files:**
- Create: `internal/messagequeue/message.go`,
  `internal/messagequeue/handler.go`, `internal/messagequeue/attempt.go`
- Modify: `internal/messagequeue/consumer.go`
- Test: `internal/messagequeue/consumer_test.go`

**Interfaces:**
- Produces:

  ```go
  // Message is a queued message as an attempt sees it.
  type Message struct {
      ID            *uuidpb.UUID
      TypeID        *uuidpb.UUID
      QueueID       *uuidpb.UUID
      EnvelopeBytes []byte
      Failures      uint64
      FailingSince  time.Time   // zero if Failures is zero
      ScheduledAt   time.Time
  }

  // Handler delivers the messages of some message types.
  type Handler interface {
      // Deliver delivers the message within tx.
      //
      // It returns ErrFailed if the delivery failed and is to be postponed
      // with its failure count incremented, ErrBusy if its target is locked
      // and it is to be postponed without a failure, and any other error if
      // the attempt can't continue, in which case tx is rolled back and the
      // message is left for another attempt.
      Deliver(
          ctx context.Context,
          tx *sql.Tx,
          m Message,
          env *envelopepb.Envelope,
          engineLogger *slog.Logger,
      ) error
  }

  var (
      ErrFailed = errors.New("delivery failed")
      ErrBusy   = errors.New("delivery target is busy")
  )
  ```

  `Consumer` gains `Concurrency int`, `Handlers uuidcollection.Map[Handler]`
  (by message type ID, replacing `MessageTypeIDs`) and `ContentionDelay
  time.Duration`, and loses the `Attempt` field. A slot is acquired before an
  attempt starts, as part of the loop's select, from a channel with
  `Concurrency` slots. A completion removes the ID from the active set
  before it releases the slot.

  The attempt, in `attempt.go`, begins a transaction and claims the hint by ID
  if it is still owned, routed, unlocked and due. If not, it falls back to the
  discovery query with `deliver_at <= $4`, the inner `LIMIT 1` removed, and `FOR
  UPDATE OF q SKIP LOCKED` on the outer query with `LIMIT 1`, and moves its
  active ID to whatever it claimed. If that returns nothing, every due row is
  locked elsewhere: the attempt commits nothing, releases its slot, and the loop
  waits for `ContentionDelay` or a wake-up before it discovers again. The claim
  loads the columns of `Message`. It unmarshals and validates the envelope,
  calls the handler for the type, and then removes the row with
  `messagequeue.remove`, or postpones it. Postponement sets `deliver_at`, and
  for `ErrFailed` increments `failures` and sets `failing_since` if null, using
  a backoff of `supervisor.Jitter` over `min(baseDelay << failures, 5m)`; for
  `ErrBusy` it uses `ContentionDelay`. The deliveries plan replaces the backoff
  with a streak.

  Each attempt logs at DEBUG "message delivery started", "message
  delivered" and "message delivery postponed" (with `delay` and a `reason`
  of `failure` or `contention`), with `handled_message` and an `attempt`
  attribute. An envelope that can't be unmarshaled is postponed as an
  `ErrFailed`, at DEBUG for now; the deliveries plan's streak gives it its
  ERROR.

- Consumes: task 3's loop.

- [ ] **Step 1: Write the failing tests**

  Replace the `Attempt` stub with a stub `Handler`, and add:

  ```go
  t.Run("it delivers a due message", ...)
  t.Run("it removes a delivered message", ...)
  t.Run("it delivers no more messages at once than its concurrency limit", ...)
  t.Run("it keeps a signal that arrives while it waits for a slot", ...)
  t.Run("it claims another due message when the hint is locked", ...)      // lock the hint in a test transaction
  t.Run("it waits for the contention delay when every due message is locked", ...)
  t.Run("it postpones a failed delivery and increments its failures", ...)
  t.Run("it postpones a busy delivery without a failure", ...)
  t.Run("it postpones a message whose envelope can not be unmarshaled", ...)
  t.Run("it leaves a message for another attempt when delivery can not continue", ...)
  t.Run("it lets an attempt finish after losing its queue", ...)              // stop the other node; the live nodes change mid-delivery
  t.Run("it rolls back attempts in flight when it stops", ...)
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement the attempt**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Stop for review**

---

### Task 5: Handlers, wiring and removal of `messagepump`

**Files:**
- Rename: `internal/aggregate/commandpump.go` → `commandhandler.go`,
  `internal/integration/commandpump.go` → `commandhandler.go`,
  `internal/process/deadlinepump.go` → `deadlinehandler.go`
- Delete: `internal/messagepump/`
- Modify: `engine.go` (`runHandler`, `workerCountForHandler`, the pump
  constants), `internal/x/xtesting/commandqueue.go`
- Test: the existing suites in `internal/aggregate`, `internal/integration`,
  `internal/process` and the root package

**Interfaces:**
- Produces: `aggregate.CommandHandler`, `integration.CommandHandler` and
  `process.DeadlineHandler`, each implementing `messagequeue.Handler`. Their
  `AcquireDelivery` and `PostponeDelivery` methods go. `HandleDelivery` becomes
  `Deliver` with the `messagequeue.Message` parameter. `messagepump.ErrFailed`
  and `ErrBusy` references become `messagequeue.ErrFailed` and
  `messagequeue.ErrBusy`. `integration.CommandHandler` gains an unexported
  semaphore of one when `Concurrency` is `MinimizeConcurrency`, acquired for the
  handler call.

  In `engine.go`, `Run` builds one `messagequeue.Consumer` whose `Handlers` map
  every command type to its handler and every deadline type to its process
  handler, with `Concurrency` from `workerCountForHandler`'s formula, and
  runs it in the group. `runHandler` no longer starts pumps.
  `workerCountForHandler` and the pump constants go.

- Consumes: `messagequeue.Consumer` and `messagequeue.Handler` from task 4.

- [ ] **Step 1: Make the handlers implement `messagequeue.Handler`**
- [ ] **Step 2: Wire the consumer and delete `messagepump`**

  Add a sentence to the doc comment of each trigger in
  `internal/schema/ddl/10-messagequeue/20-triggers.sql` saying that
  `messagequeue.Consumer` watches the signal for the queues this node is
  responsible for. Widen the package doc in `internal/messagequeue/doc.go` to
  say that the package also delivers the messages. - [ ] **Step 3: Run `make
  test`**

  The existing engine-level suites cover delivery of commands and deadlines.
  Adjust any test that relied on the pump's poll interval or worker count;
  there should be none that assert on them.

- [ ] **Step 4: Update `CHANGELOG.md`**

  Under `### Changed`: commands and deadlines are now delivered by the node
  responsible for their queue, with one per-node limit of
  10 × `GOMAXPROCS` attempts at once in place of a limit per handler.

- [ ] **Step 5: Stop for review**

---

### Task 6: Claim-first deadline delivery

**Files:**
- Modify: `internal/process/deadlinehandler.go`,
  `internal/process/instancestate.go` (`endInstance`)
- Test: `internal/process/deadlinecontention_test.go` (new),
  `internal/process/instancestate_test.go`

**Interfaces:**
- Produces: `process.DeadlineHandler` gains `LockTimeout time.Duration`. Its
  `Deliver`:
  1. Opens a savepoint, reads `current_setting('lock_timeout')`, runs
     `SET LOCAL lock_timeout = $1`, and locks the instance row with
     `SELECT … FOR UPDATE`.
  2. On success, restores the setting and releases the savepoint. If the
     instance has ended, deletes the claimed row with `messagequeue.remove` and
     returns nil, without unmarshaling the message or creating a root.
  3. On a lock timeout (`SQLSTATE 55P03`), rolls back to the savepoint, which
     also restores the setting, and returns `messagequeue.ErrBusy`.

  `endInstance` deletes the instance's pending deadlines with `FOR UPDATE
  SKIP LOCKED` in a subquery, so it never waits for a row an attempt has
  claimed. The attempt that holds it observes `ended` under the instance lock
  and removes it.

- Consumes: `messagequeue.ErrBusy`, and `ContentionDelay` from task 4, which the
  consumer applies.

- [ ] **Step 1: Write the failing tests**

  ```go
  func TestDeadlineHandler_contention(t *testing.T) {
      t.Run("it postpones a deadline without a failure when its instance is locked", ...)
      t.Run("it restores the lock timeout after acquiring the lock", ...)      // current_setting in the same tx after Deliver
      t.Run("it restores the lock timeout after a timeout", ...)
      t.Run("it discards a claimed deadline whose instance has ended", ...)   // claim in tx A; end the instance in tx B; A proceeds
  }
  ```

  In `instancestate_test.go`:

  ```go
  t.Run("it ends an instance while one of its deadlines is claimed", ...)   // ending doesn't block on the claimed row
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement it**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Stop for review**

---

## Validation to run before the deliveries plan

Not tests. Run by hand on a database with representative data, and record
the results in [#100][100]:

- `EXPLAIN (ANALYZE, BUFFERS)` for the discovery and fallback queries with
  tens of owned queues, a handful of routed types, a large queue, and a long
  active-ID list.
- Throughput and connection-pool waits with the concurrency limit
  saturated.

## Out of scope

- Buffered application records, per-delivery streaks and `failing_since`
  from a streak. The deliveries plan does those.
- Shared process preparation and the [#99][99] fix. Same.
- `WithQueueConcurrencyLimit`, capacity scores and capacity sharing. They
  are follow-ups listed in [plan 1][plan-1].
- Instance placement.

<!-- references -->

[99]: https://github.com/dogmatiq/runkit/issues/99
[100]: https://github.com/dogmatiq/runkit/issues/100
[102]: https://github.com/dogmatiq/runkit/pull/102
[logging guide]: ../maintainers/logging.md
[plan-1]: 2026-11-08-queue-1-storage.md
