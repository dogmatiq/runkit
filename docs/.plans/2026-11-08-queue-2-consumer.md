# Queue Plan 2: The Queue Consumer

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Before each task, confirm its
> assumptions with the maintainer, as "Reviewability" describes. Don't stage
> or commit code. The maintainer stages and commits each task as they review
> it. Build and test exclusively through `make` (`make test`,
> `make precommit`), never by invoking the Go toolchain directly. Tests start
> PostgreSQL through testcontainers, so Docker must be running.

**Goal:** Replace the per-handler message pumps with one queue consumer per node
that delivers the commands and deadlines in each queue while this node owns the
queue's workload.

**Architecture:** `messagequeue.Consumer` watches the live nodes, the observed
queues and the signal of each queue while it owns the queue's workload. It keeps
the set of those queues, finds the earliest deliverable message across them with
one indexed query, and runs each attempt in its own goroutine and transaction,
up to its concurrency limit. The consumer claims each message for an attempt
with `FOR UPDATE NOWAIT`, then passes it to the `messagequeue.Sink` for its
message type. If another transaction holds the message's lock, the consumer
doesn't attempt it again until a contention delay has passed. When a queue's
workload moves to another node, this node doesn't start any new attempts on its
messages; shutdown cancels them. The `messagepump` package is removed.

**Tech stack:** Go, PostgreSQL, `cosyne`, `errgroup`, `testing/synctest` for
the timer logic.

**Spec:** the "Self-affinity", "Scheduling, capacity, and lifecycle" and
"Delivery correctness and contention" sections of [#100][100], plus the
decisions below.

This is the second of three plans for [#100][100]. It depends on
[plan 1][plan-1] for the `messagequeue.messages` table, its signal,
`messagequeue.Catalog` and `cluster.FindResponsibleNode`, which task 4 renames
to `cluster.FindWorkloadOwner`.

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

- **One consumer per node, not per queue or per handler.** It looks for the next
  messages across all of the queues while it owns their workloads. Only the loop
  tracks the attempts in progress, as a set of message IDs, and it starts no
  more than its concurrency limit, `10 × GOMAXPROCS`. Each attempt is sent on a
  channel after it has been made. Nothing is shared with other nodes.
- **The consumer lands in two tasks.** Task 3 is ownership, finding the next
  messages, timers and wake-ups, dispatching to an attempt function the test
  supplies. Task 4 is the real attempt: the concurrency limit, the claim, the
  handler call and postponement. Each is reviewable alone.
- **Finding the next messages is one query** that takes the queues it consumes,
  the message types that have handlers and the IDs of the messages being
  attempted, and returns the message with the earliest `next_attempt_at`,
  including a future one. Ties are broken arbitrarily. It looks up each
  `(queue, type)` pair in the index from plan 1, so a retired route's rows are
  never read.
- **A delivery is made of attempts.** A delivery is the whole job of getting a
  message to its handler. It's complete once the message is delivered, or
  canceled if it's a deadline whose process instance has ended. An attempt is
  one transaction that tries to complete it, so a delivery may take many
  attempts. An attempt that fails postpones the delivery. The loop records each
  attempt once it has been made, along with whether it delivered the message.
  Names say "delivery" where `messagequeue` wouldn't say what's attempted:
  `deliveryAttempt` and `Consumer.attemptDelivery`.
  `Consumer.findNextMessages` returns each `message` without its envelope, and a
  claim returns its `Delivery`, which sinks receive. [#100][100] calls
  finding the next messages "discovery".
- **The consumer claims only the message that an attempt was started for.**
  [#100][100] asks for a fallback to other work that is due, because its
  discovery returned one hint per handler, so one locked message held up the
  whole handler. Here, a locked message isn't one of the next messages for
  `ContentionDelay`, and it holds up no slot in the meantime. Only an attempt on
  another node holds a message's lock for long. That happens when the workload
  of a queue moves to another node while an attempt is in progress, which may be
  often, since an integration may take as long as the handle timeout and
  membership can change several times in that period.
- **No sink waits for a lock.** A sink that finds its instance, or its
  handler, busy returns `ErrBusy` at once, and the consumer postpones the
  delivery by the contention delay. The aggregate sink locks its instance with
  `SKIP LOCKED`, and the deadline sink does the same in task 6. `NOWAIT` would
  abort the transaction, which the consumer needs to postpone the delivery. The
  bounded `lock_timeout` wait from [#100][100] isn't needed to prevent
  deadlocks, only to reduce latency, so it's left to the deliveries plan, as one
  setting for every kind of message if measurements show it's worth having.
- **An attempt's context derives from the consumer's**, not from ownership of
  its queue's workload, so when the workload moves, in-flight attempts finish,
  and shutdown cancels them.
- **Nodes own workloads.** A workload is work that only one node should do at a
  time, such as consuming an event stream or delivering the messages in a queue,
  and its key is what rendezvous hashing uses. Task 4 renames
  `cluster.FindResponsibleNode` and `Node.IsResponsibleFor` to
  `cluster.FindWorkloadOwner` and `Node.OwnsWorkload`, because ownership has
  words for handoffs, such as "the workload moves" and "the previous owner". A
  node owns a workload, never the key, the stream or the queue itself.
- **The consumer's own failures use `supervisor.Work`** with a quiet period,
  under the description "consume queue". A failure to find the next messages, or
  to claim one, is a failure of the consumer; a failed attempt is the sink's,
  reported through `ErrFailed`. The consumer's backoff stops it from starting
  attempts, but doesn't stop the attempts in flight.
- **A sink is where the consumer passes each message**, as `eventstream.Sink` is
  for the event stream consumer. Task 5 renames `messagequeue.Handler` to
  `messagequeue.Sink`, and the types that implement it are
  `aggregate.CommandSink`, `integration.CommandSink` and `process.DeadlineSink`.
  In Dogma, a handler is the application's message handler, so the engine's
  types don't use the word.
- **The consumer removes a delivered message.** When a sink returns nil, the
  consumer deletes the message within the attempt's transaction. It doesn't
  fail if that transaction already removed the message, which happens when a
  deadline ends its process instance. Sinks, and the SQL functions they call,
  don't remove the message, and `messagequeue.remove` is dropped.
- **One node delivers the commands of an integration that minimizes
  concurrency.** `messagequeue.Router` adds each of its commands to the queue
  whose ID is the integration's key. The integration sink holds an
  engine-supplied semaphore during each call to the handler, taken with
  `TryAcquire`, and returns `ErrBusy` if another call holds it. While nodes see
  different live nodes, two of them may briefly both deliver the commands,
  which Dogma allows, because a concurrency preference is a hint. Waiting for
  the semaphore, or for today's cluster-wide advisory lock, would hold a slot
  and a connection for up to the handle timeout, so the advisory lock and
  `internal/concurrency` go.
- **The router keeps resolved handler values.** `messagequeue.CommandHandler`
  holds what `RouteCommandToQueue` reads from the configuration of each command
  type's handler, because enginekit's configuration accessors resolve and
  validate on every call. It replaces `AggregateRoute`, whose name also
  collided with Dogma's "route".
- **Each sink logs to its own `EngineLogger`**, which has the handler's
  identity, as the event sinks do, so `Sink.Deliver` takes no logger. The
  consumer's own records don't have the handler's identity until the
  deliveries plan.
- **"Now" for deadlines is Go's clock**, passed to the query for the next
  messages and to the claim, for the reason in today's
  `DeadlinePump.AcquireDelivery` comment.

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

1. The consumer must start no more attempts than its concurrency limit, and must
   start the next as soon as one has been made. Task 4 has the test, which runs
   with a limit of one.
2. A wake-up that arrives while the consumer is at its concurrency limit must
   not be lost. Task 4 has the test.
3. A message must not be delivered here if it moves to a queue while another
   node owns that queue's workload, and this node holds a future timer for the
   message. Task 3 has the test.
4. A deadline whose process instance is locked must be postponed without a
   failure, and ending an instance must not wait for a deadline that an attempt
   has claimed. Task 6 has the tests.
5. Shutdown with attempts in flight must roll them back and return only when
   every attempt in progress has been made. Task 3 tests the wait, and task 4
   the rollback.

---

### Task 1: `Node.AttachSignal`

`signal.Bus` drops a signal when a watcher's channel is full, and delivers a
subtopic that only heartbeats read. The consumer in task 3 has to wake for any
of a changing set of queues, which doesn't fit one channel per watch. This
task lets `cluster.Node` attach cluster-wide signals to `cosyne.Signal`s, called
targets, and moves every watcher except heartbeats onto them. `signal.Bus` stays
until heartbeats move, in follow-up 1 of [plan 1][plan-1].

**Files:**
- Modify: `internal/cluster/node.go` (own the LISTEN connection), the
  eventstream and messagequeue triggers
- Create: `internal/cluster/signal.go`
- Modify: `internal/eventstream/source/local.go`,
  `internal/messagequeue/catalog.go`, `engine.go`
- Test: `internal/cluster/signal_test.go`, and the existing suites of the
  callers that move

**Interfaces:**
- Produces:

  ```go
  // AttachSignal attaches the given cluster-wide signal to the given target,
  // so that the target is notified each time the cluster-wide signal is,
  // until the returned function is called or ctx is canceled.
  func (n *Node) AttachSignal(ctx context.Context, signal string, target cosyne.SignalW) (detach func())
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
  keeps one wake-up signal for its whole life. While it owns the workload of a
  queue, the queue's signal is attached to the wake-up signal.

  The triggers send exact signals. `eventstream.signal_create` sends
  `eventstream.create`, without the stream ID, since discovery reloads every
  stream. The messagequeue triggers send `messagequeue.<queue-id>` for each
  queue they touch, and `messagequeue` once for the catalog.

  There is no sending `Signal` type yet. Apart from heartbeats, every signal
  comes from a database trigger, so nothing in Go sends one, and the callers
  that move here only wait for them. A way to send one comes with follow-up 1
  of [plan 1][plan-1], when heartbeats move off `signal.Bus`.

- [x] **Step 1: Agree the interfaces.** Agreed: `AttachSignal` as above.
- [x] **Step 2: Write the failing tests** for `Node.AttachSignal`: a
  cluster-wide signal notifies the target it is attached to on every node,
  other signals don't, notifications coalesce, the target is not notified when
  it's attached, and detaching ends delivery.
- [x] **Step 3: Implement them**
- [x] **Step 4: Switch stream discovery, event delivery and
  `messagequeue.Catalog` from `signal.Bus` to `Node.AttachSignal`**, one
  commit for `source.Local` and one for the catalog. Each replaces its
  `Signals *signal.Bus` field with a `Node *cluster.Node`. Each keeps its own
  loop, which loads, then waits on its target. A shared `cluster.Observable`
  was dropped, because the catalog would have been its only user.
- [x] **Step 5: Run `make test`**
- [x] **Step 6: Stop for review** after each commit.

---

### Task 2: Queue timing options

Folded into the tasks that give each option an effect, so that neither lands
before anything reads it. `WithMessageQueueContentionDelay` and
`WithMessageQueueConcurrency` are added in task 5, which builds the consumer.
`WithMessageQueueLockTimeout` was dropped, because no sink waits for a lock
(see the decisions).

---

### Task 3: `messagequeue.Consumer`: ownership and the next messages

**Files:**
- Create: `internal/messagequeue/consumer.go`,
  `internal/messagequeue/message.go`
- Test: `internal/messagequeue/consumer_test.go`

**Interfaces:**
- Produces:

  ```go
  // Consumer delivers the messages in each queue while its node owns the
  // queue's workload.
  type Consumer struct {
      DB           *sql.DB
      Node         *cluster.Node
      Queues       *Catalog
      EngineLogger *slog.Logger

      // Attempt delivers the message with the given ID. It is called in its
      // own goroutine once the message is due, and the message is not
      // offered again until it returns.
      Attempt func(ctx context.Context, messageID *uuidpb.UUID)
      // unexported: a wake-up signal, a channel of attempts made, the
      // IDs of the messages being attempted, the queue signals' detach
      // functions, and a supervisor.Work
  }

  // Run delivers messages until ctx is canceled. It must be called at most
  // once.
  func (c *Consumer) Run(ctx context.Context) error
  ```

  `Attempt` is a field in this task so the loop can be tested with a stub.
  Task 4 makes it a method and removes the field.

  `nextMessages`, in `message.go`, runs:

  ```sql
  SELECT m.message_id, m.next_attempt_at
  FROM unnest($1::uuid[]) AS k(queue_id)
  CROSS JOIN unnest($2::uuid[]) AS t(message_type_id)
  CROSS JOIN LATERAL (
      SELECT q.message_id, q.next_attempt_at
      FROM messagequeue.messages AS q
      WHERE q.queue_id = k.queue_id
        AND q.message_type_id = t.message_type_id
        AND q.message_id <> ALL($3::uuid[])
      ORDER BY q.next_attempt_at
      LIMIT 1
  ) AS m
  ORDER BY m.next_attempt_at
  LIMIT 1
  ```

  The message types are a `MessageTypeIDs *uuidcollection.Set` field in this
  task. Task 4 replaces it with `Handlers`, and derives the unexported
  `messageTypeIDs` from it.

  The loop computes the queues it consumes from `Node.LiveNodes()` and
  `Catalog.QueueIDs()`. It uses `Node.AttachSignal` to attach the signal of each
  of them, `messagequeue.<queue-id>`, to its wake-up signal before looking for
  the next messages, so no signal falls between them. It keeps each attachment's
  detach function, so when the queues it consumes change it attaches only the
  new ones and detaches only those it no longer consumes. It starts `Attempt`
  for each message that is due, adding its ID to `attemptsInProgress`, until it
  finds a message that isn't due yet, or none. Then it waits on the first of the
  live nodes, the queues, its wake-up signal, the next message becoming due, an
  attempt being made, or ctx. An attempt sends its message ID on a channel after
  it has been made, and the loop removes it from `attemptsInProgress` and looks
  for the next messages again. Only the loop touches `attemptsInProgress`, so it
  needs no lock, and on shutdown the loop receives from the channel until the
  set is empty. A failure to find the next messages is retried after a delay,
  within `nextMessages` itself, so that the loop deals only with queues,
  messages and attempts. The retries go through `supervisor.Work` with the
  description "consume queue". While that's retried, attempts made wait in the
  channel's buffer, which has room for every attempt in progress, and signals
  coalesce.

- Consumes: `Node.IsResponsibleFor`, renamed to `Node.OwnsWorkload` in task 4,
  `Catalog.QueueIDs`, `Node.AttachSignal` from task 1.

- [x] **Step 1: Write the failing tests**

  Use rows inserted by SQL into chosen queues, and an `Attempt` stub that
  records its calls and can block. Tests that need a particular set of live
  nodes run one or two real `cluster.Node`s on the test database.

  ```go
  func TestConsumer(t *testing.T) {
      t.Run("it attempts a message that is due, if its node owns the queue's workload", ...)
      t.Run("it ignores a message if another node owns the queue's workload", ...)
      t.Run("it ignores a message of a type it has no handler for", ...)
      t.Run("it attempts a future message when it becomes due", ...)
      t.Run("it attempts an earlier message queued while it waits for a later one", ...)
      t.Run("it does not attempt a message that moves to another node's queue while it waits", ...)
      t.Run("it attempts messages in the order in which they're due", ...)     // distinct next_attempt_at values
      t.Run("it moves on when the message it waits for is removed", ...)   // no signal; the timer fires and the loop finds the next
      t.Run("it does not attempt a message that is already being attempted", ...)
      t.Run("it attempts a message again once its attempt has been made", ...)       // the stub leaves the row in place
      t.Run("it keeps running when it can't find the next messages", ...)                     // drop the table mid-run; recreate; attempts resume
      t.Run("it waits for attempts in flight when it stops", ...)
  }
  ```

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement the loop and `nextMessages`**
- [x] **Step 4: Run `make test` to verify they pass**
- [x] **Step 5: Stop for review**

---

### Task 4: `messagequeue.Consumer`: the attempt

**Files:**
- Create: `internal/messagequeue/handler.go`,
  `internal/messagequeue/delivery.go`
- Modify: `internal/messagequeue/consumer.go`,
  `internal/messagequeue/doc.go`, `internal/cluster/node.go`,
  `internal/cluster/rendezvous.go`, `internal/eventstream/consumer.go`
- Test: `internal/messagequeue/consumer_test.go`,
  `internal/cluster/node_test.go`, `internal/eventstream/consumer_test.go`,
  `internal/projection/distribution_test.go`

**Interfaces:**
- Produces:

  ```go
  // Delivery is the job of getting a queued message to its handler.
  //
  // A Delivery value describes the delivery at the time that the consumer
  // claims the message for an attempt.
  type Delivery struct {
      MessageID     *uuidpb.UUID
      MessageTypeID *uuidpb.UUID
      QueueID       *uuidpb.UUID
      Failures      uint64
      FailingSince  time.Time   // zero if Failures is zero
  }

  // Handler delivers the messages of some message types.
  type Handler interface {
      // Deliver delivers the message in the given envelope within tx.
      //
      // It returns ErrFailed if this attempt to deliver the message failed,
      // so that the delivery is postponed and its failure count is
      // incremented. It returns ErrBusy if it needs a row and another
      // transaction holds that row's lock, so that the delivery is postponed
      // without a failure. Any other error ends the attempt, so tx is rolled
      // back and the message is left for another attempt.
      Deliver(
          ctx context.Context,
          tx *sql.Tx,
          d Delivery,
          env *envelopepb.Envelope,
          engineLogger *slog.Logger,
      ) error
  }

  var (
      ErrFailed = errors.New("unable to deliver message")
      ErrBusy   = errors.New("delivery handler is busy")
  )
  ```

  `Consumer` gains `Concurrency int`, `Handlers uuidcollection.Map[Handler]` (by
  message type ID, replacing `MessageTypeIDs`) and
  `ContentionDelay time.Duration`, and loses the `Attempt` field. The loop finds
  the next messages and starts an attempt only while `attemptsInProgress` holds
  fewer than `Concurrency` IDs.

  Each attempt, once made, reports whether it delivered the message that it was
  started for. The loop looks for the next messages again if it didn't, because
  the message may still be due, or may have been postponed, or if the consumer
  was at its concurrency limit. After a successful delivery below the limit,
  there's nothing new to find.

  `findNextMessages` returns a batch rather than one message, so that a backlog
  doesn't take one query per message. It takes a limit, which is the headroom,
  `Concurrency` minus the size of `attemptsInProgress`, and returns up to that
  many messages in order of `next_attempt_at`: the inner and outer `LIMIT 1`
  become `LIMIT $5`. The query also leaves out the messages in
  `contendedMessages`, described below, with
  `AND q.message_id <> ALL($4::uuid[])`. `startAttempts` starts a delivery
  attempt for each message whose next attempt is due, and waits for the first
  one that isn't due yet. The loop looks again only if the whole batch was due.
  Before it looks, the loop records every attempt waiting in the channel, so
  that several attempts made during one query free headroom for one batch.
  Batching changes how many queries run, not which messages are attempted, so
  the tests can't observe it without asserting on queries. It ships without a
  test of its own, and the validation below measures it.

  Each attempt, in `delivery.go`, is one transaction. In it, the consumer claims
  the message by ID with `FOR UPDATE NOWAIT`, if it's due, the consumer has a
  handler for its type, and this node owned the workload of its queue when the
  attempt started. If another transaction holds the message's lock, the claim
  returns an error that `xsql.IsLockNotAvailableError` recognizes, and the
  attempt ends without committing anything. The loop records the message in
  `contendedMessages`, with the time at which it may be attempted again,
  `ContentionDelay` from now. `findNextMessages` leaves out the messages in
  `contendedMessages`, and the loop removes each one when its time passes,
  waking at the earliest of those times. If the message has been removed,
  postponed or moved since it was found, the attempt ends there, and the loop
  looks again. No test covers that case, as it needs the message to change
  between being found and being claimed. The claim loads the message's
  `Delivery` and the bytes of its envelope. The consumer then unmarshals and
  validates the envelope, calls the handler for the type, and removes the row
  with `messagequeue.remove`, or postpones it. Postponement sets
  `next_attempt_at`, and for `ErrFailed` increments `failures` and sets
  `failing_since` if null, using a backoff of `supervisor.Jitter` over
  `min(100ms << failures, 5m)`, where `failures` is the count before this one,
  from two constants in `delivery.go`; for `ErrBusy` it uses `ContentionDelay`.
  The deliveries plan replaces the backoff with a streak. Any other error ends
  the attempt. Its transaction is rolled back, and the loop records a failure of
  the consumer's `supervisor.Work` and doesn't start any attempts until the
  delay that `Work` returns has passed. Such errors are failures of the
  consumer, such as the database being unavailable, so the whole consumer backs
  off, rather than each attempt waiting in its own slot.

  Each attempt logs at DEBUG "message delivery attempt started", "message
  delivered" and "message delivery postponed" (with `delay` and a `reason` of
  `failure` or `contention`), with `message_id` and `failures` attributes. The
  handler's logger, and "message delivery attempt started", also have
  `handled_message`. An invalid envelope is postponed as an `ErrFailed`, at
  DEBUG for now; the deliveries plan's streak gives it its ERROR.

- Consumes: task 3's loop.

- [x] **Step 1: Write the failing tests**

  Replace the `Attempt` stub with a stub `Handler`, and add:

  ```go
  t.Run("it delivers a message that is due", ...)
  t.Run("it removes a delivered message", ...)
  t.Run("it attempts no more messages at once than its concurrency limit", ...)
  t.Run("it keeps a signal that arrives while it is at its concurrency limit", ...)
  t.Run("it uses all of the headroom that several attempts free at once", ...)
  t.Run("it attempts other messages while one is locked", ...)      // lock it in a test transaction
  t.Run("it attempts a locked message again after the contention delay", ...)
  t.Run("it postpones the delivery after a failed attempt, and increments its failures", ...)
  t.Run("it postpones the delivery when a row that the handler needs is locked, without a failure", ...)
  t.Run("it postpones a message whose envelope can not be unmarshaled", ...)
  t.Run("it leaves a message for another attempt when an attempt ends with an error", ...)
  t.Run("it lets an attempt finish after losing its queue", ...)              // stop the other node; the live nodes change mid-delivery
  t.Run("it rolls back attempts in flight when it stops", ...)
  ```

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement the attempt**
- [x] **Step 4: Run `make test` to verify they pass**
- [x] **Step 5: Stop for review**

---

### Task 5: Sinks, wiring and removal of `messagepump`

Two commits. The first renames `messagequeue.Handler` to `messagequeue.Sink`,
with no behavior change. The second is the rest of this task.

**Files:**
- Rename (first commit): `internal/messagequeue/handler.go` → `sink.go`
- Rename: `internal/aggregate/commandpump.go` → `commandsink.go`,
  `internal/integration/commandpump.go` → `commandsink.go`,
  `internal/process/deadlinepump.go` → `deadlinesink.go`
- Delete: `internal/messagepump/`, `internal/concurrency/`
- Modify: `internal/messagequeue` (`consumer.go`, `delivery.go`, `doc.go`,
  `router.go`), `internal/process/instancestate.go`, `engine.go`,
  `engineoption.go`, `handler.go`, `routes.go`,
  `internal/x/xtesting/commandqueue.go`, and the SQL in
  `internal/schema/ddl/10-messagequeue`, `20-aggregate` and `20-integration`
- Test: `internal/messagequeue/consumer_test.go`, `router_test.go`, the
  existing suites in `internal/aggregate`, `internal/integration`,
  `internal/process` and the root package, and `engineoption_test.go` (new)

**Interfaces:**
- Produces:

  ```go
  // Sink is the destination to which a [Consumer] passes the messages in the
  // queues.
  type Sink interface {
      // Deliver delivers the message in the given envelope within tx.
      Deliver(ctx context.Context, tx *sql.Tx, d Delivery, env *envelopepb.Envelope) error
  }

  // CommandHandler is the handler for a particular command type, with the
  // values that [Router.RouteCommandToQueue] reads from its configuration.
  type CommandHandler struct {
      Key         *uuidpb.UUID
      Aggregate   dogma.AggregateMessageHandler[dogma.AggregateRoot] // nil unless an aggregate
      Concurrency dogma.ConcurrencyPreference
  }
  ```

  `Consumer.Handlers` becomes `Sinks`, and `Router.Aggregates` becomes
  `CommandHandlers *uuidcollection.Map[CommandHandler]`.
  `aggregate.CommandSink`, `integration.CommandSink` and `process.DeadlineSink`
  implement `Sink`. Their
  `DB`, `CommandTypeIDs`, `DeadlineTypeIDs`, `AcquireDelivery` and
  `PostponeDelivery` go, and each gains an `EngineLogger`. The integration sink
  loses `Concurrency` and gains `HandlerLock *semaphore.Weighted`, as
  `projection.Sink` has. `instancestate.go` returns `messagequeue.ErrFailed`.

  The aggregate's `complete_with_events` and `complete_without_events` lose
  their `message_id` parameter and their removal of the command, and
  `complete_without_events` keeps deleting an instance that has no event
  stream. Both integration functions are dropped, because the integration sink
  appends its events with `eventstream.append_any`, and so is
  `messagequeue.remove`. Each function that 0.2.0 had gets `DROP FUNCTION IF
  EXISTS` for its old signature. The consumer removes a delivered message with a plain `DELETE`.

  In `engine.go`, `runHandler` adds each sink to `queueSinks`, and `Run` starts
  one `messagequeue.Consumer` with them. A disabled handler gets no sink, as it
  gets no pump today. `workerCountForHandler` and the pump constants go. New
  options:

  ```go
  // DefaultMessageQueueContentionDelay is the default time that the engine
  // waits before it attempts to deliver a command or deadline again, if the
  // delivery needs a lock while something else holds it.
  DefaultMessageQueueContentionDelay = 200 * time.Millisecond

  func WithMessageQueueContentionDelay(delay time.Duration) EngineOption

  // The default is 10 times the value that runtime.GOMAXPROCS returns.
  func WithMessageQueueConcurrency(n int) EngineOption
  ```

  Each panics on a non-positive value, like `WithHandleTimeout`.

- Consumes: `messagequeue.Consumer` and `messagequeue.Handler` from task 4.

- [x] **Step 1: Rename `messagequeue.Handler` to `Sink`**, in its own commit.
- [x] **Step 2: Make the pumps into sinks, and change the SQL**
- [x] **Step 3: Route the commands of an integration that minimizes
  concurrency to its key**
- [x] **Step 4: Wire the consumer and delete `messagepump`**

  Add a sentence to the doc comment of each trigger in
  `internal/schema/ddl/10-messagequeue/20-triggers.sql` saying that
  `messagequeue.Consumer` waits for the signal of each queue while its node
  owns the queue's workload.

- [x] **Step 5: Run `make test`**

  The existing engine-level suites cover delivery of commands and deadlines.
  New tests: the router's integration case, the consumer completing a delivery
  whose transaction already removed the message, the integration sink returning
  `ErrBusy` while another call holds the lock, and the two options panicking on
  zero.

  Until task 6, the deadline sink locks the deadline row before the instance
  row, the reverse of `endInstance`. PostgreSQL detects the deadlocks and aborts
  one transaction. Say so at the review stop.

- [x] **Step 6: Update `CHANGELOG.md`**

  Under `### Added`: `WithMessageQueueConcurrency()`,
  `WithMessageQueueContentionDelay()` and `DefaultMessageQueueContentionDelay`.
  Under `### Changed`: the engines that share a database divide the commands
  and deadlines between them, with one limit of 10 × `GOMAXPROCS` attempts by
  default in place of a limit per handler; and one engine at a time delivers
  the commands of an integration that prefers `MinimizeConcurrency`, so two may
  briefly overlap while the cluster's membership changes.

- [x] **Step 7: Stop for review**

---

### Task 6: Claim-first deadline delivery

The consumer locks a deadline's row when it claims it, and the deadline sink
then locks the process instance. Handling an event locks the instance first,
and if the handler ends the instance, deletes the instance's deadlines, which
locks their rows. The two take the locks in opposite orders, so they can
deadlock. 0.2.0 had the same inversion.

**Files:**
- Modify: `internal/process/deadlinesink.go`,
  `internal/process/instance.go` (renamed from `instancestate.go`),
  `internal/process/eventsink.go` (renamed from `sink.go`, with `process.Sink`
  renamed to `EventSink`, and `projection.Sink` likewise),
  `internal/messagequeue/sink.go`, `internal/messagequeue/delivery.go`,
  `CHANGELOG.md`
- Test: `internal/process/deadlinesink_test.go` (new),
  `internal/process/instance_test.go` (renamed from `instancestate_test.go`)

**Decisions:**
- A deadline whose process instance has ended is deemed "canceled". Its
  delivery is complete without the handler being called. A delivery is
  complete when its message is delivered or canceled.
- `DeadlineSink.Deliver` locks the process instance with
  `FOR UPDATE SKIP LOCKED` before it unmarshals the deadline. If another transaction holds the lock, it
  returns `messagequeue.ErrBusy`. If the instance has ended, it returns nil
  without calling the handler, and the consumer removes the deadline.
- Ending an instance deletes only the deadlines that no attempt has claimed,
  using `FOR UPDATE SKIP LOCKED` in a subquery, so it never waits for a message
  row. If an attempt has claimed one, the sink in that attempt finds the
  instance ended, and the consumer removes the deadline.
- No new option, and no `lock_timeout`.
- `instance` is the engine's model of a process instance: its handler key, ID,
  whether it has ended, its stored state, its deadline queue ID and its root.
  `lockInstance` (the event path, which may create the instance and waits) and
  `tryLockInstance` (the deadline path, which never creates or waits) both
  return one, and binding, restoring the root, saving and ending are its
  methods. Deadline delivery stays at one round trip.

- [x] **Step 1: Write the failing tests**

  ```go
  func TestDeadlineSink(t *testing.T) {
      t.Run("when another transaction holds the lock on the process instance", ...)
          t.Run("it returns ErrBusy without calling the handler", ...)
      t.Run("when the process instance has ended", ...)
          t.Run("it returns nil without calling the handler", ...)
  }
  ```

  In `instance_test.go`:

  ```go
  t.Run("it ends an instance without waiting for a deadline that another transaction has locked", ...)
  ```

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement it**, and update the docs of `Sink.Deliver`,
  `Delivery` and `deliverMessage` for canceled deadlines.
- [x] **Step 4: Run `make test` to verify they pass**
- [x] **Step 5: Update `CHANGELOG.md`** under `### Fixed`.
- [x] **Step 6: Stop for review**

---

## Validation to run before the deliveries plan

Not tests. Run by hand on a database with representative data, and record
the results in [#100][100]:

- [ ] `EXPLAIN (ANALYZE, BUFFERS)` for the query for the next messages with
  tens of consumed queues, a handful of message types with handlers, a large
  queue, and many messages being attempted.
- [ ] Throughput and connection-pool waits with the concurrency limit
  saturated.
- [ ] Queries for the next messages per delivered message with a backlog of due
  messages, which batching should keep well below one.

## Work done outside the tasks

Changes made on this branch during the tasks' reviews, each in its own commit:

- [x] The projection sink holds its own handler lock, a `cosyne.Mutex` that it
  takes when the handler prefers `MinimizeConcurrency`, instead of a semaphore
  from the engine. `cosyne.Mutex` was added for this in
  [dogmatiq/cosyne#102][cosyne-102] and released in cosyne 0.4.0. (bb7e552)
- [x] The aggregate package has an `instance` type, like the process package's,
  and locks or creates an instance's row in one statement. (0c1d0e5)

## Out of scope

- Buffered application records, per-delivery streaks and `failing_since`
  from a streak. The deliveries plan does those.
- Shared process preparation and the [#99][99] fix. Same.
- Capacity scores and capacity sharing. They are follow-ups listed in
  [plan 1][plan-1].
- A limit on the attempts in progress for each queue, which an integration that
  minimizes concurrency needs to drain a backlog quickly ([#112][112]).
- Instance placement.

<!-- references -->

[99]: https://github.com/dogmatiq/runkit/issues/99
[100]: https://github.com/dogmatiq/runkit/issues/100
[112]: https://github.com/dogmatiq/runkit/issues/112
[cosyne-102]: https://github.com/dogmatiq/cosyne/pull/102
[logging guide]: ../maintainers/logging.md
[plan-1]: 2026-11-08-queue-1-storage.md
