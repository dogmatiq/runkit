# Plan: Attempt one message at a time from a serial queue (#112)

## Where the work stands

The work is on the branch `serial-queues`, which starts from `main`. The only
task is done and committed, and the engine-level test task was dropped. The
branch isn't pushed, and there's no pull request yet. #100, which #112 was
blocked by, is closed.

## Context

An integration handler that prefers `dogma.MinimizeConcurrency` drains a backlog
of commands at about one command per contention delay (5 a second with the
default 200 ms), however fast the handler is.

The router adds every command for such an integration to one queue, whose ID is
the integration's identity key. The consumer doesn't know that the integration's
sink lets only one call to the handler run at a time. While there's a backlog,
`startAttempts` in
[internal/messagequeue/consumer.go](../../internal/messagequeue/consumer.go)
starts up to its concurrency limit of attempts on that queue at once. One
attempt gets the sink's lock. Every other attempt opens a transaction, locks its
message's row, unpacks the command, rolls back, and returns `ErrBusy`, and then
the consumer writes a postponement for that message. When the first call
returns, every other message in the backlog is postponed, so nothing is due
until the contention delay has passed. The failed attempts also use database
round trips and hold the consumer's concurrency slots, which delays due messages
for other handlers.

The fix is that the consumer makes at most one attempt at a time on such a
queue. The other messages in the queue stay as they are: no attempt, no
transaction, no lock and no write. When the attempt ends, the consumer attempts
the queue's next message straight away.

Decided in planning:

- **The limit belongs to the queue, and the router is its only source.** The
  router already decides, when a command is added, that an integration that
  minimizes concurrency gets a queue of its own. The same check now also
  produces the set of serial queue IDs that the engine passes to the consumer.
  `Sink` and every sink are unchanged.
- **Nothing is added to the database.** A column on each message that records
  the router's decision was considered. It stores what the engine already knows
  from the application's configuration, and the consumer would only learn of a
  serial queue from the rows that it reads. No table of queues is needed
  either.
- **The queue index puts time before type.** `queued_messages_by_queue` is now
  on (`queue_id`, `next_attempt_at`, `message_type_id`), which reverses the
  order chosen in #100. One search per queue then reads messages in the order
  in which they're due, so the query needs no search per message type and no
  window function. The cost is that a search reads past the messages of a type
  with no sink, such as those of a disabled handler, which is rare in
  production. It checks their type in the index without reading the table: in
  a test with 50,000 such messages at the front of one queue, the whole search
  read 369 buffer pages. The schema is unreleased, so the v2.0.0 migration and
  `current/` change, and no migration is added.
- **One query finds the next messages.** A window function over a search per
  message type, two separate queries merged in Go, and CTEs that number each
  queue's candidates were all tried before the index changed. PostgreSQL 18,
  the version that the tests use, accepts a lateral `LIMIT` that depends on the
  outer row, which lets a serial queue's search take one message. The query
  takes one list of messages to skip.
- **A message in the wrong queue is slow, not wrong.** Another handler's message
  in a serial queue is delivered one at a time, which is stricter than it
  needs. A command of a serial integration in another queue, such as one added
  before a deployment changed the handler's preference, can be attempted at
  the same time as the serial queue's attempt.
- **The integration's sink has no lock.** Dogma calls `ConcurrencyPreference`
  a hint, and `MinimizeConcurrency` asks the engine to "attempt" one message at
  a time. The consumer's limit does that, so the sink's lock and its `ErrBusy`
  path are removed. While two nodes both own the queue, as
  `cluster.FindWorkloadOwner` allows, both nodes can call the handler. That was
  true with the lock too, and is out of scope.
- **No changelog entry.** The consumer that has this problem is in `[Unreleased]`
  and was never released. Check this against the maintaining-changelogs skill
  before committing.

## Words

- **serial queue**: a queue from which the consumer attempts one message at a
  time. The user chose this name from three candidates (the others were
  "exclusive queue" and "one-at-a-time queue"). The opening doc sentence of the
  consumer's field: "SerialQueueIDs is the set of IDs of the queues from which
  the consumer attempts one message at a time."
- "Concurrency limit" still means the consumer's overall limit, so the plan
  never uses it for the limit of one attempt on a serial queue.

## Settled design

- **The router.** In
  [internal/messagequeue/router.go](../../internal/messagequeue/router.go),
  `Router.SerialQueueIDs()` returns the key of each handler in
  `CommandHandlers` whose `ConcurrencyPreference` is
  `dogma.MinimizeConcurrency`. `CommandHandler` needs no new method:
  `ConcurrencyPreference` is already `dogma.MaximizeConcurrency` for an
  aggregate, so that comparison alone picks out the integrations whose commands
  `RouteCommandToQueue` adds to the handler's own queue. The set is worked out from the
  application's configuration, so it doesn't change while the engine runs.
- **The engine.** In [engine.go](../../engine.go), the consumer gets
  `SerialQueueIDs: e.queueRouter.SerialQueueIDs()`. The consumer takes the set,
  not the router, because an internal component takes the values that it
  needs.
- **What the consumer tracks.** `attemptsInProgress` changes from a set of
  message IDs to a map from each message ID to the ID of the message's queue.
  `message` gains a `QueueID`. A serial queue is busy while an attempt on one
  of its messages is in progress. Nothing is marked or unmarked:
  `recordAttempt` already deletes the attempt, whatever its outcome, and that
  frees the queue.
- **The query.** `findNextMessagesOnce` is given the owned queues, minus the
  serial queues that have an attempt in progress. It passes those queues and
  `SerialQueueIDs` as two lists. A lateral search of each queue reads its
  messages in the order in which they're due, skips types with no sink with
  `message_type_id = ANY`, and uses `LIMIT CASE WHEN` the queue is in
  `SerialQueueIDs` `THEN 1 ELSE` the headroom. The final
  `ORDER BY ... LIMIT` takes the messages from all of the searches in the order
  in which they're due. The messages being attempted and the contended
  messages are passed as one list to skip. The query also returns each
  message's queue ID.
- **When the consumer looks again.** `Run` skips `startAttempts` after a
  completed attempt if the backlog is empty. A serial queue's next message is
  due and unstarted when its attempt completes, so `startAttempts` reports that
  the backlog isn't known to be empty while any serial queue is busy. The cost
  is one extra query for each attempt that completes while a serial queue is
  busy.
- **Postponement is unchanged.** `ErrBusy`, the contention delay and
  `postponeAfterContention` stay as they are, for the instance locks of
  aggregates and processes.

## Tasks

### [x] Attempt one message at a time from a serial queue (ac03e97)

Add `Router.SerialQueueIDs`, the consumer's
`SerialQueueIDs` field, the engine's wiring, and the consumer and query changes
described under "Settled design". Update the docs that describe the behavior:
the `Consumer` doc comment,
[internal/messagequeue/doc.go](../../internal/messagequeue/doc.go). Remove the
lock, the `ConcurrencyPreference` field and the `ErrBusy` path from
[internal/integration/commandsink.go](../../internal/integration/commandsink.go),
with their tests.

Tests in
[internal/messagequeue/router_test.go](../../internal/messagequeue/router_test.go),
which check only what `SerialQueueIDs` promises, not how queue IDs are chosen:

- `SerialQueueIDs` contains the queue that `RouteCommandToQueue` chooses for an
  integration that minimizes concurrency, and not the queue for one that
  maximizes it.

Tests in
[internal/messagequeue/consumer_test.go](../../internal/messagequeue/consumer_test.go).
The serial tests run the consumer through a new helper that takes the serial
queue IDs, and `run` is unchanged:

- It makes one attempt at a time on a serial queue, and attempts the queue's
  next message as soon as the previous attempt ends.
- It doesn't postpone any message in a serial queue while it drains a backlog.
- A backlog in a serial queue, with earlier due times, doesn't stop due messages
  in other queues from being attempted at the same time.
- Two serial queues are each attempted one message at a time, and at the same
  time as each other.
- A serial queue with messages of two types has one attempt at a time across
  both types.
- It attempts the next message in a serial queue after an attempt completes,
  even when the backlog was otherwise empty.
- It still attempts messages in other queues up to the concurrency limit (the
  existing test for this still passes).

Then run the tests that cover the change through `make`, as the building skill
says.

### Dropped: Test that the engine drains a MinimizeConcurrency backlog quickly

This task would have run an engine with a long contention delay, so that a
postponement made the test time out. Without the sink's lock, the integration's
sink never returns `ErrBusy`, so nothing can postpone these commands. The
existing test in
[internal/integration/concurrency_test.go](../../internal/integration/concurrency_test.go)
that runs ten commands through a `MinimizeConcurrency` handler, and fails if the
handler is called concurrently, now checks the consumer's limit end to end.

## Not in this plan

- Keeping a `MinimizeConcurrency` handler to one call at a time across nodes
  while two nodes both own its queue.
- Any configuration of the serial limit, which is always one.
- A later allocator that chooses deadline queues from the queue IDs in the
  table, as #100 describes, must not choose a serial queue. That is a note for
  that work.
