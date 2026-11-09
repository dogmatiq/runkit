# Queue Plan 1: Storage, Queues and Process Bindings

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Before each task, confirm its
> assumptions with the maintainer, as "Reviewability" describes. Don't stage
> or commit code. The maintainer stages and commits each task as they review
> it. Build and test exclusively through `make` (`make test`,
> `make precommit`), never by invoking the Go toolchain directly. Tests start
> PostgreSQL through testcontainers, so Docker must be running.

**Goal:** Store commands and deadlines in one table, each in a queue whose ID is
its rendezvous key, migrate the existing tables into it, observe the queues in
use, find the node responsible for each queue, and bind every process instance
to the queue its deadlines are added to.

**Architecture:** A new `messagequeue` schema holds one `messages` table and the
idempotency keys. A statement trigger signals `messagequeue.<queue-id>` on the
existing `signal` channel when messages are added or move between queues.
`CreateSchema()` moves rows from the old `commandqueue.commands` and
`process.deadlines` tables into it. A new `internal/messagequeue` package owns
the observed set of queues and the router that chooses each message's initial
queue, whose ID in this pass is always the producing node's ID. The existing
message pumps are repointed at the new table so the engine keeps working until
the consumer plan replaces them. `cluster.FindResponsibleNode` returns the node
responsible for the work with any rendezvous key, with self-affinity.

**Tech stack:** Go, PostgreSQL 18, `cosyne`.

**Spec:** the "One queue system", "Self-affinity", "Process bindings and
allocation" and "Upgrade and migration" sections of [#100][100], plus the
decisions below.

This is the first of three plans for [#100][100]. The consumer and the
deliveries follow in their own plans.

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

- **One table, no `kind` column.** `message_type_id` and the application's
  configuration say whether a row is a command or a deadline. Deadlines carry
  `handler_key` and `instance_id`; commands leave them null.
- **The queue signals on the `signal` channel**, with the topic
  `messagequeue.<queue-id>`. A watcher of `messagequeue` receives each queue ID
  as the subtopic, and the bus's recovery wake-up as `""`. A message that moves
  between queues signals both.
- **The migration runs inside `CreateSchema()`**, during a stop. It is tested
  against the old tables with rows in them and against a fresh database where
  they never existed. The old tables and the `commandqueue` schema are dropped
  once their rows are moved.
- **The migration is disposable.** It lives in one file,
  `10-messagequeue/90-migrate.sql`, whose header gives the version it migrates
  from and says that deleting the file and `TestCreate_migratesQueues` is the
  whole cleanup. Nothing else in the DDL refers to the old tables. The
  checkpoint-constraint migration already in `CreateSchema()` is the precedent,
  and both go in the same follow-up.
- **`failing_since` is a column from the start**, even though only the
  deliveries plan writes it, so there is one migration.
- **A queue is the set of messages one node delivers, and its ID is its
  rendezvous key.** There is no table of queues, since a queue has no state of
  its own. "Rendezvous key" is the term for the mechanism in `cluster`.
  Everywhere else, a name says what the key identifies, so the column is
  `queue_id` on messages and `deadline_queue_id` on process instances, the way
  aggregate instances have `stream_id`.
- **The observed set of queues is a component, `messagequeue.Catalog`.** It
  watches the `messagequeue` topic and reads the whole set with `SELECT
  DISTINCT queue_id` on every signal, because the signal bus drops signals
  when a watcher's channel is full. The consumer
  plan reads ownership from the same set.
- **Most initial queue IDs are the producing node's ID** in this pass. With
  self-affinity, each node delivers what it produced, and the rendezvous moves a
  stopped node's work to a live one. The exception is a command that an
  aggregate handles. Its queue ID is the ID of the event stream that the
  target instance is bound to, as [#100][100] requires, so commands for one
  instance don't contend across nodes and the number of queues stays bounded
  by the number of streams. An instance with no stream yet falls back to the
  node ID. `messagequeue.Router` has a method for commands and one for
  deadlines, so that the follow-ups at the end of this plan can add smarter
  selection without touching the producers again.
- **No capacity scores or capacity sharing.** Heartbeats keep their current
  transport, and nothing advertises spare capacity. The consumer bounds its
  own attempts.
- **Self-affinity lives in `cluster.FindResponsibleNode`.** No `rendezvous`
  package.
- **The discovery index is `(queue_id, message_type_id, deliver_at)`.**
  Discovery in the consumer plan looks up each owned queue and routed type, so a
  retired route's rows stay outside the ranges it reads. PostgreSQL 18's skip
  scan serves `SELECT DISTINCT queue_id` from the same index. Messages with the
  same `deliver_at` come in no particular order, so `message_id` isn't a
  tie-break and isn't in the index.

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
- Use a set or map when order means nothing.
- Use "rendezvous key" only in `cluster`, where keys are resolved to nodes.
  Elsewhere name what the key identifies: a queue, by its `queue_id`.

## Review focus

These conditions are easy to get wrong, and no existing test exercises them.
Each one has a test in the task that owns the code.

1. `CreateSchema()` run twice against a migrated database must not fail or
   duplicate rows, and run against a fresh database must not reference the old
   tables. Task 1 has both tests.
2. A migrated command keeps its idempotency key, so a re-executed command with
   that key is still deduplicated after the upgrade. Task 1 has the test.
3. An existing process instance with no binding gets one the next time it is
   loaded, and that binding is persisted in the same transaction as the
   delivery. Task 4 has the test.
4. A binding changed by hand must be used for the instance's next deadline,
   not overwritten. Task 4 has the test.
5. `FindResponsibleNode` must return the same ID wherever it's called, given
   the same set of live nodes, and a key equal to the ID of a node that is no
   longer live must fall back to ordinary scoring. Task 6 has the tests.

---

### Task 1: The `messagequeue` schema and the migration

**Files:**
- Create: `internal/schema/ddl/10-messagequeue/00-tables.sql`,
  `10-funcs.sql`, `20-triggers.sql`, `90-migrate.sql`
- Delete: `internal/schema/ddl/10-commandqueue/`
- Modify: `internal/schema/ddl/20-process/00-tables.sql` (remove
  `process.deadlines`, add `deadline_queue_id uuid` to `process.instances`)
- Test: `internal/schema/schema_test.go`

**Interfaces:**
- Produces, in SQL:

  ```sql
  CREATE TABLE IF NOT EXISTS messagequeue.messages (
      message_id      uuid        PRIMARY KEY,
      message_type_id uuid        NOT NULL,
      queue_id  uuid        NOT NULL,
      handler_key     uuid,
      instance_id     text        CHECK (instance_id != ''),
      envelope        bytea       NOT NULL,
      failures        int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
      failing_since   timestamptz,
      scheduled_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
      deliver_at     timestamptz NOT NULL DEFAULT clock_timestamp(),

      CHECK ((handler_key IS NULL) = (instance_id IS NULL)),
      CHECK ((failures = 0) = (failing_since IS NULL))
  );

  CREATE INDEX IF NOT EXISTS messages_by_queue
  ON messagequeue.messages (queue_id, message_type_id, deliver_at);

  CREATE INDEX IF NOT EXISTS messages_by_instance
  ON messagequeue.messages (handler_key, instance_id)
  WHERE handler_key IS NOT NULL;

  CREATE TABLE IF NOT EXISTS messagequeue.idempotency_keys (…);  -- as today

  -- messagequeue.add_command(message_id, message_type_id, queue_id,
  -- envelope, idem_key) RETURNS TABLE (actual_message_id uuid, enqueued
  -- boolean), as commandqueue.add today with the extra column.
  -- messagequeue.remove(message_id) RETURNS void, as commandqueue.remove today.
  ```

  `scheduled_at` is the original time: `enqueued_at` for a command, the
  deadline's scheduled time for a deadline. `deliver_at` is the earliest time
  the next attempt may run. It starts equal to `scheduled_at` and moves later
  on each postponement.

  Two statement-level triggers, each with a function of the same name, send
  `pg_notify('signal', 'messagequeue.' || queue_id)` once per distinct queue.
  `signal_add` fires after insert, and `signal_move` after update, signaling
  the old and new queue of each message whose `queue_id` changed. Deletes
  signal nothing, since every successful delivery deletes a row and every node
  receives every signal. PostgreSQL doesn't allow transition tables on an
  `UPDATE OF` trigger, so `signal_move` compares the rows itself. Trigger
  functions are named for the operation, as `eventstream.signal_append` is.

  `90-migrate.sql` is a `DO` block. If `to_regclass('commandqueue.commands')` is
  not null, it inserts each row into `messagequeue.messages` with
  `queue_id = message_type_id`, `scheduled_at = enqueued_at` and
  `failing_since = CASE WHEN failures > 0 THEN deliver_at END`, moves
  `commandqueue.idempotency_keys` into `messagequeue.idempotency_keys`, and
  drops the `commandqueue` schema. If `to_regclass('process.deadlines')` is not
  null, it inserts each row with `queue_id = handler_key` and
  `scheduled_at = deliver_at`, and drops the table. The scheduled time is inside
  the protobuf envelope, which SQL can't read, so a deadline that had already
  failed before the upgrade keeps its next attempt time as its scheduled time.
  The changelog says so.

- [x] **Step 1: Write the failing tests**

  ```go
  func TestCreate_migratesQueues(t *testing.T)
  // builds the tables of v0.1.0 through v0.2.0 by hand (commandqueue.commands,
  // commandqueue.idempotency_keys, process.deadlines) with two commands, one
  // idempotency key and two deadlines, runs schema.Create twice, then checks:
  //   - messagequeue.messages has four rows with the expected keys and times
  //   - messagequeue.idempotency_keys has the key, pointing at its command
  //   - to_regclass('commandqueue.commands') and ('process.deadlines') are null
  //   - the commandqueue schema is gone

  func TestCreate_onFreshDatabase(t *testing.T)
  // schema.Create on a new database succeeds and creates messagequeue.messages
  ```

  Extend `TestCreateAndDrop` so `Drop` removes the `messagequeue` schema.

- [x] **Step 2: Run `make test` to verify they fail**

- [x] **Step 3: Write the DDL**

  `schema.Create` walks the embedded directory in name order, so
  `10-messagequeue/90-migrate.sql` runs once the `messagequeue` tables exist and
  before `20-process`. `20-process/00-tables.sql` no longer creates
  `process.deadlines`, and adds the binding column with `ALTER TABLE … ADD
  COLUMN IF NOT EXISTS deadline_queue_id uuid`.

- [x] **Step 4: Run `make test` to verify the schema tests pass**

  The engine tests fail until task 2.

- [x] **Step 5: Stop for review**

---

### Task 2: Repoint producers, pumps and test helpers

**Files:**
- Modify: `execute.go:70-95`, `internal/process/commandqueue.go`,
  `internal/process/instancestate.go:161-220`
- Modify: `internal/messagepump/commandqueue.go`,
  `internal/process/deadlinepump.go:40-130,255-270`
- Modify: `internal/x/xtesting/commandqueue.go`,
  `internal/x/xtesting/deadline.go`
- Modify: `internal/messagepump/delivery.go` (add `QueueID`)
- Test: existing tests in `internal/aggregate`, `internal/integration`,
  `internal/process`, and the root package

**Interfaces:**
- Produces: every producer passes a queue ID. In this task it is the
  node ID, through a new `NodeID *uuidpb.UUID` field on `process.Sink`,
  `process.DeadlinePump` and the integration pump, and `e.nodeID` in
  `execute.go`. Task 4 replaces this with `messagequeue.Router`.
- Consumes: the `messagequeue` schema from task 1.

- [x] **Step 1: Repoint the SQL**

  `commandqueue.add` → `messagequeue.add_command`. `persistDeadlines` inserts
  into `messagequeue.messages` with `deliver_at = scheduled_at`. `endInstance`
  deletes from `messagequeue.messages` by handler and instance.
  `AcquireCommandDelivery` reads `messagequeue.messages WHERE handler_key IS
  NULL AND message_type_id = ANY($1) AND deliver_at <= clock_timestamp()`. The
  deadline pump reads `WHERE handler_key = $1`, joining `process.instances` as
  today. Both postponements set `deliver_at`, bump `failures`, and set
  `failing_since = COALESCE(failing_since, clock_timestamp())`. Successful
  removal is `messagequeue.remove`.

- [x] **Step 2: Update the test helpers**

  The `xtesting` helpers query `messagequeue.messages`.
  `EnqueuePostponedCommand` inserts with a queue ID argument.

- [x] **Step 3: Run `make test` to verify everything passes**

  No new behavior; the existing suites are the test.

- [x] **Step 4: Update `CHANGELOG.md`**

  Under `### Changed`: the schema change, that `CreateSchema()` moves pending
  commands and deadlines into the new queue and must run while every node is
  stopped, and that a previously failed deadline's original scheduled time is
  taken from its next attempt time.

- [x] **Step 5: Stop for review**

---

### Task 3: `messagequeue.Catalog`

**Files:**
- Create: `internal/messagequeue/doc.go`,
  `internal/messagequeue/catalog.go`
- Modify: `engine.go` (build and run it),
  `internal/schema/ddl/10-messagequeue/20-triggers.sql` (name the listener)
- Test: `internal/messagequeue/catalog_test.go`

**Interfaces:**
- Produces:

  ```go
  // Catalog keeps track of which queues have messages, as seen from this
  // node.
  //
  // It reads the whole set from the database each time a message queue
  // signal arrives, so a signal dropped by the bus is covered by the next.
  type Catalog struct {
      DB           *sql.DB
      Signals      *signal.Bus
      EngineLogger *slog.Logger
      // unexported: cosyne.Observable[*uuidcollection.Set]
  }

  // QueueIDs returns an observable of the IDs of the queues that have
  // messages. The set must not be modified.
  func (c *Catalog) QueueIDs() cosyne.ObservableR[*uuidcollection.Set]

  // Run watches for queues until ctx is canceled. It must be called at most
  // once.
  func (c *Catalog) Run(ctx context.Context) error
  ```

  Each published set is a new value, so readers never see it change under
  them. The set is nil until the first read.

- Consumes: the trigger from task 1.

- [x] **Step 1: Write the failing tests**

  ```go
  func TestCatalog(t *testing.T) {
      t.Run("it reads the queues that already have messages", ...) // rows inserted before Run
      t.Run("it includes a queue once a message is added to it", ...)
      t.Run("it includes the queue that a message moves to, not the one it moved from", ...) // UPDATE … SET queue_id
      t.Run("it excludes a queue that has no messages after the next signal", ...)
  }
  ```

  Insert rows with a small SQL helper in the test; `xtesting` gains nothing
  here.

- [x] **Step 2: Run `make test` to verify they fail**

- [x] **Step 3: Implement `Catalog`**

  `Run` wraps `supervisor.Run(ctx, EngineLogger, "observe queues", …)`. The
  body watches `messagequeue` with a capacity of 1 and ignores the subtopic,
  as the local event stream source does for `eventstream.create`. On every
  signal it reads `SELECT DISTINCT queue_id FROM messagequeue.messages`, and
  publishes the result only when the set changed. The signal bus drops a
  signal when a watcher's channel is full, so reading the whole set on each
  signal means no dropped signal can leave a queue out.

- [x] **Step 4: Wire it in `engine.go`**

  `e.queues = &messagequeue.Catalog{…}` next to the node, run in
  the same group. Add a sentence to the doc comment of each `messagequeue`
  trigger saying that `messagequeue.Catalog` watches the signal to learn which
  queues have messages.

- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Stop for review**

---

### Task 4: `messagequeue.Router` and process bindings

**Files:**
- Create: `internal/messagequeue/router.go`
- Modify: `internal/process/instancestate.go`, `internal/process/sink.go`,
  `internal/process/deadlinepump.go`, `internal/process/commandqueue.go`,
  `execute.go`, `engine.go`
- Test: `internal/messagequeue/router_test.go`,
  `internal/process/instancestate_test.go`

**Interfaces:**
- Produces:

  ```go
  // Router chooses the queue for each new message.
  type Router struct {
      NodeID     *uuidpb.UUID
      Aggregates *uuidcollection.Map[AggregateRoute] // by command type ID
  }

  // AggregateRoute is the aggregate that handles a particular command type.
  type AggregateRoute struct {
      Key     *uuidpb.UUID
      Handler dogma.AggregateMessageHandler[dogma.AggregateRoot]
  }

  func (r *Router) RouteCommandToQueue(ctx context.Context, q xsql.Querier, env *envelopepb.Envelope) (*uuidpb.UUID, error)
  func (r *Router) RouteDeadlinesToQueue(ctx context.Context, q xsql.Querier, handlerKey *uuidpb.UUID, instanceID string) (*uuidpb.UUID, error)
  ```

  Both methods take the caller's querier, so a caller inside a transaction
  never needs a second pooled connection, which could stall when the
  application caps the pool. `RouteDeadlinesToQueue` doesn't use it yet.

  `RouteCommandToQueue` takes the envelope, so it can tell from the message
  type ID whether an aggregate handles the command without unpacking it. For
  an aggregate command, it routes the command to an instance and reads that
  instance's `stream_id` from `aggregate.instances`, which is the queue ID.
  If the instance doesn't exist or isn't bound to a stream, it returns the
  node ID, since [#100][100] forbids creating or binding an aggregate just to
  choose a queue. Any other command, or one whose routing panics or returns an
  empty instance ID, is added to the node's queue, and delivery reports the
  routing failure as it does today.

  `RouteDeadlinesToQueue` takes the handler's key and the instance ID, and
  returns the node ID. It chooses one queue for all of an instance's
  deadlines, rather than one for each deadline, because [#100][100] binds an
  instance to a queue when its row is first stored, including instances that
  never schedule a deadline, and every deadline it schedules is added to that
  queue. Its doc says so, since the binding outlasts the router's choice.
  Routing each deadline from its envelope alone, such as by hashing its
  instance ID into a fixed number of queues per handler, would make binding
  unnecessary, but it conflicts with score-based placement in follow-up 2.

  `loadInstance` takes the router. Its upsert returns `deadline_queue_id`
  along with the state. If the instance is unbound, it calls
  `RouteDeadlinesToQueue(handlerKey, instanceID)` and stores the result with
  an `UPDATE` under the row lock the upsert holds, so the router is only asked
  once per instance. `persistDeadlines` takes the binding as the queue ID. The
  process sink, the deadline pump and `execute.go` take the router in place of
  their `NodeID` from task 2. Integration handlers never produce commands, so
  the integration pump is unchanged.

- Consumes: nothing new.

- [x] **Step 1: Write the failing tests**

  ```go
  func TestRouter(t *testing.T) {
      // RouteCommandToQueue
      t.Run("it returns the ID of the event stream that the target instance is bound to", ...)
      t.Run("it returns the node ID when the target instance isn't bound to an event stream", ...)
      t.Run("it returns the node ID when the target instance doesn't exist", ...)
      t.Run("it returns the node ID when only another aggregate has an instance with the target ID", ...)
      t.Run("it returns the node ID for a command that no aggregate handles", ...)
      t.Run("it returns the node ID when the aggregate routes the command to an empty instance ID", ...)
      t.Run("it returns the node ID when routing the command panics", ...)
      // RouteDeadlinesToQueue
      t.Run("it returns the node ID", ...)
  }

  func TestInstanceState_deadlineQueue(t *testing.T) {
      t.Run("it binds a new instance when it first handles an event", ...)
      t.Run("it binds an existing instance when it next handles an event", ...)
      t.Run("it adds a deadline to its instance's queue", ...)
      t.Run("it keeps a binding that was changed by hand", ...)
  }
  ```

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement the router and the binding in `loadInstance`**
- [x] **Step 4: Wire the router in `engine.go`**
- [x] **Step 5: Run `make test` to verify they pass**
- [x] **Step 6: Stop for review**

---

### Task 5: Split `engine.go`

`engine.go` holds the engine's lifecycle, the code that runs each handler, and
several views of the application's configuration. Plan 2 rewrites the code that
runs each handler, so splitting the file first means that rewrite lands in a
small file of its own.

**Decide first:** how the engine turns the configuration into the things it
constructs. Step 2 depends on the answer.

- **Top-down.** Each thing the engine constructs builds, or asks for, what it
  needs from the configuration. `aggregatesForQueueRouter` is an example: the
  router's construction makes its own pass over the handlers. Each view stays
  next to the one thing that uses it, but the configuration is walked once for
  each view.
- **One pass.** The engine iterates over the configuration once, and adds the
  relevant parts of each handler to each thing it's constructing as it goes,
  such as an aggregate's command types to the router and its event types to
  the projections' consumers. The configuration is walked once, but that loop
  has to know about everything the engine constructs.
- **Constructors in each package.** Each package exports a function that takes
  the application's configuration, and any other inputs, and builds its own
  type, such as one in `messagequeue` that builds a `Router`. Each package
  knows what it needs from the configuration, and `engine.go` shrinks to
  wiring. But each package then depends on `enginekit/config`, and its types
  are no longer plain structs whose required fields the engine fills in.

**Files:**
- Modify: `engine.go`, `execute.go`, `internal/projection/compactor.go`
- Delete: `internal/projection/initialize.go`
- Create: `handler.go`, `routes.go`

- [x] **Step 1: Move the per-handler code** — `runHandler`,
  `newLoggerForHandler` and `workerCountForHandler` — into `handler.go`,
  unchanged. Delete `initializeHandler`, `projection.InitializeHandler` and the
  startup loop that calls them. Their only effect was the `projection.handlers`
  row that the compactor locks, so the `Compactor` inserts that row itself when
  it starts.
- [x] **Step 2: Move the configuration views into `routes.go`** —
  `setupCommandTypes`, `aggregatesForQueueRouter`, `consumedEventTypeIDs`,
  `inboundMessageTypeIDsForHandler` and `outboundMessageTypesForHandler` — and
  make each a plain function of the `*config.Application` or `config.Handler`
  it reads, rather than a method of the engine. `setupCommandTypes` becomes
  `executableCommandTypes`, which returns the set rather than storing it.
  `newLoggerForHandler` and `workerCountForHandler` in `handler.go` become
  plain functions too, since neither reads the engine. `runHandler` stays a
  method.
- [x] **Step 3: Run `make test`.** No behavior changes, so the existing suites
  are the test.
- [x] **Step 4: Stop for review** after each step's commit, so each diff shows
  one kind of change.

---

### Task 6: `cluster.FindResponsibleNode`

**Files:**
- Create: `internal/cluster/rendezvous.go`
- Delete: `internal/cluster/membership.go`
- Modify: `internal/cluster/node.go`, the event stream consumer and sinks,
  `internal/x/xtesting/cluster.go`
- Test: `internal/cluster/node_test.go`

**Interfaces:**
- Produces:

  ```go
  // FindResponsibleNode returns the ID of the node that is responsible for the
  // work with the given rendezvous key. It returns false if there are no live
  // nodes.
  //
  // If the key is an exact match for the ID of a live node, that node is
  // responsible. Otherwise, the live node with the highest score for the key
  // is responsible.
  func FindResponsibleNode(key *uuidpb.UUID, liveNodes *uuidcollection.Set) (*uuidpb.UUID, bool)

  // IsResponsibleFor returns true if, among the given live nodes, this node is
  // the one responsible for the work with the given rendezvous key.
  func (n *Node) IsResponsibleFor(key *uuidpb.UUID, liveNodes *uuidcollection.Set) bool

  // LiveNodes returns an observable set containing the ID of each live node.
  //
  // A node is live while its heartbeats keep arriving within
  // [Node.HeartbeatTimeout]. That includes this node, which is live only while
  // its own heartbeats arrive.
  func (n *Node) LiveNodes() cosyne.ObservableR[*uuidcollection.Set]
  ```

  `Membership` is replaced by a plain set of live node IDs, and
  `Node.Membership` by `Node.LiveNodes`. The set has no notion of a local
  node, so `Membership.LocalNodeIsResponsibleFor` becomes
  `Node.IsResponsibleFor`, which calls the unexported `isResponsibleFor` in
  `rendezvous.go` to ask whether `FindResponsibleNode` returns the node's own
  ID. A node that can't hear itself isn't in its own set, so it is still
  responsible for nothing. `Membership.Peers` goes. Its only callers,
  `xtesting.Cluster` and the cluster tests, compare the whole set instead.

  A value built to pass to these functions is a rendezvous key, so
  `eventstream.Sink.Workload` becomes `RendezvousKey`, and docs and tests
  that called these values workloads call them rendezvous keys.

- Consumes: nothing new.

- [x] **Step 1: Write the failing tests**

  The tests run real nodes in `node_test.go`, beside the existing
  responsibility tests.

  ```go
  func TestNode(t *testing.T) {
      t.Run("it makes the node whose ID is the key responsible for the work with that key", ...) // four nodes
      t.Run("it scores a key normally when no live node has that ID", ...)
      t.Run("it returns the same responsible node on every node", ...)       // FindResponsibleNode agrees with each node's IsResponsibleFor
      t.Run("it returns false when there are no live nodes", ...)
  }
  ```

- [x] **Step 2: Run `make test` to verify they fail**
- [x] **Step 3: Implement it**
- [x] **Step 4: Run `make test` to verify they pass**
- [x] **Step 5: Stop for review**

---

## Follow-ups once the queue lands

Each is a small change on its own, in this order:

1. **Capacity scores.** A shared per-node semaphore samples its occupancy and
   waiting time over each heartbeat interval. The heartbeat gains a payload
   with the score, on its own `heartbeat` channel, and `cluster.Node` listens
   on its own connection instead of through `signal.Bus`. `Node` gains a way
   to read each live node's score. This is a **[BC]** transport change.
   Heartbeats are the last users of `signal.Bus` once task 1 of [plan
   2][plan-2] adds `Node.AttachSignal`. Moving them onto the LISTEN connection
   that `cluster.Node` already owns lets this follow-up delete `signal.Bus` and
   the `signal` package. Heartbeats are also the first Go code to send a
   signal, so this follow-up adds a way to send one, such as a cluster-wide
   `Signal` type.

   The payload also carries the heartbeat's expiry, so that nodes don't have
   to share one heartbeat interval. `Node.HeartbeatTimeout` becomes a
   multiplier, such as `HeartbeatExpiryMultiplier`, that the sender applies to
   its own interval to compute the expiry. A receiver removes a node from its
   live nodes once that node's latest heartbeat expires.
2. **Score-based process binding allocation.**
   `Router.RouteDeadlinesToQueue` implements the six-step algorithm from
   [#100][100] using `messagequeue.Catalog` and the scores. Revisit
   `cluster.FindResponsibleNode`'s `ok` result once this exists. If every
   caller that sees `ok == false` returns an error, make it an error. If every
   such caller uses its own node, make it fall back to the local node.
3. **Producer-side routing for integration commands.** Task 4 routes
   aggregate commands. `Router.RouteCommandToQueue` could also route an
   integration command to its handler's key when that minimizes concurrency.
4. **`WithQueueConcurrencyLimit`.**
5. **Tuning.** Measure the 200 ms lock timeout, the 200 ms contention delay
   and the `10 × GOMAXPROCS` limit against representative load: successful
   lock waits, postponement rate, slot and connection occupancy, and
   delivery latency.

   Also measure the `messagequeue` signal. The triggers send it whenever a
   message is added or moved, so under sustained load every node's
   `messagequeue.Catalog` reloads its queue IDs back to back. Sending it only
   when a queue gains its first message would cut that, but on its own it can
   lose a queue: if the catalog reloads after the last committed message is
   deleted but before a concurrent insert commits, that insert sees a
   non-empty queue, sends no `messagequeue` signal, and the queue stays out of
   the catalog. That change needs a periodic catalog reload as a backstop.
6. **Garbage collection** of queued rows whose message types are no longer
   routed, which discovery never reads.
7. **Drop the pre-release migrations** from `CreateSchema()`, this plan's
   `90-migrate.sql` and the checkpoint-constraint one, with their tests, before
   1.0. [#103][103] proposes a migration ledger whose 1.0 baseline does this.

## Out of scope

- Discovery, admission and claiming. The consumer plan reads
  `messagequeue.Catalog` and the index from task 1.
- Changing a stored queue ID automatically.
- Score-based binding allocation and producer-side command routing. They are
  follow-ups above.
- Writing `failing_since` from a streak. The deliveries plan does that; task 2
  only keeps it consistent with `failures`.

<!-- references -->

[100]: https://github.com/dogmatiq/runkit/issues/100
[103]: https://github.com/dogmatiq/runkit/issues/103
[logging guide]: ../maintainers/logging.md
[plan-2]: 2026-11-08-queue-2-consumer.md
