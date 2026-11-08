# Queue Plan 1: Storage, Rendezvous and Process Bindings

> **For agentic workers:** Implement this plan task by task. Every task ends
> with one review stop; do not batch tasks. Before each task, confirm its
> assumptions with the maintainer, as "Reviewability" describes. Don't stage
> or commit code. The maintainer stages and commits each task as they review
> it. Build and test exclusively through `make` (`make test`,
> `make precommit`), never by invoking the Go toolchain directly. Tests start
> PostgreSQL through testcontainers, so Docker must be running.

**Goal:** Store commands and deadlines in one queue table keyed by rendezvous
key, migrate the existing tables into it, observe the keys in use, name the
node responsible for each key, and give every process instance a queue
binding that its deadlines inherit.

**Architecture:** A new `queue` schema holds one `messages` table and the
idempotency keys. A statement trigger signals `queue.<rendezvous-key>` on the
existing `signal` channel for inserts, deletes and key changes. `CreateSchema()`
moves rows from the old `commandqueue.commands` and `process.deadlines` tables
into it. A new `internal/queue` package owns the observed set of rendezvous
keys and the policy that chooses each message's initial key, which in this
pass is always the producing node's ID. The existing message pumps are
repointed at the new table so the engine keeps working until the consumer
plan replaces them. `Membership.ResponsibleNode` names the node responsible
for any rendezvous key, with self-affinity.

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
  `queue.<rendezvous-key>`. A watcher of `queue` receives each key as the
  subtopic, and the bus's recovery wake-up as `""`. A key change signals both
  the old and the new key.
- **The migration runs inside `CreateSchema()`**, during a stop. It is tested
  against the old tables with rows in them and against a fresh database where
  they never existed. The old tables and the `commandqueue` schema are dropped
  once their rows are moved.
- **The migration is disposable.** It lives in one file,
  `10-queue/90-migrate.sql`, whose header names the version it migrates from
  and says that deleting the file and `TestCreate_migratesQueues` is the
  whole cleanup. Nothing else in the DDL refers to the old tables. The
  checkpoint-constraint migration already in `CreateSchema()` is the
  precedent, and both go in the same follow-up.
- **`failing_since` is a column from the start**, even though only the
  deliveries plan writes it, so there is one migration.
- **The observed rendezvous key set is a component,
  `queue.RendezvousKeys`.** It watches the `queue` topic, adds each
  rendezvous key it hears, and rebuilds itself with
  `SELECT DISTINCT rendezvous_key` on each recovery wake-up. The consumer plan
  reads ownership from the same set.
- **Every initial rendezvous key is the producing node's ID** in this pass.
  With self-affinity, each node delivers what it produced, and the
  rendezvous moves a stopped node's work to a live one.
  `queue.RendezvousKeyPolicy` has a method for commands and one for process
  instances so that the follow-ups at the end of this plan can add smarter
  selection without touching the producers again.
- **No capacity scores or capacity sharing.** Heartbeats keep their current
  transport, and nothing advertises spare capacity. The consumer bounds its
  own attempts.
- **Self-affinity lives in `Membership.ResponsibleNode`.** No `rendezvous`
  package.
- **The discovery index leads with the key and the type**:
  `(rendezvous_key, message_type_id, deliver_at, message_id)`. Discovery in
  the consumer plan looks up each owned key and routed type, so a retired
  route's rows stay outside the ranges it reads. PostgreSQL 18's skip scan
  serves `SELECT DISTINCT rendezvous_key` from the same index.

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
- Use "rendezvous key" in names outside the rendezvous code; `key` inside it.

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
5. Every node must name the same responsible node for a key, whatever order
   it holds its peers in, and a key equal to the ID of a node that is no
   longer live must fall back to ordinary scoring. Task 5 has the tests.

---

### Task 1: The `queue` schema and the migration

**Files:**
- Create: `internal/schema/ddl/10-queue/00-tables.sql`,
  `10-funcs.sql`, `20-triggers.sql`, `90-migrate.sql`
- Delete: `internal/schema/ddl/10-commandqueue/`
- Modify: `internal/schema/ddl/20-process/00-tables.sql` (remove
  `process.deadlines`, add `rendezvous_key uuid` to `process.instances`)
- Test: `internal/schema/schema_test.go`

**Interfaces:**
- Produces, in SQL:

  ```sql
  CREATE TABLE IF NOT EXISTS queue.messages (
      message_id      uuid        PRIMARY KEY,
      message_type_id uuid        NOT NULL,
      rendezvous_key  uuid        NOT NULL,
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

  CREATE INDEX IF NOT EXISTS messages_by_rendezvous_key
  ON queue.messages (rendezvous_key, message_type_id, deliver_at, message_id);

  CREATE INDEX IF NOT EXISTS messages_by_instance
  ON queue.messages (handler_key, instance_id)
  WHERE handler_key IS NOT NULL;

  CREATE TABLE IF NOT EXISTS queue.idempotency_keys (…);  -- as today

  -- queue.add_command(message_id, message_type_id, rendezvous_key, envelope,
  -- idem_key) RETURNS TABLE (actual_message_id uuid, enqueued boolean), as
  -- commandqueue.add today with the extra column.
  -- queue.add_deadline(message_id, message_type_id, rendezvous_key,
  -- handler_key, instance_id, envelope, scheduled_at) RETURNS void.
  -- queue.remove(message_id) RETURNS void, as commandqueue.remove today.
  ```

  `scheduled_at` is the original time: `enqueued_at` for a command, the
  deadline's scheduled time for a deadline. `deliver_at` is the earliest time
  the next attempt may run. It starts equal to `scheduled_at` and moves later
  on each postponement.

  The trigger function `queue.signal_change()` runs `FOR EACH STATEMENT`
  after insert, delete, and update of `rendezvous_key`, with both transition
  tables, and performs `pg_notify('signal', 'queue.' || rendezvous_key)` for
  each distinct key in either table.

  `90-migrate.sql` is a `DO` block. If `to_regclass('commandqueue.commands')`
  is not null, it inserts each row into `queue.messages` with
  `rendezvous_key = message_type_id`, `scheduled_at = enqueued_at` and
  `failing_since = CASE WHEN failures > 0 THEN deliver_at END`, moves
  `commandqueue.idempotency_keys` into `queue.idempotency_keys`, and drops
  the `commandqueue` schema. If
  `to_regclass('process.deadlines')` is not null, it inserts each row with
  `rendezvous_key = handler_key` and `scheduled_at = deliver_at`, and drops
  the table. The scheduled time is
  inside the protobuf envelope, which SQL can't read, so a deadline that had
  already failed before the upgrade keeps its next attempt time as its
  scheduled time. The changelog says so.

- [ ] **Step 1: Write the failing tests**

  ```go
  func TestCreate_migratesQueues(t *testing.T)
  // builds the v0.2.0 tables by hand (commandqueue.commands,
  // commandqueue.idempotency_keys, process.deadlines) with two commands, one
  // idempotency key and two deadlines, runs schema.Create twice, then checks:
  //   - queue.messages has four rows with the expected keys and times
  //   - queue.idempotency_keys has the key, pointing at its command
  //   - to_regclass('commandqueue.commands') and ('process.deadlines') are null
  //   - the commandqueue schema is gone

  func TestCreate_onFreshDatabase(t *testing.T)
  // schema.Create on a new database succeeds and creates queue.messages
  ```

  Extend `TestCreateAndDrop` so `Drop` removes the `queue` schema.

- [ ] **Step 2: Run `make test` to verify they fail**

- [ ] **Step 3: Write the DDL**

  `schema.Create` walks the embedded directory in name order, so
  `10-queue/90-migrate.sql` runs once the `queue` tables exist and before
  `20-process`. `20-process/00-tables.sql` no longer creates
  `process.deadlines`, and adds the binding column with `ALTER TABLE … ADD
  COLUMN IF NOT EXISTS rendezvous_key uuid`.

- [ ] **Step 4: Run `make test` to verify the schema tests pass**

  The engine tests fail until task 2.

- [ ] **Step 5: Stop for review**

---

### Task 2: Repoint producers, pumps and test helpers

**Files:**
- Modify: `execute.go:70-95`, `internal/process/commandqueue.go`,
  `internal/process/instancestate.go:161-220`
- Modify: `internal/messagepump/commandqueue.go`,
  `internal/process/deadlinepump.go:40-130,255-270`
- Modify: `internal/x/xtesting/commandqueue.go`,
  `internal/x/xtesting/deadline.go`
- Modify: `internal/messagepump/delivery.go` (add `RendezvousKey`)
- Test: existing tests in `internal/aggregate`, `internal/integration`,
  `internal/process`, and the root package

**Interfaces:**
- Produces: every producer passes a rendezvous key. In this task it is the
  node ID, through a new `NodeID *uuidpb.UUID` field on `process.Sink`,
  `process.DeadlinePump` and the integration pump, and `e.nodeID` in
  `execute.go`. Task 4 replaces this with `RendezvousKeyPolicy`.
- Consumes: the `queue` schema from task 1.

- [ ] **Step 1: Repoint the SQL**

  `commandqueue.add` → `queue.add_command`. `persistDeadlines` →
  `queue.add_deadline`. `endInstance` deletes from `queue.messages` by
  handler and instance. `AcquireCommandDelivery` reads `queue.messages WHERE
  handler_key IS NULL AND message_type_id = ANY($1) AND deliver_at <=
  clock_timestamp()`. The deadline pump reads `WHERE handler_key = $1`,
  joining `process.instances` as today. Both postponements set `deliver_at`,
  bump `failures`, and set `failing_since = COALESCE(failing_since,
  clock_timestamp())`. Successful removal is `queue.remove`.

- [ ] **Step 2: Update the test helpers**

  The `xtesting` helpers query `queue.messages`. `EnqueuePostponedCommand`
  inserts with a rendezvous key argument.

- [ ] **Step 3: Run `make test` to verify everything passes**

  No new behavior; the existing suites are the test.

- [ ] **Step 4: Update `CHANGELOG.md`**

  Under `### Changed`: the schema change, that `CreateSchema()` moves pending
  commands and deadlines into the new queue and must run while every node is
  stopped, and that a previously failed deadline's original scheduled time is
  taken from its next attempt time.

- [ ] **Step 5: Stop for review**

---

### Task 3: `queue.RendezvousKeys`

**Files:**
- Create: `internal/queue/doc.go`, `internal/queue/rendezvouskeys.go`
- Modify: `engine.go` (build and run it)
- Test: `internal/queue/rendezvouskeys_test.go`

**Interfaces:**
- Produces:

  ```go
  // RendezvousKeys is the set of rendezvous keys present in the queue, as
  // this node has observed it.
  //
  // It hears each rendezvous key as messages are queued under it, and
  // rebuilds itself from the database whenever the signal bus recovers, so it
  // can be stale by at most the signals missed since then.
  type RendezvousKeys struct {
      DB           *sql.DB
      Signals      *signal.Bus
      EngineLogger *slog.Logger
      // unexported: cosyne.Observable[*uuidcollection.Set]
  }

  // Observe returns an observable of the set. The set must not be modified.
  func (k *RendezvousKeys) Observe() cosyne.ObservableR[*uuidcollection.Set]

  // Run watches the queue until ctx is canceled. It must be called at most
  // once.
  func (k *RendezvousKeys) Run(ctx context.Context) error
  ```

  Each published set is a new value, so readers never see it change under
  them. A rendezvous key is never removed except by a rebuild.

- Consumes: the trigger from task 1.

- [ ] **Step 1: Write the failing tests**

  ```go
  func TestRendezvousKeys(t *testing.T) {
      t.Run("it reads the rendezvous keys that are already queued", ...)   // rows inserted before Run
      t.Run("it hears a rendezvous key as a message is queued under it", ...)
      t.Run("it hears a rendezvous key when a message's key changes", ...)   // UPDATE … SET rendezvous_key
      t.Run("it ignores a subtopic that is not a UUID", ...)        // Signals.Send("queue.garbage"); set unchanged
      t.Run("it forgets a rendezvous key with no messages when it rebuilds", ...)
  }
  ```

  Insert rows with a small SQL helper in the test; `xtesting` gains nothing
  here.

- [ ] **Step 2: Run `make test` to verify they fail**

- [ ] **Step 3: Implement `RendezvousKeys`**

  `Run` wraps `supervisor.Run(ctx, EngineLogger, "observe rendezvous keys",
  …)`. The body watches `queue` with a capacity of, say, 64. On `""` it
  rebuilds with `SELECT DISTINCT rendezvous_key FROM queue.messages`. On a
  rendezvous key it parses it, logs a WARN "ignored invalid rendezvous key in
  queue signal" with the subtopic if it doesn't parse, and otherwise adds it
  if absent. Publish only when the set changed.

- [ ] **Step 4: Wire it in `engine.go`**

  `e.rendezvousKeys = &queue.RendezvousKeys{…}` next to the node, run in the
  same group.

- [ ] **Step 5: Run `make test` to verify they pass**
- [ ] **Step 6: Stop for review**

---

### Task 4: `queue.RendezvousKeyPolicy` and process bindings

**Files:**
- Create: `internal/queue/rendezvouskeypolicy.go`
- Modify: `internal/process/instancestate.go`, `internal/process/sink.go`,
  `internal/process/deadlinepump.go`, `internal/process/commandqueue.go`,
  `internal/integration/commandpump.go`, `execute.go`, `engine.go`
- Test: `internal/process/instancestate_test.go`

**Interfaces:**
- Produces:

  ```go
  // RendezvousKeyPolicy chooses the rendezvous key that each newly queued
  // message is queued under.
  //
  // It decides the initial key only. A stored key can be changed at any
  // time, and later messages follow the stored key.
  type RendezvousKeyPolicy struct {
      NodeID *uuidpb.UUID
  }

  // ForCommand returns the rendezvous key that the given command is queued
  // under.
  func (p *RendezvousKeyPolicy) ForCommand(command dogma.Command) *uuidpb.UUID

  // ForProcessInstance returns the rendezvous key that a new process
  // instance's deadlines are queued under.
  func (p *RendezvousKeyPolicy) ForProcessInstance() *uuidpb.UUID
  ```

  Both return `NodeID`. The parameter is there so that the follow-up adding
  routing changes no caller.

  In `process`, `loadInstance` returns the binding as well. It selects
  `rendezvous_key` in its `RETURNING`, and when it is null, asks the policy
  for one and updates the row before returning. `persistDeadlines` takes the
  binding as the rendezvous key. Every producer from task 2 takes a
  `RendezvousKeyPolicy *queue.RendezvousKeyPolicy` field in place of its
  `NodeID` field.

- Consumes: nothing new. The type has no tests of its own.

- [ ] **Step 1: Write the failing tests**

  In `instancestate_test.go`:

  ```go
  t.Run("it binds a new instance when it is first stored", ...)             // row has a non-null rendezvous_key after the first event
  t.Run("it binds an existing instance when it is next loaded", ...)         // insert a row with NULL by hand; deliver an event; now non-null
  t.Run("it queues a deadline under the instance's binding", ...)
  t.Run("it keeps a binding that was changed by hand", ...)                  // UPDATE the column; next deadline uses the new key
  ```

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement the policy and the binding in `loadInstance`**
- [ ] **Step 4: Wire the policy in `engine.go`**
- [ ] **Step 5: Run `make test` to verify they pass**
- [ ] **Step 6: Stop for review**

---

### Task 5: `Membership.ResponsibleNode`

**Files:**
- Modify: `internal/cluster/membership.go`
- Test: `internal/cluster/membership_test.go` (new), `node_test.go`

**Interfaces:**
- Produces:

  ```go
  // ResponsibleNode returns the ID of the live node responsible for the given
  // rendezvous key, or nil if no node is live.
  //
  // A key equal to a live node's ID belongs to that node. Any other key
  // belongs to the live node with the highest score for it.
  func (m *Membership) ResponsibleNode(key *uuidpb.UUID) *uuidpb.UUID
  ```

  `LocalNodeIsResponsibleFor(key)` becomes a wrapper: false unless
  `m.isLive`, otherwise whether `ResponsibleNode(key)` is the local node. It
  keeps its rule that a node that can't hear itself is responsible for
  nothing.

- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests**

  ```go
  func TestMembership(t *testing.T) {
      t.Run("it makes the node whose ID is the key responsible for it", ...)
      t.Run("it scores a key normally when no live node has that ID", ...)
      t.Run("it names the same responsible node on every node", ...)   // two nodes; ResponsibleNode agrees with each node's LocalNodeIsResponsibleFor
      t.Run("it names no responsible node when no node is live", ...)
  }
  ```

  Keep the existing responsibility tests in `node_test.go` as they are.

- [ ] **Step 2: Run `make test` to verify they fail**
- [ ] **Step 3: Implement it**
- [ ] **Step 4: Run `make test` to verify they pass**
- [ ] **Step 5: Stop for review**

---

## Follow-ups once the queue lands

Each is a small change on its own, in this order:

1. **Capacity scores.** A shared per-node semaphore samples its occupancy and
   waiting time over each heartbeat interval. The heartbeat gains a payload
   with the score, on its own `heartbeat` channel, and `cluster.Node` listens
   on its own connection instead of through `signal.Bus`. `Membership` gains
   `Score(nodeID)` and `LiveNodes()`. This is a **[BC]** transport change.
2. **Score-based process binding allocation.**
   `RendezvousKeyPolicy.ForProcessInstance` implements the six-step algorithm
   from [#100][100] using `queue.RendezvousKeys` and the scores.
3. **Producer-side command routing.** `RendezvousKeyPolicy.ForCommand` routes
   the command to its aggregate instance's stream, or an integration
   handler's key when it minimizes concurrency.
4. **`WithQueueConcurrencyLimit`.**
5. **Tuning.** Measure the 200 ms lock timeout, the 200 ms contention delay
   and the `10 × GOMAXPROCS` limit against representative load: successful
   lock waits, postponement rate, slot and connection occupancy, and
   delivery latency.
6. **Garbage collection** of queued rows whose message types are no longer
   routed, which discovery never reads.
7. **Drop the pre-release migrations** from `CreateSchema()`, this plan's
   `90-migrate.sql` and the checkpoint-constraint one, with their tests, once
   no v0.2.0 database is left, and before 1.0.

## Out of scope

- Discovery, admission and claiming. The consumer plan reads
  `queue.RendezvousKeys` and the index from task 1.
- Changing a stored binding or key automatically.
- Score-based binding allocation and producer-side command routing. They are
  follow-ups above.
- Writing `failing_since` from a streak. The deliveries plan does that; task 2
  only keeps it consistent with `failures`.

<!-- references -->

[100]: https://github.com/dogmatiq/runkit/issues/100
[logging guide]: ../maintainers/logging.md
