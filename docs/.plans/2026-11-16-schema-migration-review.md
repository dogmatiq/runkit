# Review of the schema migration implementation

## Context

This document records a review of the migration implementation on the
`schema-versions` branch, made on 2026-11-16 before the work was committed. It
describes the code as it was on that day. The implementation belongs to the
[schema versions plan], as the task that adds the migration machinery, the
migrations to schema versions v1.1.0 and v2.0.0, and the public functions
`ApplySchemaOnline()` and `ApplySchemaOffline()`. The code is in
`internal/schema` and the packages under it, and in `schema.go` at the root of
the repository.

The review was limited to problems that meet these criteria — a bug or a
robustness gap that lets a poorly considered call to one of those two functions,
or a poorly written migration, lose data or leave the database inoperable during
or after a migration.

This document lists seven such problems. It leaves out anything for which those
criteria couldn't be justified. Each finding says what goes wrong, how it meets
the criteria, how it was reproduced, and what would fix it. Where a test showed
the damage, the finding says so. The fixes are suggestions. Where one has been
decided, the finding says what was decided.

The findings are in the order in which they matter.

- Findings 1 to 3 are about the upgrade to schema version v2.0.0, which is part
  of this release.
- Finding 4 is about a problem that can't happen in this release. A guard
  against it has to be in the engines released now.
- Findings 5 and 6 are about the code that runs migrations. Nothing triggers
  those problems until a migration has a statement step that builds an index, or
  a transaction step that contains `COMMIT` or `ROLLBACK`.
- Finding 7 is about a missing test, which lets a poorly written change to the
  schema be released.

## The assumption about old databases

It is assumed here that runkit must upgrade an old database, which is what the
migrations to schema versions v1.1.0 and v2.0.0 do. The alternative is to drop
that upgrade, and to say that runkit doesn't upgrade an old database
automatically. Each finding ends by saying what that alternative would change.

- Finding 1 would be irrelevant.
- Finding 2 would still apply, to the rollback of any later upgrade that raises
  the major number.
- Finding 3 would lose the only case in which it does damage.
- Findings 4, 5 and 6 would be unchanged.
- Finding 7 would matter more.

## Terms

- **Schema version** — the database schema's own number, such as `v2.0.0`. The
  database records it in the table `runkit.schema_version`. A change that raises
  its major number can't be applied while engines built for the earlier major
  number are running.
- **runkit version** — the number of the Go module, such as runkit v0.3.0.
- **Target schema version** — the schema version that this runkit version
  creates and requires, which is v2.0.0.
- **DDL** — the SQL statements that define a schema, such as `CREATE TABLE`.
- **Old engine** — an engine built with runkit v0.2.0 or earlier. Those runkit
  versions don't record a schema version. They create and update their schema
  with `CreateSchema()`, which runs all of its DDL each time it is called.
- **Old database** — a database created by an old engine. It is at schema
  version v1.0.0 or v1.1.0, and holds no record of that.
- **`Apply`** — the function in `internal/schema/schema.go` that does the work
  of both public functions. Its `offline` argument is false for
  `ApplySchemaOnline()`, which applies only changes that are safe while engines
  are running. It is true for `ApplySchemaOffline()`, which also applies a
  change that raises the major number. The caller of `ApplySchemaOffline()` must
  stop every engine first, and nothing checks that.
- **Step** — one file in a migration's directory. A **transaction step** is a
  file with a name that ends in `.tx.sql`. Its statements run in one
  transaction, together with the update that records the step as committed. A
  **statement step** is a file with a name that ends in `.stmt.sql`. It holds
  one statement, which runs outside any transaction and is recorded afterwards.
- **The runner** — the `Runner` type in
  `internal/schema/internal/migrations/runner.go`. Its `Run` method applies the
  steps. This document calls that method the runner's `Run`, to keep it apart
  from the engine's `Run`, which runs an engine.
- **`current/`** — the directory `internal/schema/internal/current`, which holds
  the schema as it is at the target schema version. In its last transaction,
  after the migrations, `Apply` runs every file in it and records the target
  schema version.
- **The upgrade test** — `TestMigrate_fromReleased` in
  `internal/schema/schema_test.go`. It creates the schema of runkit v0.1.2 and
  of runkit v0.2.0, upgrades each, and compares the result with a new database.

## Findings

### 1. An old engine that restarts creates its tables again

**What goes wrong.** The migration to schema version v2.0.0 moves the pending
commands and deadlines into `messagequeue.messages`, and drops
`commandqueue.commands` and `process.deadlines`. Every old engine must be
stopped before `ApplySchemaOffline()` is called. The migration guide
`docs/MIGRATING-v0.3.0.md` said that an old engine that is still running "fails"
once those tables are dropped. That holds only until the old engine restarts.
`CreateSchema()` in runkit v0.2.0 and earlier uses `IF NOT EXISTS` and
`OR REPLACE` throughout, so it succeeds on a migrated database and creates the
old tables again. An application that calls it at startup then has every table
and function that its engine uses, and nothing reports that the database was
migrated.

**How it meets the criteria.** The poorly considered call is an
`ApplySchemaOffline()` that is made while an old engine is still running. Once
that engine restarts, three kinds of damage follow, and a test showed the
condition behind each.

- **Messages are never delivered.** The old engine adds its commands and
  deadlines to the tables that it created again. No engine of this runkit
  version reads those tables, and the migration doesn't run a second time.
  Whatever is in them when the old engine is finally stopped stays there.
- **A command is handled twice.** The old engine accepts a command with an
  idempotency key that was used before the migration.
- **Engines of this runkit version stop learning of new event streams.** This
  happens with runkit v0.2.0 only, because its `CreateSchema()` replaces the
  function that sends the signal for a new event stream.

**Reproduction.** A test applied the DDL of runkit v0.2.0, and of runkit v0.1.2,
to a migrated database in the way that their `CreateSchema()` does. Both
succeeded. Afterwards:

- `commandqueue.commands` and `process.deadlines` existed again, empty.
- `commandqueue.add()` accepted a command with an idempotency key that was
  already in `messagequeue.idempotency_keys`. A caller gives a command an
  idempotency key so that a second command with the same key is discarded.
- `ApplySchemaOnline()` and `ApplySchemaOffline()` then returned nil. The
  command that `commandqueue.add()` had added was still in
  `commandqueue.commands`.
- With runkit v0.2.0, creating an event stream sent the signal
  `eventstream.create.` followed by the ID of the event stream. Before the old
  DDL was applied, it sent `eventstream.create`. An engine of this runkit
  version waits for `eventstream.create` only, in `discover` in
  `internal/eventstream/source/local.go`. It then doesn't learn of a new event
  stream until it restarts or its listening connection reconnects.

**Why it is limited to old engines.** The problem exists only because an old
engine has no schema version to check. An engine of this runkit version, or of a
later one, is refused in the same situation. If it starts on a database with a
schema version that has a higher major number, `checkVersion` returns the error
"incompatible schema version" before `Apply` takes its lock, and nothing is
changed. `TestApply_databaseWithNewerMajorNumber` covers that.

Two conditions apply. The engine has to make the check. Today that means that
the application calls `ApplySchemaOnline()` or `ApplySchemaOffline()` at
startup, until the plan's task that makes the engine's `Run` apply the schema is
done. And the migration has to have finished, which is the subject of finding 4.

**Suggested fix.** Have the migration to v2.0.0 leave a view named
`commandqueue.commands`. The old DDL skips
`CREATE TABLE IF NOT EXISTS commandqueue.commands`, because a relation of that
name exists, and then fails at
`CREATE INDEX IF NOT EXISTS ... ON commandqueue.commands`, because a view can't
have an index.

A test created such a view on a migrated database. `CreateSchema()` of runkit
v0.1.0, v0.1.2 and v0.2.0 then failed with "cannot create index on relation".
Its transaction rolled back, so nothing was created. An old engine adds a
command by calling `commandqueue.add()`. That call failed too, because the
function didn't exist.

The cost is that the `commandqueue` schema stays. `current/` has to create it
too, because the upgrade test compares a migrated database with a new one. The
view matters only while old engines are in use, so a later migration can drop
it.

The cheaper course is to leave the code alone, and to say in the migration guide
that nothing stops an old engine from starting.

**Decision.** The cheaper course was taken. The migration guide was then
replaced by the README's section on [major schema upgrades], which applies to
every upgrade that raises the major number. It says to keep engines from earlier
runkit versions stopped, and that one from runkit v0.2.0 or earlier never
refuses to start.

**If the upgrade of old databases is dropped.** This finding would be
irrelevant, because no database would be migrated from an old schema. An old
engine that is started against a database created by this runkit version would
still create its tables there, but no migration and no call to
`ApplySchemaOffline()` leads to that.

### 2. The rollback steps leave the recorded schema version behind

**What goes wrong.** To roll back, the migration guide had the operator run
`DROP SCHEMA messagequeue CASCADE` and then [`pg_restore`][pg_restore] with
`--clean`. That option drops only the objects that are in the backup. The
upgrade also creates the `runkit` schema, and the backup doesn't contain it,
because the backup was taken before the upgrade. The restore brings back the
tables of schema version v1.1.0, and the table `runkit.schema_version` survives
it and still holds `v2.0.0`.

When the upgrade is tried again, `identifyCurrentVersion` returns the schema
version in that row without looking at the tables. `Apply` finds that the target
schema version is satisfied and returns nil.

**How it meets the criteria.** The call is the second `ApplySchemaOffline()`, on
a database that was rolled back as the guide described. It returns nil and does
nothing. The database is left without the tables that engines of this runkit
version use, and nothing reports why. No data is lost. The restored tables are
intact, and an old engine can still use them. A test showed this.

**Reproduction.** A test took a backup of a database that had the schema of
runkit v0.2.0 and one pending command. It upgraded the database, followed the
rollback steps, and called `Apply` again with `offline` true. `Apply` returned
nil and changed nothing. `commandqueue.commands` was still there, with its
command, and `messagequeue.messages` wasn't.

The same `pg_restore` also reported six errors and exited with status 1. The new
`aggregate.complete_with_events` function uses the `eventstream.event` type. So
`pg_restore` couldn't drop the type, the `eventstream` schema or the `aggregate`
schema, and then couldn't create them.

**Suggested fix.** Have the guide drop every schema of the engine before the
restore, `runkit` included:

```sql
DROP SCHEMA IF EXISTS runkit, messagequeue, application, eventstream,
    aggregate, integration, process, projection CASCADE;
```

With that statement run first, the restore of a backup from runkit v0.2.0, and
of one from runkit v0.1.2, reported no errors. The old `CreateSchema()` then
succeeded, and upgrading again moved the restored command into
`messagequeue.messages`. The list of schemas changes when the engine's schemas
are collapsed into one.

**Decision.** The README's section on [major schema upgrades] says to restore
into an empty database, or to drop the engine's schemas before a restore.

**If the upgrade of old databases is dropped.** This finding would still apply,
because the same rollback follows any upgrade that raises the major number.

### 3. The copy in the migration to v2.0.0 relies on the default isolation level

**What goes wrong.** The migration to v2.0.0 locks the old tables and then
copies their rows. Its comment says that the lock waits for the transactions
that are already using the tables, so that each copy sees their results. That is
true only at the READ COMMITTED [isolation level], where each statement sees the
rows that were committed when the statement started. At REPEATABLE READ and
SERIALIZABLE, every statement of a transaction sees the rows as they were when
the transaction's first query started.

`Transact` in `internal/x/xsql/tx.go` begins the transaction with no options.
Each step therefore runs at the default isolation level of the database or the
role, which is set by
[`default_transaction_isolation`][default isolation level]. The first query of a
step's transaction is the runner's own `set_config` query, in `Runner.Transact`,
which runs before the lock. At either of the stricter levels, the copies miss
whatever was committed while the migration waited for its lock.

**How it meets the criteria.** The poorly considered call is an
`ApplySchemaOffline()` that is made while an old engine is still running, on a
database with a default isolation level of REPEATABLE READ or SERIALIZABLE. A
command that the old engine adds while the migration waits for its lock is lost.
A command that the old engine delivers in that time is delivered again. A test
showed both.

**Reproduction.** With `default_transaction_isolation` set to `repeatable read`
on the database, and again with it set to `serializable`, a test committed a
transaction while the migration waited for its lock, as an old engine that is
still running would. It did so in two cases, with the same result at both
levels:

- In the first case, the transaction had added a command. An old engine would
  have told its caller that the command was accepted. The command wasn't copied,
  and was dropped with the old table.
- In the second case, the transaction had deleted a command, as an old engine
  does when it has delivered one. The command was copied, so it would be
  delivered again.

At `read committed`, both cases gave the right result.

**Suggested fix.** Have `Runner.Transact` set the isolation level itself, so
that every migration can rely on READ COMMITTED. With
`SET TRANSACTION ISOLATION LEVEL READ COMMITTED` as its first statement, both
cases gave the right result at all three levels.

**If the upgrade of old databases is dropped.** The copy would go, along with
the migration to v2.0.0, and this finding would have no case left. Steps would
still run at the default isolation level. That would matter again for a later
migration that locks rows or tables and then reads them.

**Decision.** Every transaction that the engine begins now uses READ COMMITTED,
through the options in `xsql.TxOptions`. That covers each migration transaction,
so `Runner.Transact` needs no change of its own. With that change, both cases
gave the right result at all three levels.

### 4. An engine can start while a migration raises the major number

**What goes wrong.** The `version` column of `runkit.schema_version` doesn't
change until every migration has finished, and `identifyCurrentVersion` reads
nothing else. The columns `committed_migration` and `committed_steps` show that
a migration is in progress, but only the runner reads them.

So the recorded schema version is still v2.0.0 while a later runkit version
applies a migration to, say, schema version v3.0.0, and after such a migration
fails part-way. An engine built for schema version v2.0.0 that starts at that
point gets nil from `ApplySchemaOnline()`, and runs on a schema that is only
partly migrated. The check planned for the engine's `Run` reads the same value.

This release can't cause it, because an old engine doesn't read the schema
version. But the engines that will need the check are the ones released now.
Adding it in the release that raises the major number is too late for them.

**How it meets the criteria.** The poorly considered call is an
`ApplySchemaOnline()` by an engine that starts while a migration that raises the
major number is in progress, or after such a migration has failed part-way. What
is lost depends on the migration. A test showed a command lost with a migration
of two steps, where the first step copies a table and the second drops it.

**Reproduction.** A test ran a migration of the kind that a later runkit version
might have. The migration, to schema version v3.0.0, had two steps. The first
copied `messagequeue.messages` into a new table, and the second dropped
`messagequeue.messages`.

- The second step was made to fail on the first run. The recorded schema version
  was then still `v2.0.0`, with one step of the migration to v3.0.0 committed.
- `Apply` returned nil for an engine built for schema version v2.0.0, with
  `offline` false and with it true.
- A call to `messagequeue.add_command()` then succeeded, as it would for that
  engine.
- The corrected migration resumed at its second step and dropped the old table.
  The command wasn't in the new table.

**Suggested fix.** Have `identifyCurrentVersion` also read
`committed_migration`, and have `checkVersion` treat the database as
incompatible when that migration has a higher major number than the target
schema version. A migration in progress that raises only the minor or patch
number must not stop an engine, because such a change is safe while engines are
running.

**If the upgrade of old databases is dropped.** This finding would be unchanged.

**Decision.** The suggested fix was made. `identifyCurrentVersion`, since
renamed `identifySchemaVersions`, also reads `committed_migration`, and
`checkVersion`, since renamed `needsUpgrade`, refuses the database when that
migration has a higher major number than the target schema version. `Apply` also
refuses a database that it would have to upgrade, while a migration to a schema
version above the target is in progress. It decides that once it holds the lock,
in the new `checkCanUpgrade`, because it has no migration to that schema version
and so can't finish the upgrade. In the reproduction, both calls to `Apply` then
returned an error. The columns `committed_migration` and `committed_steps` were
then renamed `migrating_to` and `migration_steps_completed`, so that their names
say that the migration is in progress.

### 5. A rerun accepts an invalid index left by an interrupted build

**What goes wrong.** [`CREATE INDEX CONCURRENTLY`][concurrent index build]
builds an index without blocking writes. It is how the plan has a statement step
add an index to an existing table. A build that is interrupted leaves an invalid
index — an index that exists under its name but is ignored by queries.
PostgreSQL's documentation says to drop such an index and build it again.

When a run of a statement step is interrupted, `applyStatement` in the runner
does nothing about the leftover on the next run. The plan says that such a step
uses `IF NOT EXISTS`, "after dropping an invalid leftover", but no step file can
do that. A statement step holds one statement, and a rerun skips each step that
committed. A separate step that drops the index therefore doesn't run again
before the second attempt to build it. With `IF NOT EXISTS`, the second attempt
finds the invalid index and skips the build, and the runner records the step.

Two things interrupt a build.

- **The call's context is canceled**, such as by a deadline that is shorter than
  the build.
- **A second call waits for the lock.** `Apply` holds an [advisory lock] while
  it changes the database. An advisory lock is a lock on a number. PostgreSQL
  holds it for a connection until the connection releases it or closes. A second
  call waits for that lock inside one statement, `SELECT pg_advisory_lock($1)`,
  in `lockDatabase`. Before a concurrent build finishes, it waits for every
  transaction in the database that started reading before the build's last scan
  of the table. A statement that waits in `pg_advisory_lock` is such a
  transaction. The building call holds the lock, so each of the two waits for
  the other. PostgreSQL detects such a deadlock after
  [`deadlock_timeout`][deadlock timeout], which is 1s by default, and cancels
  one of the two statements.

**How it meets the criteria.** The call is one with a deadline that is shorter
than the build, or a second call that is made while the first is building. The
second isn't poorly considered at all, because the doc comment of
`ApplySchemaOnline` says that it is safe to call from several engines at once.
The migration is written as the plan says.

The damage depends on the index. A test showed it for a unique index. Statements
that use `ON CONFLICT` on its column failed, and in one run duplicate values
were accepted. For an index that isn't unique, the cost is slower queries, which
doesn't meet the criteria. Without `IF NOT EXISTS`, every rerun fails until
someone drops the index, and the database stays usable at its old schema
version.

**Reproduction.** A test interrupted the build of a unique index with a context
deadline, and ran the runner again:

- With `IF NOT EXISTS`, the second run returned nil and recorded the step, and
  the index was still invalid. `CREATE UNIQUE INDEX IF NOT EXISTS`, which is
  what `current/` would run next, skipped it too. `INSERT ... ON CONFLICT (v)`
  then failed with "there is no unique or exclusion constraint matching the ON
  CONFLICT specification". In one run, the build was interrupted so early that
  PostgreSQL wasn't yet maintaining the index, and inserting duplicate values
  into the unique column succeeded.
- Without `IF NOT EXISTS`, the second run failed with "relation already exists".

A second test ran a statement step that builds an index, while a second
connection waited for the lock in the way that a second call to `Apply` does:

- When the second connection had waited for 1.5s before the build started, the
  build failed with "deadlock detected" and left an invalid index.
- When the second connection had waited for less than 1s, its wait failed with
  "deadlock detected", and the build finished.
- When the second connection polled `pg_try_advisory_lock` every 20ms instead,
  the build finished and nothing failed.

The upgrade test can't catch any of this, because nothing interrupts its builds.

**Suggested fix.** Two changes are needed.

- Before each statement step, have the runner drop every invalid index in the
  engine's schemas with `DROP INDEX CONCURRENTLY`. A step with `IF NOT EXISTS`
  then builds the index again. The other option leaves the drop to a person. The
  runner would fail, before a run and before the target schema version is
  recorded, if an index in the engine's schemas is invalid.
- Have `lockDatabase` poll `pg_try_advisory_lock` until it gets the lock or its
  context is canceled. Each attempt is a short statement, so no transaction
  stays open while the call waits, and a second call no longer interrupts a
  build.

**If the upgrade of old databases is dropped.** This finding would be unchanged.

**Decision.** The second fix was made. The lock moved into its own package,
`migrationlock`, whose `Acquire` polls `pg_try_advisory_lock`, so a call that
waits for the lock has no statement open. A test there builds an index
concurrently on the connection that holds the lock while another connection
waits for it. With the blocking wait, that test failed with a deadlock. The
first fix was replaced. Once it holds the lock, an upgrade fails while any index
in the engine's schemas is invalid, and the error names each one. The upgrade
can't tell whether a step builds a given index again, so the error says that the
migration requires human intervention, and that each index needs to be dropped
or rebuilt manually. Dropping every invalid index automatically was rejected,
because it can drop an index that nothing builds again.

### 6. A transaction step can end its own transaction

**What goes wrong.** `apply` in the runner sends the SQL of a transaction step,
and then the update that records the step, through one `*sql.Tx`. It relies on
both being in its own transaction, and nothing checks that. A step file that
contains `COMMIT` or `ROLLBACK` ends that transaction early. That includes a
file that is wrapped in `BEGIN` and `COMMIT`, which is a common way to write a
SQL file. PostgreSQL only warns, and nothing reports the warning.

**How it meets the criteria.** The poorly written migration is a transaction
step that contains `COMMIT` or `ROLLBACK`. A test showed two kinds of damage.

- **A step that can't be rerun.** If a statement after a `COMMIT` fails, the
  work before the `COMMIT` stays, and the step isn't recorded. Every rerun then
  fails at the work that is already there. If the migration raises the major
  number, the database is left partly migrated, and no rerun gets past the step.
- **A step recorded with part of its work undone.** If the step contains
  `ROLLBACK`, the runner records it and reports success. The schema then lacks
  whatever the step created before the `ROLLBACK`.

A step that is only wrapped in `BEGIN` and `COMMIT` wasn't shown to do damage.
It loses the lock timeout and the statement timeout after its `COMMIT`, because
the runner sets those for the transaction only, and its work is committed before
its record.

**Reproduction.** A test ran three such steps, with the runner's timeouts set to
5s and 100ms:

- `BEGIN; CREATE TABLE a ...; COMMIT;` and then another statement. The runner's
  `Run` returned nil. The statement after the `COMMIT` ran with `lock_timeout`
  and `statement_timeout` at 0.
- `CREATE TABLE a ...; COMMIT;` and then a statement that fails. Table `a`
  stayed and the step wasn't recorded, so the rerun failed with "relation
  already exists".
- `CREATE TABLE a ...; ROLLBACK; CREATE TABLE b ...;`. The runner's `Run`
  returned nil and recorded the step, and table `a` didn't exist.

**Suggested fix.** Have `apply` read `pg_current_xact_id()` before and after the
step's SQL, and fail if the two differ. With that check, the first and third
steps returned an error and recorded nothing, and the runner's own tests still
passed. The upgrade test applies every migration, so a step like these could
then not be released.

**If the upgrade of old databases is dropped.** The problem would be unchanged.
The upgrade test would have no released schema to start from until this release
exists, so it would have to be rebuilt before it could catch such a step.

### 7. No test applies `current/` to a database that already has it

**What goes wrong.** `Apply` runs `current/` whenever it upgrades a database, so
each statement in it has to work on a database that already has the object. No
test checks that. The second call in `TestApplyAndDrop` has the comment
"Migrating again verifies idempotency", but it returns before it takes the lock,
because the database is at the target schema version. No other test applies
`current/` to a database created by `current/`. A statement that can't run twice
therefore passes every test.

**How it meets the criteria.** The poorly written migration is a change to
`current/` that adds a statement that can't run twice. The upgrade that adds the
statement works. The next upgrade fails on every database, in the last
transaction of `Apply`.

- If that next upgrade raises the major number, its migration steps have
  committed by then, and no engine of either runkit version can run. A test
  showed this.
- If it raises only the minor or patch number, the upgrade fails and the
  database stays usable at its old schema version. That doesn't meet the
  criteria. A test showed this for the patch number.

**Reproduction.** In a copy of the tree,
`CREATE TYPE messagequeue.review_probe AS (a int);` was added to a file in
`current/`. Every test in `internal/schema` passed.

In the same copy, a migration to schema version v3.0.0 was added, which drops
the function `messagequeue.add_command()`. A test then called `Apply` with the
target schema version set to v2.0.0, as the release that adds the statement
would, and then with it set to v3.0.0, as the next release would.

- With the target at v3.0.0, `Apply` failed with
  `type "review_probe" already exists`, on each of two calls. Its migration step
  had committed.
- With the target back at v2.0.0, `Apply` returned nil. An engine built for
  schema version v2.0.0 adds a command by calling `messagequeue.add_command()`.
  That call failed, because the function didn't exist.

With two targets that differ only in the patch number, the second `Apply` failed
with the same error. The recorded schema version didn't change, and
`messagequeue.add_command()` still worked.

**Suggested fix.** Add a test that applies `current/` to a database that already
has it.

**If the upgrade of old databases is dropped.** This finding would matter more.
The upgrade test now applies `current/` to the tables and functions that an old
database already has, which covers part of the gap. Without old databases, the
upgrade test has no released schema to start from, and no test at all would
apply `current/` to an existing database.

## How the scenarios were run

Each scenario was a throwaway test in `internal/schema` or in
`internal/schema/internal/migrations`, run against the PostgreSQL 18 container
of the test suite. The tests aren't in the repository. A scenario that starts
from an old database used `createSchemaOf` in `internal/schema/schema_test.go`,
which applies the DDL of a released runkit version. A scenario about the runner
passed it hand-written steps, as `TestRunner` does.

The fixes for findings 3 and 6 were tried in a copy of the tree. So were the
statement and the migration to schema version v3.0.0 in finding 7. The view in
finding 1, the `DROP SCHEMA` statement in finding 2 and the polling in finding 5
were tried from within their tests, without a change to the code.

<!-- references -->

[advisory lock]: https://www.postgresql.org/docs/18/explicit-locking.html#ADVISORY-LOCKS
[concurrent index build]: https://www.postgresql.org/docs/18/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY
[deadlock timeout]: https://www.postgresql.org/docs/18/runtime-config-locks.html#GUC-DEADLOCK-TIMEOUT
[default isolation level]: https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-ISOLATION
[isolation level]: https://www.postgresql.org/docs/18/transaction-iso.html
[major schema upgrades]: ../../README.md#major-schema-upgrades
[pg_restore]: https://www.postgresql.org/docs/18/app-pgrestore.html
[schema versions plan]: 2026-11-15-schema-versions.md
