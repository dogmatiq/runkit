# Plan: schema versions, migrations and `Run` applying the schema (#103, closes #96)

## Where the work stands

All work is on the branch `schema-versions`, which has no pull request and no
upstream yet; nothing has landed on main. The first two tasks are done, and a
review of the migration machinery followed them. Its report is
[2026-11-16-schema-migration-review.md](2026-11-16-schema-migration-review.md),
and every finding in it has a decision, made in the commits listed under the
second task. The automatic schema migration flowchart and `Run` applying the
schema landed together in e30c230. Collapsing the engine's schemas into one
landed in d338872. Flattening `current/` landed in 2104575. The maintainer
document is written and awaits review; the next task after it is the operational
documentation in the README.

## Context

Table changes are hand-written guards that pile up in `internal/schema/ddl`
(the foreign-key `DO` block, `ADD COLUMN IF NOT EXISTS`, `DROP FUNCTION IF
EXISTS`, `90-migrate.sql`). #103 asks for numbered migrations recorded in a
ledger. The design that came out of review goes further. The schema has its own
semver version, which says which runkit versions can run on a database. A deploy
that needs no human decision needs no human step, so `Run` applies safe schema
changes itself. A change that does need a human, one that raises the major
number of the schema version, can only be applied by a function whose name says
so.

## Words

All approved in review. Checked against Dogma's glossary: none of "migration",
"schema" or "version" is a Dogma term, and "engine" is used in Dogma's sense.

- **runkit version** is the Go module's number. New prose (docs, comments,
  errors and changelog entries) says "runkit version" for the generic term,
  never a bare "version". A specific one is written as its tag, "runkit v0.5.0",
  because the `v` already reads as "version". Existing uses stay: the tag
  message `Version x.y.z`, the commit message `Bump version to`, the
  changelog's section headings, the release and changelog skills, and the
  sentences already in the README that this plan doesn't otherwise change.
- **schema version** is the database schema's own number, written with the `v`
  prefix as well: `v2.0.0`. It is told apart from a runkit version by the words,
  not by the number: schema code, docs, errors and the database say "schema
  version" whenever they mean the database's, never a bare "version".
- Commit messages and PR descriptions keep their usual wording, with "schema
  version" only where it means the schema's.
- **Names:** type `version.Version` in `internal/schema/internal/version`;
  variable `schema.TargetVersion`; method `CanRunOn`; table
  `runkit.schema_version`, with the columns `migrating_to` and
  `migration_steps_completed`; directory `migrations/`; public functions
  `ApplySchemaOnline()` and `ApplySchemaOffline()`; internal `schema.Apply`.
  `CreateSchema()` is removed, and `DropSchema()` stays.
- **step** is one file in a migration's directory. Its file name starts with a
  two-digit number, which is unique within the migration and orders its steps,
  such as `10-add-queues.tx.sql`. A step whose file name has the suffix `tx`
  before `.sql` holds statements that run together in one transaction. A step
  with the suffix `stmt` holds one statement, which runs on its own outside any
  transaction.

## Settled design

- **Schema version.** Plain semver with a `v` prefix, such as `v2.0.0`, and no
  build metadata. `schema.TargetVersion` is the schema version of this runkit
  version; this release is `v2.0.0`. The pairing of schema versions with runkit
  versions lives in the changelog sections.
- **Meaning.** A higher major number: engines built for an earlier major number
  can't run, and the deploy needs a human to stop them first. A higher minor
  number: additive, and older engines keep running. A higher patch number:
  nothing an engine can observe.
- **Rule.** `schema.TargetVersion.CanRunOn(databaseSchemaVersion)`: the major
  numbers are equal and the database's minor number is at least the engine's;
  the patch number is ignored. Doc sentence: "CanRunOn reports whether the
  runkit version with this schema version can run on a database at the given
  schema version."
- **The database's schema version.** Table `runkit.schema_version` holds one row
  (at most one, enforced by a unique index on the constant `true`) with
  `version` and `applied_at`: the schema version of the database and the time it
  was reached. They are set once, at the end of a successful migrate call. The
  columns `migrating_to` (a schema version) and `migration_steps_completed` (a
  count) are both NULL, or both set while a migration is in progress, to the
  schema version of that migration and how many of its steps have completed. Its
  table file lives in `internal/schema/internal/bootstrap/bootstrap.sql`, which
  the migrate code applies before it changes anything, so that the table exists.
  A database that has no row is at `v1.0.0` or `v1.1.0`, as the next item says,
  and the migrate call inserts the row, with that schema version and the time of
  the insert, before the first step.
- **Starting point.** A database with no row in `runkit.schema_version` is
  guessed from its tables. If it has `application.identity` it is at `v1.1.0`,
  the v0.2.0 schema, which added that table and the signal triggers. Otherwise,
  if it has `eventstream.streams`, it is at `v1.0.0`, the schema of v0.1.0,
  v0.1.1 and v0.1.2, which are identical. v0.1.0 is the oldest runkit version
  that upgrades directly. A database without `eventstream.streams` is new.
- **`current/` is the schema.** The directory `internal/schema/internal/current`
  (renamed from `ddl`) describes the complete schema as it is at
  `TargetVersion`, in plain SQL grouped by PostgreSQL schema, and is applied to
  every database that a migrate call changes. Doc sentence: "Each file under
  `current/` defines part of the database schema as it is at `TargetVersion`:
  its tables, functions and triggers." `migrations/` holds one directory for
  each schema version that has to change an existing database, named for the
  schema version it produces, such as `v2.0.0`. A schema version that only adds
  or replaces functions or triggers has no directory. A new, empty database gets
  `current/` only, with every migration skipped. Migrations run before
  `current/`, so a migration that fills a new table creates the table itself,
  and the drift test checks that the definition matches `current/`. `current/`
  keeps its `CREATE INDEX IF NOT EXISTS` with the same index names, so an index
  built by a step is found by `current/` and left alone.
- **Steps.** A migration's steps run in the order of their file names' numbers,
  whatever their kind, so `tx`, `stmt`, `stmt`, `tx` works. Each step file has a
  two-digit number that no other file in the migration has and exactly one of
  the suffixes, or the `schema` package panics when it loads the migrations. The
  runner knows nothing of files: it takes a list of steps and identifies a step
  by its position. A `tx` step sets `migrating_to` and
  `migration_steps_completed` in its own transaction, so the step and its record
  commit together. A `stmt` step is idempotent (for an index: `IF NOT EXISTS`),
  and sets them after it runs. An interrupted concurrent build leaves its index
  invalid, and `IF NOT EXISTS` would skip over it, so an upgrade fails while any
  index in the engine's schemas is invalid, and the error names each one. The
  upgrade can't tell whether a step builds a given index again, so a person
  decides whether to drop or rebuild each one. A rerun skips the completed steps
  of the recorded migration, in the order of migrations by schema version and
  then steps by position. A `stmt` step's doc comment says that it must be safe
  to run twice. Every transaction that the runner opens, including the bootstrap
  and the one that applies `current/`, fails a statement that ends it, such as
  `COMMIT` or `ROLLBACK`, by comparing `pg_current_xact_id()` after each
  statement. Every engine transaction, the runner's included, uses READ
  COMMITTED, through `xsql.TxOptions`.
- **Cleanup.** The last thing a successful migrate call does, in one
  transaction, is set `version` to `TargetVersion`, `applied_at` to the time,
  and `migrating_to` and `migration_steps_completed` to NULL. A crash before
  that leaves the count recorded and `version` unchanged, and the rerun skips
  every completed step and finishes. If an unreleased migration gains or loses a
  step between a failed run and its rerun, the count can skip the wrong steps;
  the unreleased-edit rule covers that risk.
- **A `stmt` step has one statement because PostgreSQL checks it.** The runner
  prepares the file's text and then executes the prepared statement; the
  extended query protocol rejects more than one statement. No SQL parsing in Go
  and no test per version: the upgrade test applies every migration against a
  real database. The first implementation work checks on PostgreSQL 18 that a
  prepared `CREATE INDEX CONCURRENTLY` and a prepared `DO` block that commits
  both run outside a transaction.
- **One lock.** A session-level advisory lock, with one distinctive 64-bit key
  in the package `internal/schema/internal/upgradelock`, is held on one
  connection for the whole migration, so that only one node
  migrates at a time. A call that waits for it polls `pg_try_advisory_lock`,
  because a statement that waited inside `pg_advisory_lock` would be an open
  transaction, and a concurrent index build in the call that holds the lock
  waits for every older transaction to end. It replaces the
  `LOCK TABLE pg_catalog.pg_namespace` that `Create` takes today, and needs no
  special privilege. `Apply` reads the schema version before it waits for the
  lock, so that a call with nothing to do doesn't wait, and reads it again once
  it holds the lock. It decides whether it can upgrade only then: before that,
  another call may be part-way through an upgrade that leaves it nothing to do.
  It closes its connection when it returns, instead of returning it to the
  pool, so that no change to the session reaches the engine's other queries.
- **Refusals.** Each error names runkit and gives the versions in one form, such
  as `runkit v0.3.0 (schema ^v2.0.0)`. `Apply` refuses a database whose schema
  version, or migration in progress, has a higher major number than the
  target; a migration in progress above the target, once it holds the lock; a
  migration in progress that this runkit version doesn't include, which may
  come from a hotfix to a prior runkit version; a recorded count of completed
  steps above the migration's steps; and, once it holds the lock, any invalid
  index in the engine's schemas. Once it holds the lock it checks, in this
  order: a migration in progress above the target, invalid indexes, and then
  an upgrade that raises the major number while online. The last is the only
  refusal that costs downtime to get past, so the problems that can be fixed
  while the application runs are reported before it. Each of these refusals
  from the `schema` package is its own exported error type, with the versions
  or index names as fields, so that code can tell them apart with
  `errors.AsType`. The runner's three distinct errors (a migration in progress
  that this runkit version doesn't include, invalid recorded progress, and a
  step that timed out online) are types too, defined in the `migrations`
  package. Nothing outside `internal/schema` can import that package, so when
  the engine needs one of them, give it a name in `schema` with a type alias.
- **Timeouts follow the entry point.** `ApplySchemaOnline()` and `Run()` assume
  that engines may be running: a `tx` step runs with a lock timeout and a
  statement timeout, and a `stmt` step runs with no timeout, because it is long
  by nature and doesn't block reads or writes. `ApplySchemaOffline()` applies no
  timeout to any step, since nobody is querying. That includes any
  `transaction_timeout` that the database or the role sets. A `stmt` step never
  has that timeout either. The values are constants in `engine.go`, passed to
  the `schema` package as required fields. A migration that can't get its lock
  in time fails and rolls back, and `Run` retries the apply with backoff instead
  of returning the error. When a statement timeout stops a step under
  `ApplySchemaOnline()` or `Run()`, the error says that the step takes longer
  than is allowed while the application is running, and to stop every instance
  of the application and apply the upgrade offline, so that a wrong guess about
  a step's cost becomes a clear human step and not an outage.
- **Allowed in a migration that raises only the minor or patch number**, because
  each either takes a brief lock at a cost that doesn't grow with the data, or
  doesn't block reads or writes: create a schema, table, function or trigger;
  add a column with no default or a constant default; create an index on a table
  that the same migration creates, with a plain `CREATE INDEX`; replace a
  function so that older engines still work; create an index on an existing
  table with `CREATE INDEX CONCURRENTLY` in a `stmt` step, and in no other way;
  add a constraint as `NOT VALID` in a `tx` step and validate it with
  `VALIDATE CONSTRAINT` in a `stmt` step. Anything else raises the major number,
  including dropping or renaming, changing a column's type, adding a constraint
  that scans the table, a plain index on an existing table, and moving or
  backfilling rows. The maintainer document replaces this list with one
  flowchart for each kind of object, which ends in major, minor, patch or "not
  covered", and is the reference from now on. Nothing checks this, so review is the only check, and the maintainer document
  states it. There is no list of tables that count as small.
- **Entry points.** The migrate routine takes one flag, `Offline`, which says
  whether engines are known to be stopped. It allows the major number to rise
  and removes the timeouts. The public API has no option.
  - `ApplySchemaOnline()`: creates the schema on an empty database and applies
    changes that raise only the minor or patch number. It returns an error
    instead of applying a change that raises the major number, and the error
    says to stop every instance of the application and apply the upgrade
    offline. An error that reaches an application's operator names runkit, but
    none of its functions. Doc sentence: "ApplySchemaOnline brings the
    database's schema to the schema version of this runkit version, creating the
    schema if the database doesn't have one yet. It applies only the changes
    that are safe to apply while engines are running."
  - `ApplySchemaOffline()`: does what `ApplySchemaOnline()` does and also
    applies a schema change that raises the major number. Nothing checks that no
    engine is running; the name and the documentation say so. Doc sentence:
    "ApplySchemaOffline does what ApplySchemaOnline does and also applies a
    schema change that raises the major number of the schema version. Stop every
    engine that uses the database before calling it; the database itself must be
    running."
  - `Run()`: reads the database's schema version at start. Satisfied: start with
    no DDL. Behind within the same major number, or empty: apply as
    `ApplySchemaOnline()` does. If the apply fails and the engine can run
    anyway (behind only in the patch number), log the failure with
    `engineLogger` and start; otherwise return the error. Behind in the major
    number: refuse, with the message above. Ahead in the major number: refuse,
    saying that the database's schema version is newer than this engine can run
    on and pointing to the changelog. The `schema` package returns errors, and
    the engine decides what to log.
  - A database whose schema version satisfies `TargetVersion` (the same major
    number, and at or above it) needs no change, which covers a newer compatible
    database. A higher major number is refused. Operators with strict DDL
    control call `ApplySchemaOnline()` from their pipeline first, and `Run` then
    never attempts DDL.
- **Invariants checked by the package itself.** When it loads the embedded
  migrations it panics if a directory name isn't a schema version of the form
  `vX.Y.Z` or repeats a core, or if a step lacks a suffix or a two-digit number,
  or two steps of a migration have the same number. Every test that migrates
  exercises this, so no test reaches into unexported state. `TestTargetVersion`
  checks that no migration is above `TargetVersion`. The runner lives with the
  migration files in the package `internal/schema/internal/migrations`, and
  takes a list of steps, so tests can use synthetic migrations, including a
  failing step that a rerun resumes.
- **Major numbers and the module.** A change that raises the major number
  comes with a **[BC]** changelog entry
  that names the engines to stop. Before 1.0 that is a minor runkit version;
  from 1.0 it is a major runkit version, with a `/v2` import path, and obsolete
  objects stay until then. This release needs every engine stopped.
- **Rolling restarts.** While the major number stays the same, engines can be
  replaced one at a time. The engine's SQL names its columns (checked: the one
  `SELECT *`, in `execute.go`, selects from an `unnest`), so an added column
  can't break an older engine's statements. The schema version covers only the
  database: cluster compatibility (heartbeats, signal channels, queue semantics,
  gRPC) is signalled by **[BC]**, not by the schema version.
- **Unreleased edits.** A migration for an unreleased schema version is edited
  in place. A database that already ran it keeps its recorded schema version and
  doesn't pick up the edit.
- **Changelog.** Every runkit version's section has the line
  `Schema version: v2.0.0 (new)` or `(existing)`, as the first paragraph of the
  section, before its `###` headings. A schema version whose upgrade can hold a
  rollout says so in its entry, in terms of the wait and never of step kinds.

## Tasks

Each task lands on main when approved, in the order below, and leaves main
releasable: the README and the changelog are true after every merge. v0.3.0 is
tagged only after all of them, #127 included. Each task ends with its changelog
entries and a passing `make precommit`, and is presented with the design bullets
it relies on. Tick a task off here with its pull request or commit when it is
done.

- [x] **The schema version type and the changelog line.** PR or commit: d122b5f
  - `internal/schema`: `Version` (parse, format, precedence, major, minor,
    patch, `CanRunOn`) and `TargetVersion`.
  - `CHANGELOG.md`: the "Schema version:" line in `[Unreleased]`.
  - Tests: `CanRunOn`; the changelog line (the topmost section's line equals
    `TargetVersion`, and says "(new)" only when the section below it differs,
    with sections before the line existed counting as `v1.0.0`).
- [x] **The migration machinery, the migration to schema version v2.0.0, and the
  public functions.** PR or commit: b332499
  - Checked on PostgreSQL 18.3: both prepared-statement cases run outside a
    transaction, and `lock_timeout` cancels a concurrent index build that waits
    for older transactions, leaving an invalid index. So a `stmt` step sets
    `lock_timeout` and `statement_timeout` to 0 on its connection. An invalid
    leftover stops the upgrade until a person drops or rebuilds it.
  - The `runkit.schema_version` table in
    `internal/schema/internal/bootstrap/bootstrap.sql`, which `schema.Apply`
    runs before it changes anything, so no migration can change it; the runner
    (`internal/schema/internal/migrations`, over a list of steps) with `tx` and
    `stmt` steps, the advisory lock, the timeouts, the cleanup and the
    invariants; `schema.Apply`.
  - Migration `v1.1.0`, one `tx` step: the v0.2.0 DDL for the
    `application.identity` table and the signal triggers, unchanged, so that a
    v0.1.x database reaches the v0.2.0 schema and every migration starts from
    exactly one schema version.
  - Migration `v2.0.0`, one `tx` step, unconditional because every database that
    reaches it is the v0.2.0 schema: create the `messagequeue` schema and
    tables, move commands and deadlines (the body of `90-migrate.sql`
    without its guards), drop `commandqueue` and `process.deadlines`, drop the
    checkpoint foreign key, add `process.instances.deadline_queue_id`, and drop
    the old aggregate and integration functions.
  - Rename `internal/schema/ddl` to `internal/schema/internal/current` (and the
    embed directive), then delete the hand-rolled guards: the `DO` block in
    `current/00-eventstream/00-tables.sql`, the `ALTER` in
    `current/20-process/00-tables.sql`, the `DROP FUNCTION` lines in
    `current/20-aggregate/10-funcs.sql`, `current/20-integration/10-funcs.sql`
    and `current/10-messagequeue/90-migrate.sql`.
  - `schema.go`: remove `CreateSchema()`, add `ApplySchemaOnline()` and
    `ApplySchemaOffline()`, with doc comments that state the
    contract and the privileges: creating schemas, tables, functions and
    triggers needs `CREATE` on the database, and altering or dropping existing
    objects needs the role that owns them.
  - Tests in `internal/schema/schema_test.go`: replace the two old-guard tests
    with an upgrade test that fetches v0.1.2 and v0.2.0 (`go mod download`) and
    applies each one's own `ddl` directory the way its `Create` did, seeds the
    rows the old tests seeded, runs `ApplySchemaOffline`, checks the rows
    survive, and compares the result with a new database (tables, columns,
    constraints, valid indexes, functions, triggers). Add tests for the outcomes
    (a newer compatible database is left alone, a different major number fails,
    `ApplySchemaOnline` refuses a change to the major number with the right
    error, a new database), for two callers at once, and, with synthetic
    migrations, for interleaved steps and a failing step that a rerun resumes.
    The tests fail, not skip, when a version can't be fetched.
  - `CHANGELOG.md`: the existing **[BC]** entry about the schema now says that
    every engine must be stopped, the database backed up, and
    `ApplySchemaOffline()` called; a **[BC]** `Changed` entry that
    `CreateSchema()` was renamed `ApplySchemaOnline()` and no longer applies a
    change to the major number; an `Added` entry for `ApplySchemaOffline()`.
    "this version" in the existing entry becomes "this runkit version".
    `docs/MIGRATING-v0.3.0.md` step 3 names the new function.
  - Follow-ups from the review of the migration machinery, each its own commit:
    666bd65 (READ COMMITTED), 31b7a44 (refuse a migration in progress that this
    runkit version can't finish), 702096e (errors worded for an application's
    operator), 4ef5c45 (the polled lock, the invalid-index refusal, and the
    operator flowcharts in `docs/schema-upgrades.md`), f43ab0c (fail a
    statement that ends its transaction), 6ed9582 (a test that applies
    `current/` again), 78c4c13 (close the connection) and 39e1095 (a `stmt`
    step must be safe to run twice).
- [x] **The automatic schema migration flowchart.** PR or commit: e30c230
  - One flowchart, `docs/automatic-schema-migration.md` (drafted), titled
    "Automatic schema migration", replaces the two charts that were in
    `docs/schema-upgrades.md`. The user decided that it is the only chart: no
    chart of how a migration is applied, no table of errors, and no flowchart
    for resolving invalid indexes.
  - It shows what the engine does to the schema when it starts. Every check,
    before and after the lock, ending in the engine running, or stopping
    because a newer engine is required or because a person has to intervene.
    The migration itself is one step, followed by whether it succeeded. The
    engine never applies an offline upgrade, so the chart has no question about
    it; the summary says what `ApplySchemaOnline()` and `ApplySchemaOffline()`
    do differently.
  - Its reader is primarily the application's developer. It says "the engine",
    its questions are full sentences, each outcome is an event that says what
    the engine does, every branch leaves a decision box, a loop returns to an
    action box rather than into a decision, each edge into an outcome has its
    own box so that no edges cross, each outcome starts with 🟢 when the engine
    runs or 🛑 when it stops, each box that loops back starts with 🔄, and its
    loop is a dotted arrow. The box about invalid indexes links to PostgreSQL's
    documentation with an HTML link in its label, which GitHub renders.
  - Check it again once `Run` applies the schema. Until then, only the two
    functions make these checks, and the chart's first box, "Engine starts",
    isn't true yet.
- [x] **`Run` applies the schema.** PR or commit: e30c230
  - `engine.go`: `Run` calls `schema.Apply` online, with the engine's timeout
    constants, before it binds the application. It tries again with the
    engine's failure policy after `xsql.IsLockNotAvailableError`, and returns
    any other error, except one it can run despite.
  - `schema.Apply` returns every error after it decides to upgrade wrapped in
    an `UpgradeFailedError`, with `Current`, `Required` and the cause, and the
    cause's message unchanged. If `Required.CanRunOn(Current)`, `Run` logs the
    error as an ERROR and starts. In practice that is a database behind only in
    its patch version.
  - The runner returns a `StepFailedError` for a failed step, beside
    `StepTimeoutError`.
  - Tests in `engine_test.go`: an empty database; behind in the minor version;
    behind in the major version; ahead in the major version; a failed upgrade
    that the engine can run despite, using an invalid index; a retry after a
    lock timeout. None asserts on log output.
  - `CHANGELOG.md`: an `Added` entry for automatic schema migration. The
    README example no longer calls `ApplySchemaOnline()`.
  - `Run()`'s doc comment states the contract.
  - README: a "Database schema" section says what the engine's schema is and
    links the current schema, then has "Versioning" (the scheme, in plain
    words), "Upgrades" (automatic and by function, linking the flowchart) and
    "Major schema upgrades". A README in `internal/schema/internal/current/`
    explains the directory and how the numbers in its names set the order. The
    operational documentation task builds on this section.
- [x] **Collapse the engine's schemas into one.** PR or commit: d338872
  - Why: the engine's eight generic schema names (`process`, `application` and
    so on) can clash with the host application's own schemas, and
    `DropSchema()` drops each of them with `CASCADE`, which would destroy an
    application schema with the same name. One schema has one name that can
    clash. Do it in this release, because the schema already needs downtime
    and is unreleased.
  - The schema is called `runkit`. A table keeps its name unless it clashes with
    another in the one schema, and then its name gets its component as a prefix:
    `aggregate_instances`, `process_instances`, `process_handlers` and
    `projection_handlers`. The other tables are `events`, `handler_checkpoints`,
    `identity` and `schema_version`. `eventstream.streams` becomes
    `event_streams`, because `streams` doesn't say what it holds, and
    `messagequeue.messages` becomes `queued_messages`, because `messages` could
    be read as every kind of message, including events. `idempotency_keys`
    becomes `command_idempotency_keys`, because Dogma's idempotency keys belong
    to commands, and the name alone didn't say so. Its indexes become
    `queued_messages_by_queue` and `queued_messages_by_instance`. The functions
    and their triggers lose their schema as a qualifier, so each was renamed,
    one at a time, to say what it does without it. A trigger and its function
    share one name. `eventstream.acquire_for_write` becomes
    `acquire_event_stream`, `append` becomes `append_events_to_specific_stream`,
    `append_any` becomes `append_events_to_any_stream`, `signal_create` becomes
    `signal_event_stream_created`, `signal_append` becomes
    `signal_events_appended`, `messagequeue.add_command` becomes
    `enqueue_command`, `signal_add` becomes `signal_message_enqueued`,
    `signal_move` becomes `signal_message_queue_changed`, and
    `aggregate.complete_with_events` becomes `save_aggregate_instance`. The
    `event` type keeps its name. `aggregate.complete_without_events` is removed,
    and the aggregate package runs its single `DELETE` itself.
  - The migration to schema version v2.0.0 moves each table of the v0.2.0
    schema with `ALTER TABLE ... SET SCHEMA`, which copies no rows,
    renames the tables and the auto-named indexes and constraints that need it,
    moves the
    `event` type, drops the old functions, and drops the old schemas. `current/`
    declares everything in the new schema, and its `20-integration` directory,
    which only creates an empty schema, is deleted. The directories of
    `current/` keep their numbers for ordering, so `schemas()` no longer derives
    schema names from them and returns `runkit`. The `runkit` schema and
    `runkit.schema_version` already exist when the migration runs, because
    `bootstrap.sql` creates them first, so the migration doesn't create the
    schema.
  - Every qualified name in the Go code, tests and docs changes (about 270
    references in 35 Go files). The signal names are renamed too, because
    they no longer match a schema: `eventstream.create` becomes
    `event_stream_created`, `eventstream.append.<stream-id>` becomes
    `events_appended.<stream-id>`, and `messagequeue` and
    `messagequeue.<queue-id>` become `messages_enqueued` and
    `messages_enqueued.<queue-id>`. This is safe only because every engine must
    be stopped for this release. The username note on `containerUsername`
    follows.
  - The public `DropSchema()` stays, because applications need it in their own
    tests. It and `schema.Drop` run the single `DROP SCHEMA IF EXISTS runkit
    CASCADE`, which can no longer drop an application schema that happens to
    share a name with one of the engine's. `schemas()` is deleted.
  - The invalid-index check looks only in `runkit`, not in the old schemas.
    No released runkit version built an index concurrently, so a database
    created by one has no invalid index of the engine's in them, and after
    v0.3.0 the old schemas are gone.
  - The upgrade test already compares the migrated database with a new one, so
    it covers the result.
- [x] **Flatten `current/` into one file per component.** PR or commit: 2104575
  - Why: each subdirectory of `current/` matched one of the old schemas. Now
    that everything is in `runkit`, the subdirectories only group files, but
    they make the README explain two levels of ordering and why the files
    directly in `current/` aren't applied.
  - Each subdirectory becomes one file named after it, such as
    `00-eventstream.sql`, holding its tables, then its functions, then its
    triggers. `embed.go` embeds `*.sql`, `executeDirectory` reads one directory
    instead of walking a tree, and the README says that the files are applied
    in lexical order of their names. The later tasks in this plan that name a
    file in `current/` are updated to the new names.
  - It's a separate commit from the collapse, so that the collapse's diff of the
    SQL shows its renames instead of whole files moving.
- [x] **Maintainer document (closes #96).** PR or commit: —
  - `docs/maintainers/schema.md`: the schema version and its meaning, the
    wording, the v0.1.0 floor, the deploy-impact rule and the list of operations
    allowed in a migration that raises only the minor or patch number, how to
    add a migration or a change to `current/` only, that a column added by a
    migration goes last in its table in `current/` because `ADD COLUMN` can't
    place it elsewhere and the upgrade test compares `pg_dump` output, which
    lists columns in order, what `tx` and `stmt` steps are for and that review
    is the only check that a `stmt` step doesn't block reads or writes, the
    rules for engine SQL (name the columns) and for replacing a function, the
    changelog line and how to word a slow upgrade, the `[!IMPORTANT]` note that
    the changelog section of a runkit version that raises the major number
    starts with, which says "This release includes breaking changes to the
    engine's SQL schema" and links to the README's section on major schema
    upgrades, the unreleased-edit rule and its risk, the rules for fixing a
    released migration ("To fix a step that fails, change only that step and
    those after it, and keep the schema version. Never change, insert or remove
    a step before it, because a database may have completed those steps, and a
    rerun skips them by count." and "If a released migration completed with a
    wrong result, fix it with a later migration."), how a hotfix to a prior
    runkit version gets its schema version, because the migration code assumes
    that every runkit version contains every migration below its target, schema
    versus cluster compatibility, the consequence of raising the major number
    after 1.0, and the Dialer versioning guide's rule that anything needing
    human intervention is a major version.
  - Review reshaped the document: it holds only instructions, with no
    background that the README or the startup flowchart covers. The schema
    version comes from one flowchart of common changes, with "Decide in review"
    for anything else. Schema changes are banned in a release of a runkit
    version that isn't the latest with its major schema version, which replaces
    the hotfix rules. Cluster compatibility and the post-1.0 splitting guidance
    were removed.
- [ ] **Operational documentation in the README.** PR or commit: —
  - A "The database schema" section for the people who deploy and run an
    application, written from what the code does, after "Running an
    application". The example there no longer calls `ApplySchemaOnline()`,
    because `Run` applies the schema. Applying it separately, with different
    database credentials, is covered under strict DDL control. It covers:
    - what the schema version is, that the engine records it in
      `runkit.schema_version`, how to read it (`SELECT version FROM
      runkit.schema_version`), and that each runkit version's changelog section
      states its schema version and whether it is new;
    - the normal case: `Run` creates the schema on an empty database and applies
      changes that raise only the minor or patch number itself, so deploying a
      new runkit version needs no separate step, and the database role needs the
      privileges listed in the doc comments;
    - strict DDL control: call `ApplySchemaOnline()` from a pipeline with a
      privileged `*sql.DB` before deploying; the engine's own role then needs no
      DDL privileges, and `Run` only checks and starts. An example with two
      database handles;
    - raising the major number: `Run` and `ApplySchemaOnline()` refuse. The
      section "Major schema upgrades" already holds the steps and the
      rollback, and applies to every such upgrade. It changes only to say that
      `Run` refuses too;
    - which runkit versions can run on which database, in plain words: the same
      major number in the schema version, and a database whose schema version is
      at least the engine's, so engines can be replaced one at a time while the
      major number stays the same, and an older engine keeps running after a
      newer one has upgraded the database. An engine refuses a database with a
      higher major number, and its error points to the changelog;
    - the upgrade floor: v0.1.0 is the oldest runkit version that upgrades
      directly, and a migrate call applies every schema version above the
      database's, in order;
    - one general sentence that a new runkit version may apply schema changes
      before it starts, and that the changelog says when a version's changes are
      slow. Nothing about step kinds;
    - the connection that `ApplySchemaOnline()`, `ApplySchemaOffline()` and
      `Run` need: a direct connection to PostgreSQL, or a connection pooler in
      session mode. They hold an advisory lock on one connection while they
      change the schema, and a pooler in transaction mode, such as PgBouncer,
      can leave that lock held, so that later calls wait forever. From the
      [review of the migration implementation](2026-11-16-schema-migration-review.md).
- [ ] **Drop `failures` and `resume_at` (#127).** PR or commit: — Drop both
  columns in the migration to schema version v2.0.0, stop declaring them in
  `current/00-eventstream.sql`, and test it in the upgrade test.
- [ ] **Upgrade test needs no change when a schema version is released.** PR or
  commit: —
  - Why: today, releasing a new schema version means adding the runkit version
    to `releasedVersions` by hand. The first time that happens, after v0.3.0,
    `createSchemaOf` and `testMigrateFrom` also break, because both assume the
    layout and tables of runkit v0.2.0 and earlier.
  - Add the schema version line to the released changelog sections: v0.1.0
    `Schema version: v1.0.0 (new)`, v0.1.1 and v0.1.2 `v1.0.0 (existing)`, and
    v0.2.0 `v1.1.0 (new)`. The user allowed this edit of released sections.
  - The upgrade test takes its runkit versions from the changelog sections
    marked `(new)`, so `releasedVersions` goes away.
  - `createSchemaOf` creates a schema from a runkit version of v0.3.0 or later
    by applying that version's bootstrap and current schema and recording its
    schema version.
  - The data checks in `testMigrateFrom` run only for the runkit versions whose
    tables they insert into, v0.2.0 and earlier.
  - Remove the guide's instruction about `releasedVersions` and
    `createSchemaOf`.

Before tagging v0.3.0, review the `[Unreleased]` section of `CHANGELOG.md` with
the user, which they deferred until then. Known fixes: the entry "The database
schema has changed in a way that engines built for the earlier schema can't run
on" says "engines built for", but runkit is the engine, and its clause is
object-first. The README now refers to "release notes" rather than the
changelog, because readers may arrive from GitHub's release page.

Later, not part of these tasks: a test that runs the previous runkit version's
engine against an upgraded database. I'd draft an issue and show it before
filing.

Also later, once runkit v0.3.0 is released: make the upgrade test start from
every released runkit version, found with `go list -m -versions`, with each
one's schema version read from the changelog line of its release. A schema
author then needs no setup, and a failing test lists what their migration lacks.
Until then the test starts from v0.1.2 and v0.2.0, the two released schemas.

Prose in the README, the maintainer document and any doc comment follows the
writing-prose skill. Doc comments state the contract only; the reasons live in
implementation comments and the maintainer document.

## Critical files

`internal/schema/schema.go`, `internal/schema/schema_test.go`,
`internal/schema/version.go`, `internal/schema/checks.go` (the checks that
`Apply` makes before it changes anything), `internal/schema/error.go` (the error
types that those checks return), `internal/schema/internal/current/**` (renamed
from `ddl`), `internal/schema/internal/migrations/` (the migrations and the
runner), `internal/schema/internal/upgradelock/`,
`internal/schema/internal/version/`, `internal/x/xsql/tx.go`, `engine.go`,
`internal/application/bind.go`, `schema.go`, `README.md`, `CHANGELOG.md`,
`docs/automatic-schema-migration.md`, new `docs/maintainers/schema.md`.

## Verification

Plain `make precommit` (the Makefile already runs one test package at a time),
run in the background with its output and exit status saved to a file, checking
that both `go fix` and `go fmt` appear in the output. The upgrade test against
the real v0.1.2 and v0.2.0 DDL. A manual run of v0.2.0's `CreateSchema()`
against a scratch database, then the new `ApplySchemaOffline()` and `Run`.

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
