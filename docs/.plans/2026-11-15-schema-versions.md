# Plan: schema versions, migrations and `Run` applying the schema (#103, closes #96)

## Context

Table changes are hand-written guards that pile up in `internal/schema/ddl`
(the foreign-key `DO` block, `ADD COLUMN IF NOT EXISTS`, `DROP FUNCTION IF
EXISTS`, `90-migrate.sql`). #103 asks for numbered migrations recorded in a
ledger. The design that came out of review goes further. The schema has its own
semver version, which says which runkit versions can run on a database. A deploy
that needs no human decision needs no human step, so `Run` applies safe schema
changes itself. A change that does need a human, a schema major, can only be
applied by a function whose name says so.

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
- **Migration guides** (`docs/MIGRATING-v0.3.0.md` and any later
  `docs/MIGRATING-*.md`) keep "version" for the runkit version throughout,
  because each is about moving to one runkit version, and say "schema version"
  where they mean the database's. The maintainer document records this.
- Commit messages and PR descriptions keep their usual wording, with "schema
  version" only where it means the schema's.
- **Names:** type `schema.Version`; constant `schema.CurrentVersion`; method
  `CanRunOn`; tables `engine.schema_version` and `engine.migration_steps`;
  directory `migrations/`; public functions `MigrateSchemaOnline()`,
  `MigrateSchemaOffline()` and `DropSchema()`; internal
  `schema.Migrate`. `CreateSchema()` is removed.
- **step** is one file in a migration's directory. A step whose file name has
  the suffix `tx` before `.sql` holds statements that run together in one
  transaction. A step with the suffix `stmt` holds one statement, which runs on
  its own outside any transaction.

## Settled design

- **Schema version.** Plain semver with a `v` prefix, such as `v2.0.0`, and no
  build metadata. `schema.CurrentVersion` is the schema version of this runkit
  version; this release is `v2.0.0`. The pairing of schema versions with runkit
  versions lives in the changelog sections.
- **Meaning.** Major: engines built for an earlier major can't run, and the
  deploy needs a human to stop them first. Minor: additive, and older engines
  keep running. Patch: nothing an engine can observe.
- **Rule.** `schema.CurrentVersion.CanRunOn(databaseSchemaVersion)`: the majors
  are equal and the database's minor is at least the engine's; the patch is
  ignored. Doc sentence: "CanRunOn reports whether the runkit version with this
  schema version can run on a database at the given schema version."
- **The database's schema version.** Table `engine.schema_version` holds one row
  (a singleton, like `application.identity`) with `version` and `applied_at`:
  the schema version of the database and the time it was reached. It is set once,
  at the end of a successful migrate call. Its table file lives in `current/` like
  the others, and the migrate code applies that file first so that it can read it.
- **Starting point.** A database with an engine table (`eventstream.streams`)
  but no `engine.schema_version` is at `v1.0.0`, the v0.2.0 schema. v0.2.0 is the
  oldest runkit version that upgrades directly. A database without
  `eventstream.streams` is new.
- **`current/` is the schema.** The directory `internal/schema/current` (renamed
  from `ddl`) describes the complete schema as it is at `CurrentVersion`, in
  plain SQL grouped by PostgreSQL schema, and is applied to every database that
  a migrate call changes. Doc sentence: "Each file under `current/` defines part
  of the database schema as it is at `CurrentVersion`: its tables, functions and
  triggers." `migrations/` holds one directory for each schema version that has
  to change an existing database, named for the schema version it produces, such
  as `v2.0.0`. A schema version that only adds or replaces functions or triggers
  has no directory. A new, empty database gets `current/` only, with every
  migration skipped. Migrations run before `current/`, so a migration that fills
  a new table creates the table itself, and the drift test checks that the
  definition matches `current/`. `current/` keeps its `CREATE INDEX IF NOT
  EXISTS` with the same index names, so an index built by a step is found by
  `current/` and left alone.
- **Steps.** A migration's steps run in file-name order, whatever their kind, so
  `tx`, `stmt`, `stmt`, `tx` works. Each step has exactly one of the suffixes,
  or the package panics when it loads the migrations. A `tx` step records itself
  in `engine.migration_steps` in its own transaction. A `stmt` step is
  idempotent (for an index: `IF NOT EXISTS`, after dropping an invalid
  leftover), and is recorded after it runs. Rows are keyed by migration and step
  name. A rerun resumes at the first unrecorded step.
- **Cleanup.** The last thing a successful migrate call does, in one
  transaction, is set `engine.schema_version` to `CurrentVersion` and delete
  every row of `engine.migration_steps`. A crash before that leaves every step
  recorded and the version unchanged, and the rerun skips the recorded steps and
  finishes. Rows for steps that no longer exist, for example after an in-place
  edit of an unreleased migration, are ignored and deleted at the end.
- **A `stmt` step has one statement because PostgreSQL checks it.** The runner
  prepares the file's text and then executes the prepared statement; the
  extended query protocol rejects more than one statement. No SQL parsing in Go
  and no test per version: the upgrade test applies every migration against a
  real database. The first implementation work checks on PostgreSQL 18 that a
  prepared `CREATE INDEX CONCURRENTLY` and a prepared `DO` block that commits
  both run outside a transaction.
- **One lock.** A session-level advisory lock, with a distinctive two-integer
  key, is held on one connection for the whole migration, so that only one node
  migrates at a time. It replaces the `LOCK TABLE pg_catalog.pg_namespace` that
  `Create` takes today, and needs no special privilege.
- **Timeouts follow the entry point.** `MigrateSchemaOnline()` and `Run()` assume that
  engines may be running: a `tx` step runs with a lock timeout and a statement
  timeout, and a `stmt` step runs with no timeout, because it is long by nature
  and doesn't block reads or writes. `MigrateSchemaOffline()` applies
  no timeout to any step, since nobody is querying. The values are constants in
  `engine.go`, passed to the `schema` package as required fields. A migration
  that can't get its lock in time fails and rolls back, and `Run` retries the
  apply with backoff instead of returning the error. When a statement timeout
  stops a step under `MigrateSchemaOnline()` or `Run()`, the error says that the change
  takes longer than is allowed while engines may be running, and to stop every
  engine and call `MigrateSchemaOffline()`, so that a wrong guess about
  a step's cost becomes a clear human step and not an outage.
- **Allowed in a minor or patch migration**, because the cost doesn't grow with
  the data and the lock is brief: create a schema, table, function or trigger;
  add a column with no default or a constant default; create an index on a table
  that the same migration creates, with a plain `CREATE INDEX`; replace a
  function so that older engines still work; create an index on an existing table
  with `CREATE INDEX CONCURRENTLY` in a `stmt` step, and in no other way; add a
  constraint as `NOT VALID` in a `tx` step and validate it with `VALIDATE
  CONSTRAINT` in a `stmt` step. Anything else is a schema major, including
  dropping or renaming, changing a column's type, adding a constraint that scans
  the table, a plain index on an existing table, and moving or backfilling rows.
  A `stmt` step must hold an operation that doesn't block reads or writes: the
  two above, or a `DO` block that commits in batches. Nothing checks this, so
  review is the only check, and the maintainer document states it. There is no
  list of tables that count as small.
- **Entry points.** The migrate routine takes one flag, whether engines are
  known to be stopped, which allows a schema major and removes the timeouts. The
  public API has no option.
  - `MigrateSchemaOnline()`: creates the schema on an empty database and applies
    minor and patch changes. It returns an error instead of applying a schema
    major, and the error names `MigrateSchemaOffline()` and says to stop every
    engine first. Doc sentence: "MigrateSchemaOnline brings the database's schema
    to the schema version of this runkit version, creating the schema if the
    database has none. It applies only changes that are safe while engines are
    running."
  - `MigrateSchemaOffline()`: does what `MigrateSchemaOnline()` does and also
    applies a schema major. Nothing checks that no engine is running; the name
    and the documentation say so. Doc sentence: "MigrateSchemaOffline does what
    MigrateSchemaOnline does and also applies a schema major. Stop every engine
    that uses the database before calling it; the database itself must be
    running."
  - `Run()`: reads the database's schema version at start. Satisfied: start
    with no DDL. Behind within the same major, or empty: apply as
    `MigrateSchemaOnline()` does. If the apply fails and the engine can run anyway
    (behind by a patch only), log the failure with `engineLogger` and start;
    otherwise return the error. Behind by a major: refuse, with the message
    above. Ahead by a major: refuse, saying that the database's schema version
    is newer than this engine can run on and pointing to the changelog. The
    `schema` package returns errors, and the engine decides what to log.
  - A database at or above `CurrentVersion` by precedence needs no change, which
    covers a newer compatible database. Operators with strict DDL control call
    `MigrateSchemaOnline()` from their pipeline first, and `Run` then never attempts
    DDL.
- **Invariants checked by the package itself.** When it loads the embedded
  migrations it panics if a directory name isn't a schema version of the form
  `vX.Y.Z`, repeats a core, or is above `CurrentVersion`, or if a step lacks a
  suffix. Every test that migrates exercises this, so no test reaches into
  unexported state. The runner lives in its own internal package and takes an
  `fs.FS`, so tests can use synthetic migrations, including a failing step that
  a rerun resumes.
- **Majors and the module.** A schema major comes with a **[BC]** changelog entry
  that names the engines to stop. Before 1.0 that is a minor runkit version;
  from 1.0 it is a major runkit version, with a `/v2` import path, and obsolete
  objects stay until then. This release needs every engine stopped.
- **Rolling restarts.** Within a schema major, engines can be replaced one at a
  time. The engine's SQL names its columns (checked: the one `SELECT *`, in
  `execute.go`, selects from an `unnest`), so an added column can't break an
  older engine's statements. The schema version covers only the database:
  cluster compatibility (heartbeats, signal channels, queue semantics, gRPC) is
  signalled by **[BC]**, not by the schema version.
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

- [x] **The schema version type and the changelog line.** PR or commit: —
  - `internal/schema`: `Version` (parse, format, precedence, major, minor, patch,
    `CanRunOn`) and `CurrentVersion`.
  - `CHANGELOG.md`: the "Schema version:" line in `[Unreleased]`.
  - Tests: `CanRunOn`; the changelog line (the topmost section's line equals
    `CurrentVersion`, and says "(new)" only when the section below it differs,
    with sections before the line existed counting as `v1.0.0`).
- [ ] **The migration machinery, the migration to schema version v2.0.0, and the
  public functions.** PR or commit: —
  - Check on PostgreSQL 18 the two prepared-statement cases above, and whether
    `lock_timeout` cancels a concurrent index build while it waits for older
    transactions. If it doesn't, a lock timeout could be kept as a backstop for
    `stmt` steps.
  - The `engine.schema_version` and `engine.migration_steps` table files in
    `current/00-engine/`; the runner (its own internal package, over an `fs.FS`)
    with `tx` and `stmt` steps, the advisory lock, the timeouts, the cleanup and
    the invariants; `schema.Migrate`.
  - Migration `v2.0.0`, one `tx` step, unconditional because every database that
    reaches it is the v0.2.0 schema: create the `messagequeue` schema and
    tables, move commands and deadlines (the body of `90-migrate.sql` without its
    guards), drop `commandqueue` and `process.deadlines`, drop the checkpoint
    foreign key, add `process.instances.deadline_queue_id`, and drop the old
    aggregate and integration functions.
  - Rename `internal/schema/ddl` to `internal/schema/current` (and the embed
    directive), then delete the hand-rolled guards: the `DO` block in
    `current/00-eventstream/00-tables.sql`, the `ALTER` in
    `current/20-process/00-tables.sql`, the `DROP FUNCTION` lines in
    `current/20-aggregate/10-funcs.sql`, `current/20-integration/10-funcs.sql`
    and `current/10-messagequeue/90-migrate.sql`.
  - `schema.go`: remove `CreateSchema()`, add `MigrateSchemaOnline()` and
    `MigrateSchemaOffline()`, with doc comments that state the
    contract and the privileges: creating schemas, tables, functions and
    triggers needs `CREATE` on the database, and altering or dropping existing
    objects needs the role that owns them.
  - Tests in `internal/schema/schema_test.go`: replace the two old-guard tests
    with an upgrade test that fetches v0.2.0 (`go mod download`) and applies its
    own `ddl` directory the way its `Create` did, seeds the rows the old tests seeded, runs
    `MigrateSchemaOffline`, checks the rows survive, and compares the
    result with a new database (tables, columns, constraints, valid indexes,
    functions, triggers). Add tests for the outcomes (a newer compatible database
    is left alone, a different major fails, `MigrateSchemaOnline` refuses a major with
    the right error, a new database), for two callers at once, and, with
    synthetic migrations, for interleaved steps and a failing step that a rerun
    resumes. The tests fail, not skip, when v0.2.0 can't be fetched.
  - `CHANGELOG.md`: the existing **[BC]** entry about the schema now says that
    every engine must be stopped, the database backed up, and
    `MigrateSchemaOffline()` called; a **[BC]** `Changed` entry that
    `CreateSchema()` was renamed `MigrateSchemaOnline()` and no longer applies a
    schema major; an `Added` entry for `MigrateSchemaOffline()`.
    "this version" in the existing entry becomes "this runkit version".
    `docs/MIGRATING-v0.3.0.md` step 3 names the new function.
- [ ] **`Run` applies the schema.** PR or commit: —
  - `engine.go` and the start-up sequence (`internal/application/bind.go` is
    where the engine binds the database today): the check and apply described
    under `Run`, with the lock-timeout retry and backoff, errors that name
    schema versions, and the failure log through `engineLogger`. The timeout
    constants live in `engine.go`.
  - Tests: `Run` on an empty database; behind by a minor; behind by a major;
    ahead by a major; behind by a patch with a role that can't apply it (if the
    test database allows creating roles); a retry after a lock timeout. None
    asserts on log output.
  - `CHANGELOG.md`: an `Added` entry that `Run` applies schema changes that are
    safe.
  - `Run()`'s doc comment states the contract.
- [ ] **Operational documentation in the README.** PR or commit: —
  - A "The database schema" section for the people who deploy and run an
    application, written from what the code does, after "Running an
    application". The example there reduces to `New()` and `Run()`. It covers:
    - what the schema version is, that the engine records it in
      `engine.schema_version`, how to read it (`SELECT version FROM
      engine.schema_version`), and that each runkit version's changelog section
      states its schema version and whether it is new;
    - the normal case: `Run` creates the schema on an empty database and applies
      minor and patch changes itself, so deploying a new runkit version needs no
      separate step, and the database role needs the privileges listed in the
      doc comments;
    - strict DDL control: call `MigrateSchemaOnline()` from a pipeline with a
      privileged `*sql.DB` before deploying; the engine's own role then needs no
      DDL privileges, and `Run` only checks and starts. An example with two
      database handles;
    - schema majors: `Run` and `MigrateSchemaOnline()` refuse; the **[BC]** changelog
      entry says which engines to stop; the steps are to stop them, back up, call
      `MigrateSchemaOffline()` and start the new runkit version. This
      links to the migration guide and its backup instructions;
    - which runkit versions can run on which database, in plain words: the same
      schema major, and a database whose schema version is at least the engine's,
      so engines can be replaced one at a time within a major, and an older
      engine keeps running after a newer one has upgraded the database. An
      engine refuses a database with a newer major, and its error points to the
      changelog;
    - the upgrade floor: v0.2.0 is the oldest runkit version that upgrades
      directly, and a migrate call applies every schema version above the
      database's, in order;
    - one general sentence that a new runkit version may apply schema changes
      before it starts, and that the changelog says when a version's changes are
      slow. Nothing about step kinds;
    - `DropSchema()`.
- [ ] **Maintainer document (closes #96).** PR or commit: —
  - `docs/maintainers/schema.md`: the schema version and its meaning, the
    wording (including the migration guide exception), the v0.2.0 floor, the
    deploy-impact rule and the list of operations allowed in a minor or patch
    migration, how to add a migration or a change to `current/` only, what `tx`
    and `stmt` steps are for and that review is the only check that a `stmt` step
    doesn't block reads or writes, the rules for engine SQL (name the columns) and for
    replacing a function, the changelog line and how to word a slow upgrade, the
    unreleased-edit rule and its risk, schema versus cluster compatibility, the
    consequence for majors after 1.0, and the Dialer versioning guide's rule that
    anything needing human intervention is a major version.
- [ ] **Drop `failures` and `resume_at` (#127).** PR or commit: — Drop both
  columns in the migration to schema version v2.0.0, stop declaring them in
  `current/00-eventstream/00-tables.sql`, and test it in the upgrade test.

Later, not part of these tasks: a test that runs the previous runkit version's
engine against an upgraded database. I'd draft an issue and show it before
filing.

Prose in the README, the maintainer document, the migration guide and any doc
comment follows the writing-prose skill. Doc comments state the contract only;
the reasons live in implementation comments and the maintainer document.

## Critical files

`internal/schema/schema.go`, `internal/schema/schema_test.go`,
`internal/schema/current/**` (renamed from `ddl`), new
`internal/schema/migrations/`, a new internal
runner package, `engine.go`, `internal/application/bind.go`, `schema.go`,
`README.md`, `CHANGELOG.md`, `docs/MIGRATING-v0.3.0.md`, new
`docs/maintainers/schema.md`.

## Verification

Plain `make precommit` (the Makefile already runs one test package at a time),
run in the background with its output and exit status saved to a file, checking
that both `go fix` and `go fmt` appear in the output. The upgrade test against
the real v0.2.0 DDL. A manual run of v0.2.0's `CreateSchema()` against a scratch
database, then the new `MigrateSchemaOffline()` and `Run`.

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
