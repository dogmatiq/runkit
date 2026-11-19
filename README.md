<div align="center">

# Dogma Engine

Run message-driven, event-sourced [Dogma] applications in production.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

This repository provides a [Dogma engine]. It is embedded in your application's
Go binary and orchestrates message delivery, state persistence, and the
execution of application logic. PostgreSQL provides persistence and coordination
between nodes running the same application.

Run multiple engine instances for the same application against a shared database
to form a cluster. Work is distributed across nodes and reassigned as nodes join
or leave.

Applications can also consume events recorded by other Dogma applications. The
engines exchange events over gRPC, delivering them to process and projection
handlers in the consuming application.

## Requirements

The engine stores its state in [PostgreSQL]. The minimum supported version is
PostgreSQL 18.

> [!NOTE]
> The engine doesn't work through a connection pooler in transaction mode, such
> as Neon's pooled connection string or Supabase's port 6543. Use a direct
> connection, or a pooler in session mode.

Applications choose their own projection storage; the engine's PostgreSQL
requirement does not constrain it. [projectionkit] provides adapters for building
projections with several database systems.

## Running an application

Pass your application and a database connection opened with the [pgx] driver to
`runkit.New()`:

```go
engine := runkit.New(myDogmaApp, db)

// Run blocks until the context is canceled. It returns an error if the
// engine is unable to start.
return engine.Run(ctx)
```

## Database schema

The engine keeps its state in tables of its own, in the PostgreSQL database that
you give it. The SQL files in the [current schema] directory define those
tables, and the functions and triggers that go with them.

The engine is a library, so its schema comes with it, and a new runkit version
may change the schema. The engine makes those changes itself, so upgrading
runkit usually needs nothing more than deploying your application.

### Versioning

The engine's schema has a version of its own, which is separate from the runkit
version. The table `runkit.schema_version` holds the current schema version. To
read it, run `SELECT version FROM runkit.schema_version`. The release notes of
each runkit version document the schema version that the runkit version needs.
Schema versions follow [semantic versioning], written with a `v` prefix, such as
`v2.0.0`:

- The **major** number goes up when the schema changes in a way that stops older
  engines from using it.
- The **minor** number goes up when the schema gains something new. Older
  engines keep working.
- The **patch** number goes up when the schema changes without changing what it
  does, such as a new index that makes a query faster.

This means that an engine works with a schema that's newer than the engine
needs, as long as the major number is the same. When one engine upgrades the
schema, the older engines that share the database keep running. For example, an
engine that needs `v2.1.0` works with a database at `v2.4.0`, but not with one
at `v3.0.0`.

After runkit `v1.0.0` is released, new major versions of the schema will only be
introduced in new major versions of runkit.

### Upgrades

- When the engine starts, it creates the schema if necessary, or upgrades an
  older schema if that's safe to do while other engines are running. If the
  upgrade isn't safe, the engine stops with an error. The [automatic schema
  migration flowchart] illustrates how the engine decides what to do.
- [`ApplySchemaOnline()`] does what the engine does automatically, without
  running the engine. It's intended for deployments where the engine's runtime
  database credentials aren't allowed to change the schema, and a separate
  deployment step calls it with credentials that are.
- [`ApplySchemaOffline()`] does the same, and can also apply a major schema
  upgrade, which raises the major number of the schema version. Every engine
  must be stopped before it's called, as [major schema upgrades] explains.

Many upgrades are applied in a single transaction, so they either finish or
change nothing. An upgrade with several steps can fail part-way through, and the
next attempt carries on from the step where it stopped.

### Restricting the engine's privileges

By default, the engine creates and upgrades the schema itself, so its database
role needs the `CREATE` privilege on the database, and must own the `runkit`
schema and everything in it.

To run the engine in production with a role that can't change the schema,
manage the schema yourself:

- Before you deploy a new runkit version, call [`ApplySchemaOnline()`] as a role
  that can change the schema. When the engine starts, it then finds the schema
  up to date, and doesn't change it.

  ```go
  if err := runkit.ApplySchemaOnline(ctx, schemaDB); err != nil {
  	return err
  }
  ```

- Grant the engine's role `USAGE` on the `runkit` schema, `SELECT`, `INSERT`,
  `UPDATE` and `DELETE` on its tables, and `EXECUTE` on its functions. Grant the
  same on the objects that each upgrade adds, or use `ALTER DEFAULT PRIVILEGES`
  so that they get these privileges automatically.

### Major schema upgrades

<!--
Don't remove or rename this section. The changelog section of every runkit
version with a major schema upgrade links to it, and so do the release notes on
GitHub, which are copied from the changelog.
-->

A major schema upgrade brings the schema to a new major version, which stops
older engines from using it. The release notes of a runkit version that needs a
major schema upgrade say so at the top.

#### How to upgrade

> [!CAUTION]
> An engine that runs during the upgrade can write to a table while the upgrade
> changes or removes it, and what it writes can be lost.
> [`ApplySchemaOffline()`] doesn't check whether any engine is running, so stop
> every engine that uses the database before calling it.

1. Stop every engine that uses the database, and back up the database.
2. Call [`ApplySchemaOffline()`] from the new runkit version, once. Call it from
   a separate program or deployment step, not from your application's startup
   code.
3. If it returns an error, correct the cause and call it again.
4. Once it has returned without an error, start the engines of the new runkit
   version. Engines from earlier runkit versions now refuse to start on this
   database.

> [!WARNING]
> Versions of runkit prior to `v0.3.0` don't check the schema version, so they
> start without an error. What they do after that is unpredictable, and can
> include losing data. Keep them stopped.

#### Rolling back

Only a backup can undo a major schema upgrade.

- **Restore into an empty database, or drop the engine's schema first.** A
  restore that only replaces the objects in the backup leaves every new object
  from the upgrade in place. For an upgrade from runkit `v0.2.0` or earlier,
  those objects include the `runkit` schema, which still records the new schema
  version, so a later call to [`ApplySchemaOffline()`] changes nothing.
- **If engines have run since the upgrade, restore or rebuild projections too.**
  Restoring the backup removes the events recorded since the upgrade. Each
  projection keeps its checkpoints in its own storage, which the restore doesn't
  touch, so a checkpoint can be past the new end of a stream. The projection
  then skips the new events that are recorded at those offsets.

<!-- references -->

[`ApplySchemaOffline()`]: https://pkg.go.dev/github.com/dogmatiq/runkit#ApplySchemaOffline
[`ApplySchemaOnline()`]: https://pkg.go.dev/github.com/dogmatiq/runkit#ApplySchemaOnline
[automatic schema migration flowchart]: docs/automatic-schema-migration.md
[major schema upgrades]: #major-schema-upgrades
[current schema]: internal/schema/internal/current/
[dogma]: https://github.com/dogmatiq/dogma
[dogma engine]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#engine
[PostgreSQL]: https://www.postgresql.org/
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[projectionkit]: https://github.com/dogmatiq/projectionkit
[semantic versioning]: https://semver.org/
