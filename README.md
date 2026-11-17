<div align="center">

# Dogma Engine

Run message-driven, event-sourced [Dogma] applications in production.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

This repository provides a [Dogma engine]. It is embedded in your application's
Go binary and orchestrates message delivery, state persistence, and the execution
of application logic. PostgreSQL provides persistence and coordination between
nodes running the same application.

Run multiple engine instances for the same application against a shared database
to form a cluster. Work is distributed across nodes and reassigned as nodes join
or leave.

Applications can also consume events recorded by other Dogma applications. The
engines exchange events over gRPC, delivering them to process and projection
handlers in the consuming application.

## Requirements

The engine stores its state in [PostgreSQL]. The minimum supported version is
PostgreSQL 18.

Applications choose their own projection storage; the engine's PostgreSQL
requirement does not constrain it. [projectionkit] provides adapters for building
projections with several database systems.

## Running an application

Pass your application and a database connection opened with the [pgx] driver to
`runkit.New()`:

```go
// Create or update the engine's PostgreSQL schema. This is separate from
// running the engine so it can use different database credentials.
if err := runkit.ApplySchemaOnline(ctx, db); err != nil {
	return err
}

engine := runkit.New(myDogmaApp, db)

// Run blocks until the context is canceled. It returns an error if the
// engine is unable to start.
return engine.Run(ctx)
```

## Major schema upgrades

Some runkit versions change the database schema so that engines from earlier
runkit versions can no longer use the database. Upgrading to one of them needs
every engine to be stopped. The [changelog] section of such a runkit version
starts with a note that links here.

### How schema changes are applied

The database records its schema version, such as `v2.0.0`, in the table
`runkit.schema_version`. Each runkit version needs a particular schema version,
and its section of the changelog states which one.

- `ApplySchemaOnline()` applies only the changes that are safe while engines are
  running. If the new schema version has a higher major number, it returns an
  error and changes nothing.
- `ApplySchemaOffline()` also applies a change that raises the major number. It
  relies on every engine being stopped. If it fails part-way, calling it again
  carries on from where it stopped.

### Upgrading

> [!CAUTION]
> `ApplySchemaOffline()` doesn't check whether any engine is running. An engine
> that runs during the upgrade can write to a table while the upgrade changes or
> removes it, and what it writes can be lost. Stop every engine that uses the
> database before calling it.

1. Stop every engine that uses the database, and back up the database.
2. Call `ApplySchemaOffline()` from the new runkit version, once. Call it from
   something that runs by itself, such as a deployment step, and keep
   `ApplySchemaOnline()` as your application's call at startup.
3. If it returns an error, correct the cause and call it again.
4. Once it has returned without an error, start the engines of the new runkit
   version, and keep engines from earlier runkit versions stopped. An engine
   from runkit v0.3.0 or later refuses to start if its application calls
   `ApplySchemaOnline()` at startup, as the example above does. Nothing else
   stops such an engine, and an engine from runkit v0.2.0 or earlier never
   refuses.

### Rolling back

Only a backup can undo the upgrade.

- **Restore into an empty database, or drop the engine's schemas first.** A
  restore that replaces only the objects in the backup leaves every new object
  from the upgrade in place. After an upgrade from runkit v0.2.0 or earlier,
  that includes the `runkit` schema with the new schema version, so a later call
  to `ApplySchemaOffline()` changes nothing.
- **If engines have run since the upgrade, restore or rebuild projections too.**
  The restore removes the events recorded since the upgrade. A projection that
  handled them still has their offsets as its checkpoint, so it skips the events
  that are later recorded at those offsets.

<!-- references -->

[changelog]: CHANGELOG.md
[dogma]: https://github.com/dogmatiq/dogma
[dogma engine]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#engine
[PostgreSQL]: https://www.postgresql.org/
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[projectionkit]: https://github.com/dogmatiq/projectionkit
