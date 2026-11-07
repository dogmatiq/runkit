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
if err := runkit.CreateSchema(ctx, db); err != nil {
	return err
}

engine := runkit.New(myDogmaApp, db)

// Run blocks until the context is canceled. It returns an error if the
// engine is unable to start.
return engine.Run(ctx)
```


<!-- references -->

[dogma]: https://github.com/dogmatiq/dogma
[dogma engine]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#engine
[PostgreSQL]: https://www.postgresql.org/
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[projectionkit]: https://github.com/dogmatiq/projectionkit
