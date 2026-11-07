<div align="center">

# Dogma Engine Reference Implementation

Run [Dogma] applications with PostgreSQL.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

## Overview

RunKit is a reference implementation of a [Dogma] engine. It is embedded in your
application's Go binary and orchestrates message delivery, state persistence,
and the execution of application logic.

Use Dogma to define your application's messages, handlers, identities, and
routes. RunKit provides the infrastructure that runs those handlers and stores
their data in PostgreSQL.

## Features

- **PostgreSQL persistence** — Stores events, pending commands, process state,
  and deadlines in the application's database.

- **Handler execution** — Runs the aggregate, integration, process, and
  projection handlers defined by your Dogma application.

- **Clustering** — Distributes work between nodes running the same application
  against a shared database, adjusting as nodes join and leave.

- **Remote events** — Makes application event streams available over gRPC and
  supports consuming events from remote applications.

## Requirements

- Go 1.27 or later.
- PostgreSQL 18 or later.
- A database connection opened with the [pgx] driver.

Each database belongs to one Dogma application identity. Nodes running the same
application share that database; different applications need separate databases.
The caller owns the database connection pool and closes it after the engine has
stopped.

## Getting started

If you are new to Dogma, start with its [concepts] document. The [example
application] demonstrates how to structure a Dogma application and test its
message flow with [testkit]. For reference material, see the [API documentation].
Release-specific changes and upgrade instructions are in the [changelog].

Add RunKit to your application's module:

```sh
go get github.com/dogmatiq/runkit
```

Create a PostgreSQL database for the application and obtain its connection
string, such as `postgres://user:password@localhost:5432/myapp?sslmode=disable`
for a local development database.

The following wiring accepts your existing Dogma application and a command it
handles. It opens the database, creates the engine's schema, starts the engine,
and submits the command. Pass a context that is canceled when the application
should stop, for example one created with `signal.NotifyContext()`.

```go
package example

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/runkit"
	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
)

func run(
	ctx context.Context,
	dsn string,
	app dogma.Application,
	command dogma.Command,
) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := runkit.CreateSchema(ctx, db); err != nil {
		return err
	}

	engine := runkit.New(app, db)
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return engine.Run(ctx)
	})

	group.Go(func() error {
		return engine.ExecuteCommand(ctx, command)
	})

	return group.Wait()
}
```

`CreateSchema()` creates or updates the engine's schema and can be called more
than once. The database user must have permission to create the required
schemas, tables, and functions.

`Run()` blocks until the context is canceled or the engine cannot continue.
Call it once per engine instance. The example keeps the database open until
both goroutines finish, including when startup or command submission fails.

`ExecuteCommand()` waits for the engine to be ready, then returns once the
engine has taken ownership of the command. It does not wait for the handler to
finish by default. A service can call it from HTTP handlers or other application
entry points. See [dogma.CommandExecutor] for execution options and event
observation. `Engine.Ready()` exposes readiness for health checks.

## Clustering and remote events

Run additional engine instances with the same application and database to form a
cluster. Nodes discover each other through PostgreSQL and adjust work assignment
as membership changes.

An engine can also serve its application's event streams to other applications
over gRPC. Configure `WithListenAddress()` to enable the server; it is disabled
by default. Configure transport credentials with `grpc.Creds()` to encrypt
connections and authenticate clients. Any client that can connect can consume
all of the application's events.

To consume remote events in projections, supply a `RemoteEngineDiscoverer` using
`WithRemoteEngineDiscoverer()`. `StaticRemoteEngines()` is available for a fixed
set of gRPC connections. Each connection represents a remote application cluster,
rather than each of its individual nodes. The caller owns these connections and
closes them after the engine stops using them. See the [remote discovery API]
for the connection lifecycle and the [engine options] for server configuration.

## Ecosystem

RunKit is part of the Dogma collection of Go modules:

- [dogma] — Defines the API for building applications.
- [testkit] — Utilities for testing Dogma applications.
- [projectionkit] — Utilities for building projections in popular database systems.

## Development

Run the test suite with:

```sh
go test ./...
```

Database tests start PostgreSQL containers automatically using Testcontainers.
Docker must be running and accessible to the test process. An existing
application database is not required.

RunKit is distributed under the [MIT license].

<!-- references -->

[api documentation]: https://pkg.go.dev/github.com/dogmatiq/runkit
[changelog]: CHANGELOG.md
[concepts]: https://github.com/dogmatiq/dogma/blob/main/docs/concepts.md
[dogma]: https://github.com/dogmatiq/dogma
[dogma.CommandExecutor]: https://pkg.go.dev/github.com/dogmatiq/dogma#CommandExecutor
[engine options]: https://pkg.go.dev/github.com/dogmatiq/runkit#EngineOption
[example application]: https://github.com/dogmatiq/example
[MIT license]: LICENSE
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[projectionkit]: https://github.com/dogmatiq/projectionkit
[remote discovery API]: https://pkg.go.dev/github.com/dogmatiq/runkit#RemoteEngineDiscoverer
[testkit]: https://github.com/dogmatiq/testkit
