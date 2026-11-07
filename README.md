<div align="center">

# RunKit

A reference implementation of a [Dogma] engine, backed by PostgreSQL.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

RunKit runs the application and message handlers you define with Dogma. It
provides event storage, durable command delivery, process state and deadlines,
and projection execution. Multiple engine nodes can run the same application
against a shared database and distribute work across the cluster.

Use Dogma to define your application's messages, handlers, identities, and
routes. Use RunKit to supply the persistence and execution machinery. The
[API documentation] describes the engine's public interface; the [changelog]
covers release-specific changes and upgrade instructions.

## Requirements

- Go 1.27 or later.
- PostgreSQL 18 or later.
- A database connection opened with the [pgx] driver.

Each database belongs to one Dogma application identity. Nodes running the same
application share that database; different applications need separate databases.
The caller owns the database connection pool and closes it after the engine has
stopped.

## Getting started

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

[Dogma]: https://github.com/dogmatiq/dogma
[API documentation]: https://pkg.go.dev/github.com/dogmatiq/runkit
[changelog]: CHANGELOG.md
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[dogma.CommandExecutor]: https://pkg.go.dev/github.com/dogmatiq/dogma#CommandExecutor
[remote discovery API]: https://pkg.go.dev/github.com/dogmatiq/runkit#RemoteEngineDiscoverer
[engine options]: https://pkg.go.dev/github.com/dogmatiq/runkit#EngineOption
[MIT license]: LICENSE
