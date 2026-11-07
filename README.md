<div align="center">

# Dogma Engine

Run message-driven, event-sourced [Dogma] applications.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

The [engine] is embedded in your application's Go binary and orchestrates message
delivery, state persistence, and the execution of application logic. PostgreSQL
provides persistence and coordination between nodes running the same application.

## Features

- **Handler execution** — Runs the aggregate, integration, process, and
  projection handlers defined by your Dogma application.

- **Persistence** — Stores events, pending commands, process state, and deadlines
  in the engine's database.

- **Clustering** — Distributes work between nodes running the same application
  against a shared database, adjusting as nodes join and leave.

- **Events between applications** — Delivers events recorded by one Dogma
  application to process and projection handlers in another, using gRPC.

## Requirements

The engine stores its state in [PostgreSQL]. The minimum supported version is
PostgreSQL 18.

Applications choose their own projection storage; the engine's PostgreSQL
requirement does not constrain it. [ProjectionKit] provides adapters for building
projections with several database systems.

<!-- references -->

[dogma]: https://github.com/dogmatiq/dogma
[engine]: https://github.com/dogmatiq/dogma/blob/main/docs/glossary.md#engine
[PostgreSQL]: https://www.postgresql.org/
[projectionkit]: https://github.com/dogmatiq/projectionkit
