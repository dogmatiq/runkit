<div align="center">

# Dogma Engine

Run message-driven, event-sourced [Dogma] applications with PostgreSQL.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

The engine is embedded in your application's Go binary and orchestrates message
delivery, state persistence, and the execution of application logic. PostgreSQL
provides persistence and coordination between nodes running the same application.

## Features

- **Handler execution** — Runs the aggregate, integration, process, and
  projection handlers defined by your Dogma application.

- **PostgreSQL persistence** — Stores events, pending commands, process state,
  and deadlines in the application's database.

- **Clustering** — Distributes work between nodes running the same application
  against a shared database, adjusting as nodes join and leave.

- **Remote events** — Makes application event streams available over gRPC and
  supports consuming events from remote applications.

## Requirements

The engine stores its state in [PostgreSQL]. The minimum supported version is
PostgreSQL 18.

## Documentation

For engine configuration and use, see the [API documentation]. Release-specific
changes and upgrade instructions are in the [changelog].

For Dogma's application model and terminology, see its [concepts] document and
[example application].

<!-- references -->

[api documentation]: https://pkg.go.dev/github.com/dogmatiq/runkit
[changelog]: CHANGELOG.md
[concepts]: https://github.com/dogmatiq/dogma/blob/main/docs/concepts.md
[dogma]: https://github.com/dogmatiq/dogma
[example application]: https://github.com/dogmatiq/example
[PostgreSQL]: https://www.postgresql.org/
