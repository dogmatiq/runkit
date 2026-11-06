<div align="center">

# Dogma Engine Reference Implementation

This is a reference implementation of a [Dogma] engine.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/runkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/runkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/runkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/runkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/runkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/runkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/runkit)

</div>

## Requirements

The engine stores its state in [PostgreSQL]. The minimum supported version is
PostgreSQL 18.

## Process event consumption

Process handlers consume local and remote event streams through the same catalog
as projections. A new process handler starts each stream at the beginning,
including historical events. Existing handlers resume from their stored
checkpoints; adding a route
does not rewind those checkpoints. A stream without a checkpoint starts at the
beginning.

When upgrading from a version that uses `eventstream.handler_checkpoints`, stop
all engine nodes before calling `CreateSchema()` and restarting them. The schema
upgrade preserves process checkpoint offsets, moves them into the process schema,
and removes the old event-pump tables. Old and new engine versions cannot run
against the database together during this upgrade.

<!-- references -->

[Dogma]: https://github.com/dogmatiq/dogma
[PostgreSQL]: https://www.postgresql.org/
