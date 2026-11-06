# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
[dogmatiq/protobuf]: https://github.com/dogmatiq/protobuf
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

This release introduces clustering. Several engines could already run against
the same database, but they competed with each other for work. They now
coordinate as the nodes of a cluster, which runs a single application, and work
is distributed across the cluster, adjusting as nodes join and leave.

It also adds partial support for consuming events across applications.
Projections can now consume events recorded by applications running in other
clusters. Support for process handlers will be added in a future release.

An engine serves its application's events over gRPC when it is given a listen
address using `WithListenAddress()`. It finds the clusters whose events its
projections consume using a `RemoteEngineDiscoverer`, which reports one
connection per cluster.

> [!WARNING]
> As with any gRPC server, the engine's gRPC server is insecure unless transport
> credentials are supplied using `grpc.Creds()`. Any client that can connect can
> consume all of the application's events, so the credentials should
> authenticate clients, for example with mutual TLS.

> [!NOTE]
> gRPC clients reject messages larger than 4 MiB by default. The engine's gRPC
> server sends events in batches of up to 2 MiB. If a single message is larger
> than that, it is sent on its own. To consume events larger than 4 MiB, a
> `RemoteEngineDiscoverer` must configure its connections with a higher limit.

### Added

- Added `New()` and the `WithLogger()`, `WithListenAddress()` and
  `WithProjectionCompactInterval()` options.
- Added a gRPC server so that engines running other applications can consume
  this application's events. It serves `EventStreamConsumerAPI` from
  [dogmatiq/protobuf]. It is disabled by default. Use `WithListenAddress()` to
  enable it.
- Added `Engine.ListenAddr()`, which returns the address that the gRPC server is
  listening on. It is useful when the server listens on a port chosen by the
  operating system, such as in tests.
- Added `RemoteEngineDiscoverer`, `StaticRemoteEngines()` and the
  `WithRemoteEngineDiscoverer()` option, so that projections can consume events
  recorded by applications running on remote engines. A discoverer adds and
  removes gRPC connections to remote engines as they become available and
  unavailable. Use `StaticRemoteEngines()` for a fixed set of connections.

### Changed

- **[BC]** The fields of `Engine` are now unexported. Use `New()` to create an
  engine, and `EngineOption` values to configure it.
- **[BC]** The database passed to `New()` must use the [pgx] driver. `New()`
  panics if it uses any other driver.
- **[BC]** `WithProjectionCompactInterval()` panics if the interval is not
  positive. Previously, a non-positive `ProjectionCompactInterval` meant that
  `DefaultProjectionCompactInterval` was used.
- **[BC]** `Engine.Run()` must be called at most once.
- **[BC]** A database can only be used by a single application. The first
  engine to run against a database binds it to that engine's application.
  `Engine.Run()` returns an error if the database is bound to an application
  with a different identity key.
- `Engine.ExecuteCommand()` now returns an error once the engine has stopped.
- `Engine.Ready()` now only closes once the engine is listening for gRPC
  requests, if the gRPC server is enabled.
- The database schema has changed. Call `CreateSchema()` to update a database
  created by an earlier version before running this version.
- `ProjectionMessageHandler.Compact()` is now called with a 10-minute deadline.
  A compaction that fails or exceeds the deadline is retried after a short
  backoff, rather than at the next compaction interval.
- PostgreSQL 18 is now the minimum supported version.
- Go 1.27 is now the minimum supported version.

### Fixed

- Fixed an issue where resetting a projection's storage did not cause it to be
  rebuilt. Projections now resume from the offset returned by their
  `CheckpointOffset()` method.
- Fixed an issue where `Engine.Run()` panicked when part of the engine failed.
  Failures such as database errors are now logged and retried. `Run()` returns
  an error only when the engine can't start or continue, for example because
  the application's configuration is invalid or the engine can't listen on its
  address.

## [0.1.2] - 2026-07-04

### Added

- Added `CreateSchema()` and `DropSchema()`

## [0.1.1] - 2026-07-04

- Rename root package to `runkit`.

## [0.1.0] - 2026-07-04

- Initial release

<!-- references -->

[unreleased]: https://github.com/dogmatiq/runkit/compare/v0.1.2...HEAD
[0.1.0]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.0
[0.1.1]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.1
[0.1.2]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.2

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
