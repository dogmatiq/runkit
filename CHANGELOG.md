# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- Added `New()` and the `WithLogger()`, `WithListenAddress()` and
  `WithProjectionCompactInterval()` options.
- Added a gRPC server so that engines running other applications can consume
  this application's events. It is disabled by default. Use
  `WithListenAddress()` to enable it. As with any gRPC server, it is insecure
  unless transport credentials are supplied using `grpc.Creds()`.
- Added `Engine.ListenAddr()`.
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
- PostgreSQL 18 is now the minimum supported version.
- Removed some of the conditions under which `Engine.Run()` panics. It now
  returns an error in more cases where the engine is unable to continue.
- `Engine.ExecuteCommand()` now returns an error once the engine has stopped.
- The database schema has changed. Call `CreateSchema()` to update a database
  created by an earlier version.
- `ProjectionMessageHandler.Compact()` is now called with a 10-minute deadline.
  A compaction that fails or exceeds the deadline is retried after a short
  backoff, rather than at the next compaction interval.

### Fixed

- Fixed an issue where resetting a projection's storage did not cause it to be
  rebuilt. Projections now resume from the offset returned by their
  `CheckpointOffset()` method.

## [0.1.2] - 2026-07-04

### Added

- Added `CreateSchema()` and `DropSchema()`

## [0.1.1] - 2026-07-04

- Rename root package to `runkit`.

## [0.1.0] - 2026-07-04

- Initial release

<!-- references -->

[unreleased]: https://github.com/dogmatiq/runkit
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
