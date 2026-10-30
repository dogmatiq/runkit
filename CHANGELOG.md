# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- Added `New()` and `EngineOption`, with the `WithLogger()`,
  `WithListenAddress()` and `WithProjectionCompactInterval()` options.
- Added a gRPC server that implements `messaginggrpc.EventStreamConsumerAPI`,
  so that other engines can list and consume this engine's event streams. It
  listens on `DefaultListenAddr` (`:50555`) unless `WithListenAddress()` is
  used.
- Added `Engine.ListenAddr()`.
- Engines now send each other a heartbeat once per second using PostgreSQL
  notifications.

### Changed

- **[BC]** The fields of `Engine` are now unexported. Use `New()` to create an
  engine, and `EngineOption` values to configure it.
- `Engine.Run()` no longer panics when part of the engine stops unexpectedly. It
  returns an error instead.
- `Engine.ExecuteCommand()` now returns an error once the engine has stopped.
- The database schema has changed. Call `CreateSchema()` to update a database
  created by an earlier version.

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
