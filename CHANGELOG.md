# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs
[dogma's process handler documentation]: https://pkg.go.dev/github.com/dogmatiq/dogma#ProcessMessageHandler
[dogmatiq/protobuf]: https://github.com/dogmatiq/protobuf
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[major schema upgrades]: https://github.com/dogmatiq/runkit/blob/main/README.md#major-schema-upgrades
[pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

Schema version: v2.0.0 (new)

This release gives the database schema its own version number. The engine now
applies the schema automatically when it starts.

> [!IMPORTANT]
> This release makes breaking changes to the engine's database schema, so
> upgrading from an earlier runkit version needs downtime. Follow the steps in
> [major schema upgrades].

This release also completes support for consuming events across applications.
Process handlers can now consume events recorded by applications running in
other clusters, as projections already could.

> [!WARNING]
> Earlier versions silently ignored process handler routes for events from
> remote applications. Once you upgrade, those routes take effect, and the
> handlers receive every matching event, old and new. Before you upgrade, check
> that they can safely act on old events.

The engine now puts less load on the database when it delivers commands and
deadlines. Each engine used to poll the database for commands and deadlines to
deliver, and every engine in a cluster competed for the same messages. The
engines now divide the messages between them, and a database notification wakes
the responsible engine as soon as a command or deadline is added.

### Added

- Added the ability for process handlers to consume events from remote
  applications.
- Added schema versioning and migration:
  - The schema now has its own version number, separate from runkit's, and the
    release notes of each runkit version start with the schema version that it
    needs.
  - `Engine.Run()` now creates or upgrades the schema when the engine starts,
    but only makes the changes that are safe while other engines are running.
  - `ApplySchemaOnline()` makes the same changes as `Engine.Run()`, for an
    engine whose database role isn't allowed to change the schema.
  - `ApplySchemaOffline()` applies the schema changes that can't be made while
    engines are running.
- Added `FailurePolicy`, which controls how long the engine waits before
  retrying failed work, and when repeated failures are logged as an ERROR. The
  policy for each kind of work defaults to `DefaultFailurePolicy()`, and these
  options change it:
  - `WithEngineFailurePolicy()`
  - `WithMessageQueueDeliveryFailurePolicy()`
  - `WithEventStreamDispatchFailurePolicy()`
  - `WithProjectionCompactionFailurePolicy()`
- Added `WithProjectionCompactTimeout()`, which sets how long each attempt to
  compact a projection may take. It defaults to 10 minutes, as before.
- Added `WithMessageQueueContentionDelay()`, which sets how long the engine
  waits before retrying a command or deadline that can't be delivered until
  another delivery finishes, such as a second command for the same aggregate
  instance.
- Added support for the `ListEventStreams` and `ReadEventSegment` calls of the
  [dogmatiq/protobuf] `EventStreamConsumerAPI` service to the engine's gRPC
  server.
- Added the length of the event stream to the segments that the engine's gRPC
  server sends for `WatchEvents` and `ReadEvents`.

### Changed

- **[BC]** Updated to Dogma v0.26.0.
- **[BC]** Changed the database schema. Engines from earlier runkit versions
  can't use the new schema. The upgrade copies every command and deadline that's
  waiting to be delivered, so the more there are, the longer it takes. The
  engine's tables, functions and triggers have moved from several PostgreSQL
  schemas into a single `runkit` schema, and `DropSchema()` now drops only that
  schema.
- **[BC]** Added timeouts to the engine's calls to application handlers. A call
  that times out counts as a failure, and is retried later. Handling a message
  times out after one minute, and calls to `RouteEventToInstance()` and
  `CheckpointOffset()` time out after 10 seconds. Use `WithHandleTimeout()` and
  `WithRouteTimeout()` to change these limits.
- **[BC]** Changed `DefaultProjectionCompactInterval` from a constant to a
  function.
- **[BC]** Changed the `add` parameter of `RemoteEngineDiscoverer`, which is the
  function that a discoverer calls to add a remote engine's connection. The
  function now takes a description of the remote engine as its first argument,
  and the engine uses the description to identify the remote engine in its logs.
  `StaticRemoteEngines()` uses each connection's target as its description.
- Changed the limit on how many commands and deadlines an engine delivers at
  once. It used to apply to each handler separately, and now applies to all of
  them together, so an engine with more than one aggregate, integration or
  process handler may deliver fewer at once. The limit defaults to 10 ×
  `GOMAXPROCS`. Use `WithMessageQueueConcurrency()` to raise it.
- `Engine.Run()` now configures the database's connection pool, replacing the
  application's settings for open and idle connections. It removes the limit on
  open connections, so the limit set by `WithMessageQueueConcurrency()` decides
  how many connections the engine uses for deliveries.
- Changed how the engine retries and logs failures:
  - When a retry might succeed, the engine now waits at most 15 seconds between
    attempts, down from five minutes.
  - When a retry is unlikely to succeed, such as after a panic in application
    code, the engine still waits up to five minutes.
  - When the same work keeps failing, the engine now logs at most one of the
    failures as an ERROR and the rest as DEBUG, instead of logging every
    failure as an ERROR.
  - When the work succeeds again, the engine logs an INFO record that says how
    many attempts failed and for how long. If none of the failures was logged
    as an ERROR, this record is DEBUG too.
  - A custom `FailurePolicy` can change these delays and log levels.
- Changed what the engine logs:
  - Every log record now has a `layer` attribute, set to `application` or
    `engine`.
  - Every log record now includes the application's identity.
  - The `component` attribute has been removed.
  - The records of what a handler did while handling a message, such as each
    message it produced and each call to its scope's `Log()` method, are now
    written only if handling succeeds.
  - When the application can't unmarshal an aggregate instance's snapshot, the
    engine now logs a WARN instead of an ERROR. It still handles the command, by
    replaying all of the instance's events.
  - When the application can't marshal an aggregate instance's snapshot, the
    engine now logs a DEBUG record instead of an ERROR. It saves the instance
    without a snapshot.

### Removed

- **[BC]** Removed `CreateSchema()`. `Engine.Run()` now creates or upgrades the
  schema, so call `ApplySchemaOnline()` beforehand only if the engine's database
  role isn't allowed to change the schema.

### Fixed

- New process handlers missed the events recorded before they were deployed.
  They now handle the full history, as [Dogma's process handler documentation]
  requires.
- Ending a process instance while one of its deadlines was being delivered
  could fail with an error. Now the instance ends, and the deadline is canceled
  without calling the handler.
- When a process root's `MarshalBinary()` method failed after handling a
  deadline, the commands that the process executed were queued on every
  attempt, even though nothing else was saved, so they were duplicated. They're
  now queued only if the rest of the process's changes are saved too.
- **[BC]** Event observers passed to `Engine.ExecuteCommand()` had these
  problems:
  - An observer received the same events again on every poll, and never
    received events beyond the first 100. Now it receives each event once.
  - While the call waited, the engine queried the database every
    25 milliseconds, and each query could read every event that the engine had
    recorded. Now the observer receives each event as soon as it's recorded,
    and the engine queries the database again when the command causes another
    event.
  - When the context's deadline passed before an observer was satisfied, the
    call returned `dogma.ErrEventObserverNotSatisfied`, which Dogma reserves for
    when the engine has determined that no more relevant events can occur. It
    now returns `context.DeadlineExceeded`.
  - If the engine stopped while the call waited, the call went on waiting. It
    now returns an error.

## [0.2.0] - 2026-11-06

Schema version: v1.1.0 (new)

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
- Fixed an issue where a panic in a message's `UnmarshalBinary()` or
  `Validate()` method crashed the engine when the message was delivered to an
  aggregate, integration or process handler. The engine now handles the message
  as one that it can't unmarshal.

## [0.1.2] - 2026-07-04

Schema version: v1.0.0 (existing)

### Added

- Added `CreateSchema()` and `DropSchema()`

## [0.1.1] - 2026-07-04

Schema version: v1.0.0 (existing)

- Rename root package to `runkit`.

## [0.1.0] - 2026-07-04

Schema version: v1.0.0 (new)

- Initial release

<!-- references -->

[unreleased]: https://github.com/dogmatiq/runkit
[0.1.0]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.0
[0.1.1]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.1
[0.1.2]: https://github.com/dogmatiq/runkit/releases/tag/v0.1.2
[0.2.0]: https://github.com/dogmatiq/runkit/releases/tag/v0.2.0

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
