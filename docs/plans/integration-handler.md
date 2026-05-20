# Integration Handler Implementation Plan

## Overview

Implement the integration handler type, which connects a Dogma application to
external systems. Integration handlers consume commands and may record events to
describe the outcome.

## Phase 1 -- Refactor command queue

Decouple aggregate-specific routing from the shared command queue.

### Schema changes (schema.sql)

Remove from `pending_commands`:

- `routed_to_handler_key` column
- `routed_to_aggregate_instance_id` column
- `has_complete_route` constraint
- `fk_aggregate_instance` FK constraint
- `idx_pending_commands_by_route` index
- `WHERE routed_to_handler_key IS NULL` filter on `idx_pending_commands_by_type`

Add new table:

```sql
CREATE TABLE IF NOT EXISTS aggregate_command_routes (
    message_id  uuid PRIMARY KEY REFERENCES pending_commands(message_id),
    handler_key uuid NOT NULL,
    instance_id text NOT NULL CHECK (instance_id != ''),
    FOREIGN KEY (handler_key, instance_id) REFERENCES aggregate_instances(handler_key, instance_id)
);
```

Column names are chosen to allow joins via `USING` with `pending_commands`,
`aggregate_instances`.

### API changes (commandqueue package)

Remove:

- `Acquire()`
- `Route()`
- `Reset()`

Keep:

- `Enqueue()`
- `Ack()`
- `Nack()`
- `NextAttemptByCorrelationID()`

### Aggregate package changes

Move claim/route/reset logic into the `aggregate` package as unexported
functions or methods. The aggregate controller now:

1. Finds unrouted commands using `NOT EXISTS` against `aggregate_command_routes`.
2. Routes them (inserts into `aggregate_command_routes`).
3. Workers claim routed commands by joining `pending_commands`,
   `aggregate_command_routes`, and `aggregate_instances` (with `FOR UPDATE SKIP
LOCKED` on the instance row).
4. Reset (for reroute) deletes the routing row.

### Exit criteria

All existing tests pass with no behavior change.

## Phase 2 -- Concurrency lock infrastructure

### New package: internal/concurrency/

Provides a shared locking primitive for handler types that declare
`MinimizeConcurrency`. Used by integration (now) and projection (later).

### Schema changes (schema.sql)

```sql
CREATE TABLE IF NOT EXISTS concurrency_locks (
    handler_key uuid PRIMARY KEY
);
```

### API

A function that attempts to acquire the lock row within an existing transaction:

```go
func Acquire(ctx context.Context, tx *sql.Tx, handlerKey *uuidpb.UUID) (bool, error)
```

Uses `SELECT ... FROM concurrency_locks WHERE handler_key = $1 FOR UPDATE SKIP
LOCKED`. Returns true if the lock was acquired, false if another transaction
holds it.

The lock row is pre-inserted by the controller at startup via `INSERT ... ON
CONFLICT DO NOTHING`.

## Phase 3 -- Integration controller

### Package: internal/integration/

Files: `controller.go`, `worker.go`, `scope.go`, `doc.go`.

### Controller (controller.go)

- Reads handler config (identity, routes, concurrency preference).
- Starts N workers in an errgroup:
  - `MinimizeConcurrency`: N = 1.
  - `MaximizeConcurrency`: N = hardcoded constant.
- Returns when context is cancelled or a worker returns a fatal error.

### Worker (worker.go)

Each worker runs an independent loop:

1. Begin transaction.
2. If `MinimizeConcurrency`: acquire concurrency lock. If not acquired, roll
   back, sleep 25ms, retry.
3. Claim one command by subscribed message types using `FOR UPDATE SKIP LOCKED`.
   If none available, roll back, sleep 25ms, retry.
4. Unpack the command envelope.
5. Execute `HandleCommand` with the scope.
6. On success: append buffered events to the event stream, ack the command,
   commit.
7. On error: discard buffered events, nack the command (increment attempt count,
   set backoff), commit.
8. Repeat.

Polling interval: 25ms (fixed, matching aggregate).

### Scope (scope.go)

Implements `dogma.IntegrationCommandScope`:

- `RecordEvent(m)` -- buffers the event for later persistence.
- `Now()` -- returns the current time.
- `Log(format, args...)` -- structured logging.

Events are buffered in a slice and persisted atomically with the ack in the
worker's transaction.

### Engine wiring (engine.go)

New case in the handler type switch:

```go
case *config.Integration:
    g.Go(func() error {
        c := &integration.Controller{
            DB:     e.db,
            Packer: e.packer,
            Config: h,
            Logger: logger,
        }
        return c.Run(ctx)
    })
```

### Cleanup

Delete `internal/_todo/_integration/`.

### Testing

Tests follow the existing aggregate test style:

- One `TestController` with subtests via `t.Run`.
- Inline setup, raw SQL where needed.
- `xtesting.WaitQuery` for async assertions.
- Stubs from enginekit with inline configuration.
- Minimal helpers -- tests are documentation.

## Open items

- Worker count (N) for `MaximizeConcurrency` is a hardcoded constant. Revisit
  with connection-pool-aware sizing (divide pool among handlers) once the engine
  matures.
- Rolling deploy behavior when concurrency preference changes between versions:
  brief overlap during the transition is acceptable.
