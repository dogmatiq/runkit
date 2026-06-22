# Advisory locks for MinimizeConcurrency locking

Use PostgreSQL advisory locks instead of `SELECT FOR UPDATE` row locking to
serialize projection handlers that prefer `MinimizeConcurrency`.

Advisory locks avoid holding row-level locks for the duration of event handling,
which could reduce contention on `handler_checkpoints` rows and simplify the
interaction between the stream task's checkpoint update and the concurrency
serialization concern.

## Possibly related

- `internal/projection/streamtask.go` — `acquireLock` method
