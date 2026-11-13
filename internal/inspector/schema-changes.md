# Schema changes that would help the inspector

The inspector reads the engine's tables as they are, and never writes. This
document lists indexes and small schema changes that would make its queries
cheaper, or let it answer questions that it can't answer now. None of them are
needed for the engine itself, and each one has a cost to the engine, which is
noted with it.

## Event streams

### An index on `eventstream.events.recorded_at`

Nothing indexes `recorded_at`, so the inspector can't cheaply ask how many
events were recorded in a period of time. Instead, it remembers each stream's
length between its own readings, and counts the events between two offsets.
That works only while the inspector is running, so its rate charts start empty
and the count of active aggregate instances needs 30 seconds of history.

A [BRIN index][brin] on `recorded_at` would answer these questions directly.
Events are appended in roughly the order of `recorded_at`, which is the case
that BRIN indexes are made for. A BRIN index is very small and adds little work
to each insert.

### The recording handler's key on every event

`eventstream.events.aggregate_handler_key` is set only for events that an
aggregate records. To find the handler that recorded an integration's event,
the inspector looks up the event's type in the linked application's
configuration. Dogma allows only one handler to record each event type, so this
works, but only for a tool that links the application.

A `handler_key` column that is set for every event would make per-handler counts
a plain `GROUP BY`. It costs 16 bytes for each event.

## Message queues

### The handler's key on queued commands

`messagequeue.messages.handler_key` is set only for deadlines. To find the
handler of a queued command, the inspector looks up the command's type in the
linked application's configuration, as above. Setting `handler_key` for
commands as well would make the backlog of each handler a plain `GROUP BY`.

### `failures` in the index on queues

The inspector's summary of the queues counts the messages in each queue, and
how many of them are due and failing. It reads every row of
`messagequeue.messages` to do it. The table holds only undelivered messages, so
it's usually small, but it's largest exactly when a backlog builds.

Adding `failures` to `messages_by_queue` with `INCLUDE (failures)` would let
PostgreSQL answer the summary from the index alone, once VACUUM has marked the
pages as visible to all transactions. It makes the index slightly larger.

### A count of delivered messages

The inspector's rate of messages per minute comes from PostgreSQL's count of
rows deleted from `messagequeue.messages`, in `pg_stat_user_tables`. That count
covers every queue and handler at once, includes deadlines removed when their
process instance ended, and includes rows deleted by transactions that rolled
back.

A count of deliveries for each handler, updated in the delivery transaction,
would give an exact rate per handler. It would also make the row for a busy
handler a point of contention between deliveries, so it's probably not worth
it.

## Instances

### A partial index on process instances that are in progress

The inspector counts each process handler's instances that have not ended, and
groups them by deadline queue. Ended instances keep their rows, so this reads
every instance that the handler has ever had.

An index on `process.instances (handler_key, deadline_queue_id) WHERE NOT ended`
would let it read only the instances in progress, from the index alone.

### An index on aggregate instances by event stream

The inspector groups each aggregate handler's instances by the event stream
that they're bound to. The primary key finds the handler's instances, but the
stream ID is only in the table rows.

An index on `aggregate.instances (handler_key, stream_id)` would let it answer
from the index alone.

## Nodes

### Each node's settings

The inspector can't see any node's settings, such as its heartbeat interval or
its interval between projection compactions, and each node can have different
settings. So it can't tell whether a compaction is overdue, and it assumes the
engine's default heartbeat timeout.

A table in which each node records its settings when it starts would let the
inspector show them, and judge each node against its own settings.

<!-- references -->

[brin]: https://www.postgresql.org/docs/current/brin.html
