CREATE SCHEMA IF NOT EXISTS messagequeue;

--------------------------------------------------------------------------------
-- The "messages" table holds the commands and deadlines that have not yet been
-- delivered.
--
-- Each message belongs to a queue. A queue is the set of messages that a single
-- node delivers, and its ID is the rendezvous key that decides which node that
-- is. There is no table of queues, since a queue has no state of its own. A
-- deadline also has the handler key and instance ID of the process instance
-- it is delivered to.
--
-- "scheduled_for" is when the message was meant to be delivered, and never
-- changes. "next_attempt_at" is the earliest time the next attempt may run, and
-- moves later each time an attempt is postponed.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messagequeue.messages (
    message_id      uuid        PRIMARY KEY,
    message_type_id uuid        NOT NULL,
    queue_id        uuid        NOT NULL,
    handler_key     uuid,
    instance_id     text        CHECK (instance_id != ''),
    envelope        bytea       NOT NULL,
    failures        int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
    failing_since   timestamptz,
    scheduled_for   timestamptz NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CHECK ((handler_key IS NULL) = (instance_id IS NULL)),
    CHECK ((failures = 0) = (failing_since IS NULL))
);

-- Create an index for finding the next message to deliver in each queue, for
-- each message type that is routed to a handler.
CREATE INDEX IF NOT EXISTS messages_by_queue
ON messagequeue.messages (
    queue_id,
    message_type_id,
    next_attempt_at
);

-- Create an index for finding the deadlines of a process instance.
CREATE INDEX IF NOT EXISTS messages_by_instance
ON messagequeue.messages (
    handler_key,
    instance_id
)
WHERE handler_key IS NOT NULL;

--------------------------------------------------------------------------------
-- The "idempotency_keys" table is a write-only list of idempotency keys
-- specified when executing a command with the dogma.WithIdempotencyKey()
-- option.
--
-- It is used to deduplicate commands with the same key, even after they have
-- been delivered and deleted from the "messages" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS messagequeue.idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != ''),
    message_id      uuid NOT NULL UNIQUE
);
