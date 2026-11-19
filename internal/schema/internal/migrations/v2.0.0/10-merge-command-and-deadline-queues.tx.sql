--------------------------------------------------------------------------------
-- Lock the old tables before reading them. The migration then waits for the
-- transactions that are already using them, so each copy below sees their
-- results, and a write that an engine still running attempts later fails
-- instead of being lost when the tables are dropped.
--------------------------------------------------------------------------------
LOCK TABLE
    commandqueue.commands,
    commandqueue.idempotency_keys,
    process.deadlines
IN ACCESS EXCLUSIVE MODE;

--------------------------------------------------------------------------------
-- The "queued_messages" table is created as it is in "current/", which is
-- applied after this migration and doesn't alter tables that already exist.
-- The "runkit" schema already exists, because "bootstrap.sql" creates it before
-- any migration.
--------------------------------------------------------------------------------
CREATE TABLE runkit.queued_messages (
    message_id              uuid        PRIMARY KEY,
    message_type_id         uuid        NOT NULL,
    queue_id                uuid        NOT NULL,
    handler_key             uuid,
    instance_id             text        CHECK (instance_id != ''),
    envelope                bytea       NOT NULL,
    failures                int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
    first_failed_at         timestamptz,
    failure_logged_as_error boolean     NOT NULL DEFAULT false,
    scheduled_for           timestamptz NOT NULL,
    next_attempt_at         timestamptz NOT NULL,

    CHECK ((handler_key IS NULL) = (instance_id IS NULL)),
    CHECK ((failures = 0) = (first_failed_at IS NULL)),
    CHECK (failures > 0 OR NOT failure_logged_as_error)
);

CREATE INDEX queued_messages_by_queue
ON runkit.queued_messages (
    queue_id,
    message_type_id,
    next_attempt_at
);

CREATE INDEX queued_messages_by_instance
ON runkit.queued_messages (
    handler_key,
    instance_id
)
WHERE handler_key IS NOT NULL;

--------------------------------------------------------------------------------
-- Move the pending commands and deadlines into "runkit.queued_messages".
--
-- A new message's queue ID is the ID of the node that produced it, but that
-- isn't available here. The old tables don't record which node produced each
-- message, and every node has a new ID once it restarts. Instead, the queue ID
-- of each command is its message type ID, and the queue ID of each deadline is
-- its handler's key. That keeps the number of queues small, while spreading the
-- backlog across the live nodes rather than giving it all to one.
--
-- A migrated message that has failed gets the time of its next attempt as its
-- "first_failed_at", because the time of its first failure isn't known.
--
-- A migrated deadline gets the time of its next attempt as its
-- "scheduled_for", because its scheduled time is only in its envelope.
--------------------------------------------------------------------------------
INSERT INTO runkit.queued_messages (
    message_id,
    message_type_id,
    queue_id,
    envelope,
    failures,
    first_failed_at,
    scheduled_for,
    next_attempt_at
)
SELECT
    message_id,
    message_type_id,
    message_type_id,
    envelope,
    failures,
    CASE WHEN failures > 0 THEN deliver_at END,
    enqueued_at,
    deliver_at
FROM commandqueue.commands;

--------------------------------------------------------------------------------
-- Move the idempotency keys table as it is, because it has the same columns and
-- constraints as the new one, and it keeps every key after its command has been
-- delivered, so copying its rows would take time in proportion to the history
-- of the application, not to its backlog.
--
-- It's renamed first, and so are the constraints that PostgreSQL named after
-- it, to match the names in a new database.
--------------------------------------------------------------------------------
ALTER TABLE commandqueue.idempotency_keys RENAME TO command_idempotency_keys;
ALTER TABLE commandqueue.command_idempotency_keys RENAME CONSTRAINT idempotency_keys_pkey TO command_idempotency_keys_pkey;
ALTER TABLE commandqueue.command_idempotency_keys RENAME CONSTRAINT idempotency_keys_idempotency_key_not_null TO command_idempotency_keys_idempotency_key_not_null;
ALTER TABLE commandqueue.command_idempotency_keys RENAME CONSTRAINT idempotency_keys_idempotency_key_check TO command_idempotency_keys_idempotency_key_check;
ALTER TABLE commandqueue.command_idempotency_keys RENAME CONSTRAINT idempotency_keys_message_id_not_null TO command_idempotency_keys_message_id_not_null;
ALTER TABLE commandqueue.command_idempotency_keys RENAME CONSTRAINT idempotency_keys_message_id_key TO command_idempotency_keys_message_id_key;
ALTER TABLE commandqueue.command_idempotency_keys SET SCHEMA runkit;

DROP TABLE commandqueue.commands;
DROP FUNCTION commandqueue.add(uuid, uuid, bytea, text);
DROP FUNCTION commandqueue.remove(uuid);

INSERT INTO runkit.queued_messages (
    message_id,
    message_type_id,
    queue_id,
    handler_key,
    instance_id,
    envelope,
    failures,
    first_failed_at,
    scheduled_for,
    next_attempt_at
)
SELECT
    message_id,
    message_type_id,
    handler_key,
    handler_key,
    instance_id,
    envelope,
    failures,
    CASE WHEN failures > 0 THEN deliver_at END,
    deliver_at,
    deliver_at
FROM process.deadlines;

DROP TABLE process.deadlines;

--------------------------------------------------------------------------------
-- Add the queue that a process instance's deadlines are added to.
--------------------------------------------------------------------------------
ALTER TABLE process.instances
ADD COLUMN deadline_queue_id uuid;

--------------------------------------------------------------------------------
-- Drop the functions that "current/" replaces with different argument lists,
-- because CREATE OR REPLACE FUNCTION would add an overload instead of
-- replacing them, and the functions that nothing calls anymore.
--------------------------------------------------------------------------------
DROP FUNCTION aggregate.complete_without_events(uuid, uuid, text);
DROP FUNCTION aggregate.complete_with_events(uuid, uuid, text, uuid, uuid, bytea, eventstream.event[]);
DROP FUNCTION integration.complete_without_events(uuid);
DROP FUNCTION integration.complete_with_events(uuid, uuid, eventstream.event[]);
