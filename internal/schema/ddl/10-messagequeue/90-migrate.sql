--------------------------------------------------------------------------------
-- Move the pending commands and deadlines of a database created by any version
-- from v0.1.0 through v0.2.0 into "messagequeue.messages".
--
-- In those versions, commands were stored in "commandqueue.commands" and
-- deadlines in "process.deadlines", whose columns are the same in every one of
-- them.
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
--
-- This file does nothing on a database that never had those tables. Once no
-- such database remains, deleting this file and TestCreate_migratesQueues is
-- the whole cleanup.
--------------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('commandqueue.commands') IS NOT NULL THEN
        INSERT INTO messagequeue.messages (
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

        INSERT INTO messagequeue.idempotency_keys (
            idempotency_key,
            message_id
        )
        SELECT
            idempotency_key,
            message_id
        FROM commandqueue.idempotency_keys;

        DROP SCHEMA commandqueue CASCADE;
    END IF;

    IF to_regclass('process.deadlines') IS NOT NULL THEN
        INSERT INTO messagequeue.messages (
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
    END IF;
END;
$$;
