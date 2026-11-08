--------------------------------------------------------------------------------
-- Move the pending commands and deadlines of a v0.2.0 database into
-- "messagequeue.messages".
--
-- Until v0.2.0, commands were queued in "commandqueue.commands" and deadlines
-- in "process.deadlines". The queue ID of each command is its message type ID,
-- and the queue ID of each deadline is its handler's key, so that each is
-- delivered by the node responsible for that key. A message that had already
-- failed is treated as failing since its next attempt, which is also the
-- scheduled time of a deadline, because the real scheduled time is inside the
-- envelope.
--
-- This file does nothing on a database that never had those tables. Once no
-- v0.2.0 database remains, deleting this file and TestCreate_migratesQueues is
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
            failing_since,
            scheduled_at,
            deliver_at
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
            failing_since,
            scheduled_at,
            deliver_at
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
