--------------------------------------------------------------------------------
-- The "enqueue_command" function queues a command.
--
-- If idem_key is empty, the command is always queued and (message_id, TRUE) is
-- returned.
--
-- If idem_key is non-empty and has not been seen before, the command is queued
-- and (message_id, TRUE) is returned.
--
-- If idem_key has been seen before, the command is discarded and
-- (original_message_id, FALSE) is returned, where original_message_id is the
-- ID of the message that originally claimed the key.
--
-- queued_at is the time that the command was queued. It's the time at which the
-- command first becomes due, and the caller supplies it from its own clock
-- because the consumer compares next_attempt_at with that clock, not the
-- database's.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.enqueue_command(
    message_id      uuid,
    message_type_id uuid,
    queue_id        uuid,
    envelope        bytea,
    idem_key        text,
    queued_at       timestamptz
)
RETURNS TABLE (actual_message_id uuid, enqueued boolean)
LANGUAGE plpgsql
AS $$
BEGIN
    IF enqueue_command.idem_key != '' THEN
        INSERT INTO runkit.command_idempotency_keys (
            idempotency_key,
            message_id
        )
        VALUES (
            enqueue_command.idem_key,
            enqueue_command.message_id
        )
        ON CONFLICT (idempotency_key) DO NOTHING;

        IF NOT FOUND THEN
            RETURN QUERY
            SELECT k.message_id, FALSE
            FROM runkit.command_idempotency_keys k
            WHERE k.idempotency_key = enqueue_command.idem_key;
            RETURN;
        END IF;
    END IF;

    INSERT INTO runkit.queued_messages (
        message_id,
        message_type_id,
        queue_id,
        envelope,
        scheduled_for,
        next_attempt_at
    ) VALUES (
        enqueue_command.message_id,
        enqueue_command.message_type_id,
        enqueue_command.queue_id,
        enqueue_command.envelope,
        enqueue_command.queued_at,
        enqueue_command.queued_at
    );

    RETURN QUERY SELECT enqueue_command.message_id, TRUE;
END;
$$;
