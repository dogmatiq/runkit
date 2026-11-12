--------------------------------------------------------------------------------
-- The "add_command" function queues a command.
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
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION messagequeue.add_command(
    message_id      uuid,
    message_type_id uuid,
    queue_id        uuid,
    envelope        bytea,
    idem_key        text
)
RETURNS TABLE (actual_message_id uuid, enqueued boolean)
LANGUAGE plpgsql
AS $$
BEGIN
    IF add_command.idem_key != '' THEN
        INSERT INTO messagequeue.idempotency_keys (
            idempotency_key,
            message_id
        )
        VALUES (
            add_command.idem_key,
            add_command.message_id
        )
        ON CONFLICT (idempotency_key) DO NOTHING;

        IF NOT FOUND THEN
            RETURN QUERY
            SELECT k.message_id, FALSE
            FROM messagequeue.idempotency_keys k
            WHERE k.idempotency_key = add_command.idem_key;
            RETURN;
        END IF;
    END IF;

    INSERT INTO messagequeue.messages (
        message_id,
        message_type_id,
        queue_id,
        envelope
    ) VALUES (
        add_command.message_id,
        add_command.message_type_id,
        add_command.queue_id,
        add_command.envelope
    );

    RETURN QUERY SELECT add_command.message_id, TRUE;
END;
$$;
