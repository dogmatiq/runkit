--------------------------------------------------------------------------------
-- The "add" function enqueues a command for execution.
--
-- If idempotency_key is empty, the command is always inserted and
-- (message_id, TRUE) is returned.
--
-- If idempotency_key is non-empty and has not been seen before, the command is
-- inserted and (message_id, TRUE) is returned.
--
-- If idempotency_key has been seen before, the command is discarded and
-- (original_message_id, FALSE) is returned, where original_message_id is the
-- ID of the message that originally claimed the key.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION commandqueue.add(
    message_id       uuid,
    correlation_id   uuid,
    message_type_id  uuid,
    envelope         bytea,
    idem_key         text
)
RETURNS TABLE (actual_message_id uuid, enqueued boolean)
LANGUAGE plpgsql
AS $$
BEGIN
    IF add.idem_key != '' THEN
        INSERT INTO commandqueue.idempotency_keys (
            idempotency_key,
            message_id
        )
        VALUES (
            add.idem_key,
            add.message_id
        )
        ON CONFLICT (idempotency_key) DO NOTHING;

        IF NOT FOUND THEN
            RETURN QUERY
            SELECT k.message_id, FALSE
            FROM commandqueue.idempotency_keys k
            WHERE k.idempotency_key = add.idem_key;
            RETURN;
        END IF;
    END IF;

    INSERT INTO commandqueue.commands (
        message_id,
        correlation_id,
        message_type_id,
        envelope
    ) VALUES (
        add.message_id,
        add.correlation_id,
        add.message_type_id,
        add.envelope
    );

    RETURN QUERY SELECT add.message_id, TRUE;
END;
$$;

--------------------------------------------------------------------------------
-- The "remove" function deletes a command from the queue by its message ID.
--
-- It raises an exception if the command does not exist.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION commandqueue.remove(
    message_id uuid
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM commandqueue.commands AS c
    WHERE c.message_id = remove.message_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'command % does not exist in the queue', message_id;
    END IF;
END;
$$;

--------------------------------------------------------------------------------
-- The "postpone" function postpones a command by rescheduling it after a fixed
-- delay without incrementing its failure count.
--
-- It raises an exception if the command does not exist.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION commandqueue.postpone(
    message_id      uuid,
    backoff_base_ms bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE commandqueue.commands SET
        execute_at = clock_timestamp() + postpone.backoff_base_ms * interval '1 millisecond'
    WHERE commandqueue.commands.message_id = postpone.message_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'command % does not exist in the queue', message_id;
    END IF;
END;
$$;

--------------------------------------------------------------------------------
-- The "fail_and_postpone" function records a failure against a command and
-- postpones it by rescheduling it after an exponentially increasing delay based
-- on its failure count.
--
-- It raises an exception if the command does not exist.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION commandqueue.fail_and_postpone(
    message_id       uuid,
    backoff_base_ms  bigint,
    backoff_limit_ms bigint
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE commandqueue.commands SET
        failures = failures + 1,
        execute_at = clock_timestamp() + LEAST(
            pow(2, failures) * fail_and_postpone.backoff_base_ms,
            fail_and_postpone.backoff_limit_ms
        ) * interval '1 millisecond'
    WHERE commandqueue.commands.message_id = fail_and_postpone.message_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'command % does not exist in the queue', message_id;
    END IF;
END;
$$;
