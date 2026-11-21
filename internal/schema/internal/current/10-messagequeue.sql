--------------------------------------------------------------------------------
-- The "queued_messages" table holds the commands and deadlines that have not
-- yet been delivered.
--
-- Each message belongs to a queue. A queue is the set of messages that a single
-- node delivers, and its ID is the rendezvous key that decides which node that
-- is. There is no table of queues, since a queue has no state of its own. A
-- deadline also has the handler key and instance ID of the process instance
-- it is delivered to.
--
-- "scheduled_for" is when the message was meant to be delivered, and never
-- changes. "next_attempt_at" is the earliest time the next attempt may run, and
-- moves later each time an attempt is postponed. Neither has a default, because
-- the consumer compares "next_attempt_at" with the clock of the engine that
-- attempts the message, so the engine that adds a message must supply the times
-- from its own clock, not the database's.
--
-- "failures", "first_failed_at" and "failure_logged_as_error" are the
-- message's failure streak: the number of attempts that have failed since the
-- message was queued, the time of the first of them, and whether one of them
-- has been logged as an ERROR.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.queued_messages (
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

-- Create an index for finding the next messages to deliver in each queue, in
-- the order in which they're due, and for listing the queues that have
-- messages. It includes each message's type, which lets a search skip a message
-- whose type has no handler on the engine, without reading the table.
CREATE INDEX IF NOT EXISTS queued_messages_by_queue
ON runkit.queued_messages (
    queue_id,
    next_attempt_at,
    message_type_id
);

-- Create an index for finding the deadlines of a process instance.
CREATE INDEX IF NOT EXISTS queued_messages_by_instance
ON runkit.queued_messages (
    handler_key,
    instance_id
)
WHERE handler_key IS NOT NULL;

--------------------------------------------------------------------------------
-- The "command_idempotency_keys" table is a write-only list of idempotency keys
-- specified when executing a command with the dogma.WithIdempotencyKey()
-- option.
--
-- It is used to deduplicate commands with the same key, even after they have
-- been delivered and deleted from the "queued_messages" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.command_idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != ''),
    message_id      uuid NOT NULL UNIQUE
);

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

--------------------------------------------------------------------------------
-- The "signal_message_enqueued" trigger sends the "messages_enqueued" signal
-- once, and the "messages_enqueued.queue.<queue-id>" signal for the queue of each
-- message that is added. messagequeue.Catalog waits for "messages_enqueued" to
-- learn which queues have messages. messagequeue.Consumer waits for the signal
-- of each queue while its node owns the queue's workload, so messages added to
-- other queues don't wake it.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that adds many messages sends each signal once.
--
-- Removing a message signals nothing, since every delivery removes one.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_message_enqueued()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', signal)
    FROM (
        SELECT 'messages_enqueued.queue.' || queue_id AS signal FROM inserted
        UNION
        SELECT 'messages_enqueued' FROM inserted
    ) AS signals;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_message_enqueued
AFTER INSERT ON runkit.queued_messages
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_message_enqueued();

--------------------------------------------------------------------------------
-- The "signal_message_queue_changed" trigger sends the "messages_enqueued"
-- signal once, and the "messages_enqueued.queue.<queue-id>" signal for each message's
-- new queue.
-- An update that doesn't change a message's queue signals nothing.
-- messagequeue.Consumer waits for these signals as it does for the signals of
-- "signal_message_enqueued".
--
-- As with a removal, a message's old queue isn't signaled. Moving a message
-- can only make the old queue's next message due later. A consumer that was
-- waiting for the message wakes when the message would have been due, then
-- finds the queue's next message.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_message_queue_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', signal)
    FROM (
        WITH moved AS (
            SELECT i.queue_id
            FROM deleted AS d
            INNER JOIN inserted AS i ON i.message_id = d.message_id
            WHERE i.queue_id != d.queue_id
        )
        SELECT 'messages_enqueued.queue.' || queue_id AS signal FROM moved
        UNION
        SELECT 'messages_enqueued' FROM moved
    ) AS signals;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_message_queue_changed
AFTER UPDATE ON runkit.queued_messages
REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_message_queue_changed();
