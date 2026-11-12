--------------------------------------------------------------------------------
-- The "signal_add" trigger sends the "messagequeue" signal once, and the
-- "messagequeue.<queue-id>" signal for the queue of each message that is
-- added. messagequeue.Catalog waits for "messagequeue" to learn which queues
-- have messages. messagequeue.Consumer waits for the signal of each queue
-- while its node owns the queue's workload, so messages added to other queues
-- don't wake it.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that adds many messages sends each signal once.
--
-- Removing a message signals nothing, since every delivery removes one.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION messagequeue.signal_add()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', signal)
    FROM (
        SELECT 'messagequeue.' || queue_id AS signal FROM inserted
        UNION
        SELECT 'messagequeue' FROM inserted
    ) AS signals;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_add
AFTER INSERT ON messagequeue.messages
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION messagequeue.signal_add();

--------------------------------------------------------------------------------
-- The "signal_move" trigger sends the "messagequeue" signal once, and the
-- "messagequeue.<queue-id>" signal for each message's new queue. An update
-- that doesn't change a message's queue signals nothing. messagequeue.Consumer
-- waits for these signals as it does for the signals of "signal_add".
--
-- As with a removal, a message's old queue isn't signaled. Moving a message
-- can only make the old queue's next message due later. A consumer that was
-- waiting for the message wakes when the message would have been due, then
-- finds the queue's next message.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION messagequeue.signal_move()
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
        SELECT 'messagequeue.' || queue_id AS signal FROM moved
        UNION
        SELECT 'messagequeue' FROM moved
    ) AS signals;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_move
AFTER UPDATE ON messagequeue.messages
REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION messagequeue.signal_move();
