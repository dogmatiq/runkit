--------------------------------------------------------------------------------
-- The "signal_add" trigger sends the "messagequeue.<queue-id>" signal for each
-- queue that messages are added to, and the "messagequeue" signal once.
-- messagequeue.Catalog waits for "messagequeue" to learn which queues have
-- messages. Each queue's own signal lets a node wait only for the queues it's
-- responsible for.
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
-- The "signal_move" trigger sends the "messagequeue.<queue-id>" signal for each
-- queue that a message moved to, and the "messagequeue" signal once. An update
-- that doesn't change a message's queue signals nothing.
--
-- As with a removal, the queue that a message moved from isn't signaled. The
-- message leaving can only make that queue's next message later, which a
-- consumer finds out when the moved message would have been due.
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
