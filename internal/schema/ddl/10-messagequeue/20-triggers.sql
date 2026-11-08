--------------------------------------------------------------------------------
-- The "signal_add" trigger sends a "messagequeue" signal through the signal bus
-- in internal/signal when messages are added to a queue. The Catalog type in
-- internal/messagequeue watches for it to learn which queues have messages.
--
-- The signal's subtopic is the queue ID, so that only the node responsible for
-- the queue needs to look. PostgreSQL delivers identical notifications sent
-- within one transaction only once, so a transaction that adds many messages to
-- one queue produces a single signal.
--
-- Removing a message signals nothing, since every delivery removes one and
-- every node receives every signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION messagequeue.signal_add()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'messagequeue.' || queue_id)
    FROM (SELECT DISTINCT queue_id FROM inserted) AS queues;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_add
AFTER INSERT ON messagequeue.messages
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION messagequeue.signal_add();

--------------------------------------------------------------------------------
-- The "signal_move" trigger sends a "messagequeue" signal through the signal
-- bus in internal/signal when messages move from one queue to another. The
-- Catalog type in internal/messagequeue watches for it to learn which queues
-- have messages.
--
-- It sends one for both the queue each message left and the queue it joined. An
-- update that leaves a message in the same queue signals nothing.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION messagequeue.signal_move()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'messagequeue.' || queue_id)
    FROM (
        SELECT d.queue_id
        FROM deleted AS d
        INNER JOIN inserted AS i ON i.message_id = d.message_id
        WHERE i.queue_id != d.queue_id
        UNION
        SELECT i.queue_id
        FROM inserted AS i
        INNER JOIN deleted AS d ON d.message_id = i.message_id
        WHERE i.queue_id != d.queue_id
    ) AS queues;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_move
AFTER UPDATE ON messagequeue.messages
REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION messagequeue.signal_move();
