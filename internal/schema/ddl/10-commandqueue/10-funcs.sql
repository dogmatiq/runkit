CREATE SCHEMA IF NOT EXISTS commandqueue;

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
