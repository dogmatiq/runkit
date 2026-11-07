--------------------------------------------------------------------------------
-- Wake a handler's deadline consumer after newly scheduled deadlines commit.
-- Statement triggers cover every insert path; duplicate notifications within a
-- transaction are coalesced by PostgreSQL.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION process.signal_deadlines()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'process.deadlines.' || handler_key)
    FROM (SELECT DISTINCT handler_key FROM inserted) AS handlers;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_deadlines
AFTER INSERT ON process.deadlines
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION process.signal_deadlines();
