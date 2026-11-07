--------------------------------------------------------------------------------
-- Wake deadline schedulers when pending work changes.
--
-- Notifications are hints: schedulers read authoritative state from the
-- database after waking. Identical notifications in one transaction coalesce
-- and are delivered only on commit.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION process.signal_deadlines()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'process.deadlines');
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_deadlines
AFTER INSERT OR UPDATE OR DELETE ON process.deadlines
FOR EACH STATEMENT
EXECUTE FUNCTION process.signal_deadlines();
