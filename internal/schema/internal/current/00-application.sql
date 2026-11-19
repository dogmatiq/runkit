--------------------------------------------------------------------------------
-- Lock the three tables below, in the order in which the engine's transactions
-- lock them. They are the tables that have an index or a trigger created by the
-- files in this directory. This is the first file that runs, so no statement
-- locks any of these tables before this block does.
--
-- An upgrade runs these files while engines are running. CREATE INDEX and
-- CREATE OR REPLACE TRIGGER lock their table even when the index or trigger
-- already exists. An engine's delivery locks "queued_messages", then
-- "event_streams", then "events". Two transactions that lock the same tables in
-- different orders can each hold one lock while waiting for the other's, and
-- PostgreSQL ends one of them with a deadlock error. Locking the tables here,
-- in the engine's order, prevents that.
--
-- SHARE ROW EXCLUSIVE is at least as strong as the lock that any later
-- statement takes on these tables, so none of them waits for a lock after this
-- block. A table that doesn't exist yet is skipped, as on a new database.
--------------------------------------------------------------------------------
DO $$
DECLARE
    locked_table text;
BEGIN
    FOREACH locked_table IN ARRAY ARRAY[
        'runkit.queued_messages',
        'runkit.event_streams',
        'runkit.events'
    ] LOOP
        IF to_regclass(locked_table) IS NOT NULL THEN
            EXECUTE format(
                'LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE',
                locked_table
            );
        END IF;
    END LOOP;
END $$;

--------------------------------------------------------------------------------
-- The "identity" table contains the identity of the application that the
-- database is bound to.
--
-- The "singleton" column ensures that the table contains at most one row.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.identity (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    key       uuid    NOT NULL,
    name      text    NOT NULL CHECK (name != '')
);
