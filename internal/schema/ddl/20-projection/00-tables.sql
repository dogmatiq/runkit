CREATE SCHEMA IF NOT EXISTS projection;

--------------------------------------------------------------------------------
-- The "handlers" table provides handler-level locking for projection handlers
-- that prefer minimized concurrency.
--
-- A row is inserted for each handler and then locked with a blocking SELECT FOR
-- UPDATE to serialize event handling.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projection.handlers (
    handler_key uuid PRIMARY KEY
);
