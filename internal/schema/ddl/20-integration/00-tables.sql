CREATE SCHEMA IF NOT EXISTS integration;

--------------------------------------------------------------------------------
-- The "handlers" table provides handler-level locking for integration handlers
-- that prefer minimized concurrency.
--
-- A row is inserted for each handler and then locked with a blocking SELECT FOR
-- UPDATE to serialize command handling.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration.handlers (
    handler_key uuid PRIMARY KEY
);
