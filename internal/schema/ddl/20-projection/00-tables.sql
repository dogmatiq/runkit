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

--------------------------------------------------------------------------------
-- The "compaction" table coordinates compaction across multiple engine
-- instances.
--
-- A row is inserted for each handler and locked with SELECT FOR UPDATE SKIP
-- LOCKED to ensure only one instance compacts at a time.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projection.compaction (
    handler_key       uuid PRIMARY KEY,
    last_compacted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
