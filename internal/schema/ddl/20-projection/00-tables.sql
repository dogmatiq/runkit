CREATE SCHEMA IF NOT EXISTS projection;

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
