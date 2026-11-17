CREATE SCHEMA IF NOT EXISTS projection;

--------------------------------------------------------------------------------
-- The "handlers" table keeps track of handlers that exist or have existed
-- within the application.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS projection.handlers (
    handler_key       uuid PRIMARY KEY,
    last_compacted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
