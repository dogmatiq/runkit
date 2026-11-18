--------------------------------------------------------------------------------
-- The "projection_handlers" table keeps track of projection handlers that exist
-- or have existed within the application.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.projection_handlers (
    handler_key       uuid PRIMARY KEY,
    last_compacted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
