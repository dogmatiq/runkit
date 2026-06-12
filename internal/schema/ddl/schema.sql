CREATE SCHEMA IF NOT EXISTS dogma;

--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.aggregate_instances (
    handler_key           uuid   NOT NULL,
    instance_id           text   NOT NULL CHECK (instance_id != ''),
    stream_id             uuid   NOT NULL REFERENCES eventstream.streams(id),
    snapshot              bytea,
    offset_after_snapshot bigint NOT NULL DEFAULT 0 CHECK (offset_after_snapshot >= 0),

    PRIMARY KEY (handler_key, instance_id),

    CHECK (
        CASE
            WHEN snapshot IS NULL
            THEN offset_after_snapshot = 0
            ELSE offset_after_snapshot > 0
        END
    )
);

--------------------------------------------------------------------------------
-- The "integration_handler_locks" table provides row-level locking for
-- integration handlers that prefer minimized concurrency.
--
-- A row is inserted for each handler and then locked with a blocking SELECT FOR
-- UPDATE to serialize command handling.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.integration_handler_locks (
    handler_key uuid PRIMARY KEY
);
