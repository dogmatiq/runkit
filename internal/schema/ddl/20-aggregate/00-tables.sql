CREATE SCHEMA IF NOT EXISTS aggregate;

--------------------------------------------------------------------------------
-- The "instances" table stores meta-data about each aggregate instance, and a
-- snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregate.instances (
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
