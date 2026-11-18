--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.aggregate_instances (
    handler_key     uuid   NOT NULL,
    instance_id     text   NOT NULL CHECK (instance_id != ''),
    stream_id       uuid   REFERENCES runkit.event_streams(id),
    snapshot        bytea,
    snapshot_offset bigint CHECK (snapshot_offset >= 0),

    PRIMARY KEY (handler_key, instance_id),

    CHECK (
        CASE
            WHEN stream_id IS NULL THEN snapshot IS NULL AND snapshot_offset IS NULL
            WHEN snapshot IS NULL  THEN snapshot_offset IS NULL
            ELSE                        snapshot_offset IS NOT NULL
        END
    )
);
