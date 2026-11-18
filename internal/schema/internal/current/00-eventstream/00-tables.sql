--------------------------------------------------------------------------------
-- The "event_streams" table is the set of all event streams.
--
-- The "next_offset" column is the next unused offset on the stream, which is
-- equivalent to the number of events already on the stream.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.event_streams (
    id          uuid   PRIMARY KEY,
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);

--------------------------------------------------------------------------------
-- The "events" table contains all events recorded by aggregate and integration
-- handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.events (
    message_id            uuid        PRIMARY KEY,
    correlation_id        uuid        NOT NULL,
    message_type_id       uuid        NOT NULL,
    envelope              bytea       NOT NULL,
    stream_id             uuid        NOT NULL REFERENCES runkit.event_streams(id),
    stream_offset         bigint      NOT NULL CHECK (stream_offset >= 0),
    aggregate_handler_key uuid,
    aggregate_instance_id text        CHECK (aggregate_instance_id != ''),
    recorded_at           timestamptz NOT NULL DEFAULT clock_timestamp(),

    UNIQUE (stream_id, stream_offset),

    -- Ensure that either both aggregate_handler_key and aggregate_instance_id
    -- are NULL (an event from an integration handler), or neither are NULL (an
    -- event from an aggregate handler).
    CHECK (
        CASE
            WHEN aggregate_handler_key IS NULL
            THEN aggregate_instance_id IS NULL
            ELSE aggregate_instance_id IS NOT NULL
        END
    )
);

-- Create an index for finding events by the aggregate instance that recorded
-- them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS events_by_aggregate_instance
ON runkit.events (
    aggregate_handler_key,
    aggregate_instance_id,
    stream_offset
)
WHERE aggregate_handler_key IS NOT NULL;

-- Create an index for finding events of a specific type by their offset within
-- a stream.
CREATE INDEX IF NOT EXISTS events_by_stream_and_offset
ON runkit.events (
    stream_id,
    message_type_id,
    stream_offset
);

--------------------------------------------------------------------------------
-- The "handler_checkpoints" table tracks per-stream checkpoint progress for
-- each process handler.
--
-- A checkpoint row is locked while applying event effects to serialize
-- processing for a specific handler/stream pair. Stream IDs may refer to local
-- or remote streams.
--
-- The failures and resume_at columns are not used.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.handler_checkpoints (
    handler_key       uuid        NOT NULL,
    stream_id         uuid        NOT NULL,
    checkpoint_offset bigint      NOT NULL DEFAULT 0 CHECK (checkpoint_offset >= 0),
    failures          int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
    resume_at         timestamptz NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (handler_key, stream_id)
);
