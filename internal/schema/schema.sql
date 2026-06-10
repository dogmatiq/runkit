CREATE SCHEMA IF NOT EXISTS dogma;

--------------------------------------------------------------------------------
-- The "pending_commands" table is a queue of commands messages that have not
-- yet been handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.pending_commands (
    message_id            uuid        PRIMARY KEY,
    message_type_id       uuid        NOT NULL,
    envelope              bytea       NOT NULL,
    attempt_count         int         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    attempt_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Create an index for finding commands that are ready to be processed by their
-- type, ordered by priority (attempt count).
CREATE INDEX IF NOT EXISTS pending_commands_by_type
ON dogma.pending_commands (
    message_type_id,
    attempt_count
);

--------------------------------------------------------------------------------
-- The "event_streams" table is the set of all event streams.
--
-- The "next_offset" column is the next unused offset on the stream, which is
-- equivalent to the number of events already on the stream.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.event_streams (
    id          uuid   PRIMARY KEY,
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);

--------------------------------------------------------------------------------
-- The "events" table contains all events recorded by aggregate and integration
-- handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.events (
    message_id            uuid        PRIMARY KEY,
    envelope              bytea       NOT NULL,
    event_stream_id       uuid        NOT NULL REFERENCES dogma.event_streams(id),
    event_stream_offset   bigint      NOT NULL CHECK (event_stream_offset >= 0),
    aggregate_handler_key uuid,
    aggregate_instance_id text        CHECK (aggregate_instance_id != ''),
    recorded_at           timestamptz NOT NULL DEFAULT clock_timestamp(),

    UNIQUE (event_stream_id, event_stream_offset),

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
ON dogma.events (
    aggregate_handler_key,
    aggregate_instance_id,
    event_stream_offset
)
WHERE aggregate_handler_key IS NOT NULL;

--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.aggregate_instances (
    handler_key     uuid NOT NULL,
    instance_id     text NOT NULL CHECK (instance_id != ''),
    event_stream_id uuid NOT NULL,

    PRIMARY KEY (handler_key, instance_id)
);
