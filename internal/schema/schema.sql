CREATE SCHEMA IF NOT EXISTS dogma;

--------------------------------------------------------------------------------
-- The "pending_commands" table is a queue of commands messages that have not
-- yet been handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.pending_commands (
    message_id      uuid        PRIMARY KEY,
    message_type_id uuid        NOT NULL,
    envelope        bytea       NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count   int         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)
);

-- Create an index for finding commands that are ready to be processed by their
-- type.
CREATE INDEX IF NOT EXISTS idx_pending_commands_by_type
ON dogma.pending_commands (
    message_type_id,
    next_attempt_at
);

--------------------------------------------------------------------------------
-- The "event_streams" table is the set of all event streams.
--
-- The "next_offset" column is the next unused offset on the stream, which is
-- equivalent to the number of events already on the stream.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.event_streams (
    event_stream_id uuid   PRIMARY KEY,
    next_offset     bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);

--------------------------------------------------------------------------------
-- The "events" table contains all events recorded by aggregate and integration
-- handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.events (
    event_stream_id       uuid        NOT NULL,
    event_offset          bigint      NOT NULL CHECK (event_offset >= 0),
    envelope              bytea       NOT NULL,
    recorded_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    aggregate_handler_key uuid,
    aggregate_instance_id text        CHECK (aggregate_instance_id != ''),

    PRIMARY KEY (event_stream_id, event_offset),

    -- Ensure that either both aggregate_handler_key and aggregate_instance_id
    -- are NULL (an event from an integration handler), or neither are NULL (an
    -- event from an aggregate handler).
    CONSTRAINT has_complete_aggregate_identity
    CHECK (
        CASE
            WHEN aggregate_handler_key IS NULL
            THEN aggregate_instance_id IS NULL
            ELSE aggregate_instance_id IS NOT NULL
        END
    ),

    CONSTRAINT fk_event_stream
    FOREIGN KEY (event_stream_id)
    REFERENCES dogma.event_streams (event_stream_id)
    ON DELETE RESTRICT -- can't delete streams that contain events
    ON UPDATE RESTRICT -- can't change a stream's ID
);

-- Create an index for finding events by the aggregate instance that recorded
-- them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS idx_events_by_aggregate_instance
ON dogma.events (
    event_stream_id,
    event_offset,
    aggregate_handler_key,
    aggregate_instance_id
)
WHERE aggregate_handler_key IS NOT NULL;
