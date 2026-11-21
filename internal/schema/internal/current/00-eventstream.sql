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

-- Create an index for finding the events that have a given correlation ID.
--
-- It is used to find the events that a command caused, for the event observers
-- of ExecuteCommand().
CREATE INDEX IF NOT EXISTS events_by_correlation_id
ON runkit.events (correlation_id);

--------------------------------------------------------------------------------
-- The "event" type encapsulates event data for use with the
-- "append_events_to_specific_stream" and "append_events_to_any_stream"
-- functions.
--------------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE runkit.event AS (
        message_id      uuid,
        message_type_id uuid,
        envelope        bytea
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


--------------------------------------------------------------------------------
-- The "acquire_event_stream" function is used to obtain an event stream for
-- append operations.
--
-- It attempts to acquire an exclusive lock on an existing stream. If all
-- existing streams are locked, and hence doing so would create stream
-- contention, it either blocks until a stream becomes available (if a
-- zero-length stream already exists) or starts a new event stream.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.acquire_event_stream()
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    acquired_id uuid;
BEGIN
    -- Phase 1: try to grab a free stream without blocking.
    SELECT id INTO acquired_id
    FROM runkit.event_streams
    ORDER BY next_offset, random()
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF acquired_id IS NOT NULL THEN
        RETURN acquired_id;
    END IF;

    -- Phase 2: all streams are locked. If a zero-length stream already exists,
    -- block until any stream becomes available rather than creating a redundant
    -- empty stream.
    IF EXISTS (SELECT 1 FROM runkit.event_streams WHERE next_offset = 0) THEN
        SELECT id INTO acquired_id
        FROM runkit.event_streams
        ORDER BY next_offset, random()
        FOR UPDATE
        LIMIT 1;

        RETURN acquired_id;
    END IF;

    -- Phase 3: no zero-length streams exist, create a new one.
    INSERT INTO runkit.event_streams (id)
    VALUES (gen_random_uuid())
    RETURNING id INTO acquired_id;

    RETURN acquired_id;
END;
$$;

--------------------------------------------------------------------------------
-- The "append_events_to_specific_stream" function appends events to a specific
-- event stream.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.append_events_to_specific_stream(
    stream_id             uuid,
    correlation_id        uuid,
    aggregate_handler_key uuid,
    aggregate_instance_id text,
    events                runkit.event[]
)
RETURNS bigint
LANGUAGE sql
AS $$
    WITH updated_stream AS (
        UPDATE runkit.event_streams SET
            next_offset = next_offset
                + array_length(append_events_to_specific_stream.events, 1)
        WHERE id = append_events_to_specific_stream.stream_id
        RETURNING next_offset
            - array_length(append_events_to_specific_stream.events, 1)
            AS base_offset
    ),
    event_list AS (
        SELECT
            e.message_id,
            e.message_type_id,
            e.envelope,
            ordinal - 1 AS ordinal
        FROM unnest(append_events_to_specific_stream.events) WITH ORDINALITY AS e(
            message_id,
            message_type_id,
            envelope,
            ordinal
        )
    ),
    inserted_events AS (
        INSERT INTO runkit.events (
            stream_id,
            stream_offset,
            message_id,
            correlation_id,
            message_type_id,
            envelope,
            aggregate_handler_key,
            aggregate_instance_id
        )
        SELECT
            append_events_to_specific_stream.stream_id,
            s.base_offset + e.ordinal,
            e.message_id,
            append_events_to_specific_stream.correlation_id,
            e.message_type_id,
            e.envelope,
            append_events_to_specific_stream.aggregate_handler_key,
            append_events_to_specific_stream.aggregate_instance_id
        FROM event_list AS e
        CROSS JOIN updated_stream AS s
        RETURNING stream_offset
    )
    SELECT MAX(stream_offset) + 1
    FROM inserted_events;
$$;

--------------------------------------------------------------------------------
-- The "append_events_to_any_stream" function acquires an event stream and
-- appends events to it.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.append_events_to_any_stream(
    correlation_id        uuid,
    aggregate_handler_key uuid,
    aggregate_instance_id text,
    events                runkit.event[]
)
RETURNS TABLE(stream_id uuid, next_offset bigint)
LANGUAGE sql
AS $$
    WITH acquired AS (
        SELECT runkit.acquire_event_stream() AS id
    )
    SELECT
        a.id,
        runkit.append_events_to_specific_stream(
            a.id,
            append_events_to_any_stream.correlation_id,
            append_events_to_any_stream.aggregate_handler_key,
            append_events_to_any_stream.aggregate_instance_id,
            append_events_to_any_stream.events
        )
    FROM acquired AS a;
$$;

--------------------------------------------------------------------------------
-- The "signal_event_stream_created" trigger sends the "event_stream_created"
-- signal when event streams are created. source.Local in
-- internal/eventstream/source waits for it to discover new streams.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that creates many streams produces a single signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_event_stream_created()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'event_stream_created') FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_event_stream_created
AFTER INSERT ON runkit.event_streams
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_event_stream_created();

--------------------------------------------------------------------------------
-- The "signal_events_appended" trigger sends the
-- "events_appended.stream.<stream-id>" signal when events are appended to an event
-- stream. source.Local in internal/eventstream/source waits for each stream's
-- signal to deliver its new events.
--
-- PostgreSQL delivers identical notifications sent within one transaction only
-- once, so a transaction that appends many events to a stream produces a single
-- signal.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.signal_events_appended()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('signal', 'events_appended.stream.' || stream_id) FROM inserted;
    PERFORM pg_notify('signal', 'events_appended.correlation.' || correlation_id) FROM inserted;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER signal_events_appended
AFTER INSERT ON runkit.events
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION runkit.signal_events_appended();
