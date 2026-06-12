CREATE SCHEMA IF NOT EXISTS eventstream;

--------------------------------------------------------------------------------
-- The "event" type encapsulates event data for use with the "append" and
-- "append_any" functions.
--------------------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE eventstream.event AS (
        message_id      uuid,
        message_type_id uuid,
        envelope        bytea
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


--------------------------------------------------------------------------------
-- The "acquire" function is used to obtain an event stream for append
-- operations.
--
-- It attempts to acquire an exclusive lock on an existing stream. If all
-- existing streams are locked, and hence doing so would create stream
-- contention, it starts a new event stream.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.acquire()
RETURNS uuid
LANGUAGE sql
AS $$
    WITH existing AS (
        SELECT id
        FROM eventstream.streams
        ORDER BY next_offset, random()
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ),
    created AS (
        INSERT INTO eventstream.streams (id)
        SELECT gen_random_uuid()
        WHERE NOT EXISTS (SELECT 1 FROM existing)
        RETURNING id
    )
    SELECT id FROM existing
    UNION ALL
    SELECT id FROM created
    LIMIT 1;
$$;


--------------------------------------------------------------------------------
-- The "append" function appends events to a specific event stream.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.append(
    stream_id             uuid,
    correlation_id        uuid,
    aggregate_handler_key uuid,
    aggregate_instance_id text,
    events                eventstream.event[]
)
RETURNS bigint
LANGUAGE sql
AS $$
    WITH updated_stream AS (
        UPDATE eventstream.streams SET
            next_offset = next_offset + array_length(events, 1)
        WHERE id = stream_id
        RETURNING OLD.next_offset AS base_offset
    ),
    event_list AS (
        SELECT
            e.message_id,
            e.message_type_id,
            e.envelope,
            ordinal - 1 AS ordinal
        FROM unnest(events) WITH ORDINALITY AS e(
            message_id,
            message_type_id,
            envelope,
            ordinal
        )
    )
    INSERT INTO eventstream.events (
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
        append.stream_id,
        s.base_offset + e.ordinal,
        e.message_id,
        append.correlation_id,
        e.message_type_id,
        e.envelope,
        append.aggregate_handler_key,
        append.aggregate_instance_id
    FROM event_list AS e, updated_stream AS s
    RETURNING stream_offset + 1;
$$;

--------------------------------------------------------------------------------
-- The "append_any" function acquires an event stream and appends events to it.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.append_any(
    correlation_id        uuid,
    aggregate_handler_key uuid,
    aggregate_instance_id text,
    events                eventstream.event[]
)
RETURNS bigint
LANGUAGE sql
AS $$
    SELECT eventstream.append(
        eventstream.acquire(),
        correlation_id,
        aggregate_handler_key,
        aggregate_instance_id,
        events
    );
$$;
