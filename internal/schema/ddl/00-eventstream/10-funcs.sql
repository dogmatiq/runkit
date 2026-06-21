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
-- The "acquire_for_write" function is used to obtain an event stream for append
-- operations.
--
-- It attempts to acquire an exclusive lock on an existing stream. If all
-- existing streams are locked, and hence doing so would create stream
-- contention, it either blocks until a stream becomes available (if a
-- zero-length stream already exists) or starts a new event stream.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.acquire_for_write()
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
    acquired_id uuid;
BEGIN
    -- Phase 1: try to grab a free stream without blocking.
    SELECT id INTO acquired_id
    FROM eventstream.streams
    ORDER BY next_offset, random()
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF acquired_id IS NOT NULL THEN
        RETURN acquired_id;
    END IF;

    -- Phase 2: all streams are locked. If a zero-length stream already exists,
    -- block until any stream becomes available rather than creating a redundant
    -- empty stream.
    IF EXISTS (SELECT 1 FROM eventstream.streams WHERE next_offset = 0) THEN
        SELECT id INTO acquired_id
        FROM eventstream.streams
        ORDER BY next_offset, random()
        FOR UPDATE
        LIMIT 1;

        RETURN acquired_id;
    END IF;

    -- Phase 3: no zero-length streams exist, create a new one.
    INSERT INTO eventstream.streams (id)
    VALUES (gen_random_uuid())
    RETURNING id INTO acquired_id;

    RETURN acquired_id;
END;
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
            next_offset = next_offset + array_length(append.events, 1)
        WHERE id = append.stream_id
        RETURNING next_offset - array_length(append.events, 1) AS base_offset
    ),
    event_list AS (
        SELECT
            e.message_id,
            e.message_type_id,
            e.envelope,
            ordinal - 1 AS ordinal
        FROM unnest(append.events) WITH ORDINALITY AS e(
            message_id,
            message_type_id,
            envelope,
            ordinal
        )
    ),
    inserted_events AS (
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
        RETURNING
            stream_offset,
            message_type_id
    ),
    deduped_types AS (
        SELECT DISTINCT ON (message_type_id)
            message_type_id,
            stream_offset
        FROM inserted_events
        ORDER BY message_type_id, stream_offset DESC
    ),
    upsert_types AS (
        INSERT INTO eventstream.event_types (
            stream_id,
            message_type_id,
            latest_offset
        )
        SELECT
            append.stream_id,
            dt.message_type_id,
            dt.stream_offset
        FROM deduped_types AS dt
        ON CONFLICT (stream_id, message_type_id)
        DO UPDATE SET latest_offset = EXCLUDED.latest_offset
    )
    SELECT MAX(stream_offset) + 1
    FROM inserted_events;
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
RETURNS TABLE(stream_id uuid, next_offset bigint)
LANGUAGE sql
AS $$
    WITH acquired AS (
        SELECT eventstream.acquire_for_write() AS id
    )
    SELECT
        a.id,
        eventstream.append(
            a.id,
            append_any.correlation_id,
            append_any.aggregate_handler_key,
            append_any.aggregate_instance_id,
            append_any.events
        )
    FROM acquired AS a;
$$;

--------------------------------------------------------------------------------
-- The "acquire_for_read" function acquires an event stream for a handler to
-- read pending events from.
--
-- It locks a row in "handler_checkpoints" and returns the stream_id and the
-- checkpoint_offset at the time of acquisition. If the handler is not yet
-- tracking a stream that has relevant events, a new row is inserted. Otherwise,
-- an existing row is locked with FOR UPDATE SKIP LOCKED, choosing the stream
-- with the largest gap between next_offset and checkpoint_offset.
--
-- Returns a single row, or no rows if no stream has pending events of the
-- relevant types.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION eventstream.acquire_for_read(
    handler_key      uuid,
    message_type_ids uuid[]
)
RETURNS TABLE(
    stream_id         uuid,
    checkpoint_offset bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    acquired_stream_id uuid;
BEGIN
    -- Attempt to insert a checkpoint row for a stream that the handler is not
    -- yet tracking, but which has events of the relevant types.
    INSERT INTO eventstream.handler_checkpoints (handler_key, stream_id)
    SELECT
        acquire_for_read.handler_key,
        t.stream_id
    FROM eventstream.event_types AS t
    WHERE t.message_type_id = ANY(acquire_for_read.message_type_ids)
    AND NOT EXISTS (
        SELECT 1
        FROM eventstream.handler_checkpoints AS h
        WHERE h.handler_key = acquire_for_read.handler_key
        AND h.stream_id = t.stream_id
    )
    ORDER BY random()
    LIMIT 1
    ON CONFLICT DO NOTHING
    RETURNING eventstream.handler_checkpoints.stream_id INTO acquired_stream_id;

    -- If the row was inserted successfully it is implicitly locked by this
    -- transaction and the checkpoint_offset is unknown. In all likelihood it
    -- will be zero, but we must fetch this offset from the handler itself.
    IF acquired_stream_id IS NOT NULL THEN
        RETURN QUERY
        SELECT
            acquired_stream_id,
            NULL::bigint;
        RETURN;
    END IF;

    -- Lock an existing checkpoint row for a stream that has pending events of
    -- the relevant types, choosing the stream with the largest gap.
    RETURN QUERY
    SELECT
        h.stream_id,
        h.checkpoint_offset
    FROM eventstream.handler_checkpoints AS h
    INNER JOIN eventstream.streams AS s
        ON s.id = h.stream_id
    INNER JOIN eventstream.event_types AS t
        ON t.stream_id = h.stream_id
        AND t.message_type_id = ANY(acquire_for_read.message_type_ids)
        AND t.latest_offset >= COALESCE(h.checkpoint_offset, 0)
    WHERE h.handler_key = acquire_for_read.handler_key
    AND h.resume_at <= clock_timestamp()
    AND s.next_offset > COALESCE(h.checkpoint_offset, 0)
    ORDER BY (s.next_offset - COALESCE(h.checkpoint_offset, 0)) DESC
    FOR UPDATE OF h SKIP LOCKED
    LIMIT 1;
END;
$$;

CREATE OR REPLACE FUNCTION eventstream.fail_and_postpone(
    handler_key  uuid,
    stream_id    uuid,
    backoff_base interval,
    backoff_cap  interval
)
RETURNS void
LANGUAGE sql
AS $$
    UPDATE eventstream.handler_checkpoints SET
        failures = failures + 1,
        resume_at = clock_timestamp() + common.exponential_backoff(
            failures,
            fail_and_postpone.backoff_base,
            fail_and_postpone.backoff_cap
        )
    WHERE handler_key = fail_and_postpone.handler_key
    AND stream_id = fail_and_postpone.stream_id;
$$;
