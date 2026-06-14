--------------------------------------------------------------------------------
-- The "complete_without_events" function removes a command from the queue when
-- no events were recorded during handling. It deletes the aggregate instance if
-- it has no bound event stream (i.e., no prior events have ever been recorded).
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION aggregate.complete_without_events(
    message_id  uuid,
    handler_key uuid,
    instance_id text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- If the instance has never produced events, delete it.
    DELETE FROM aggregate.instances
    WHERE aggregate.instances.handler_key = complete_without_events.handler_key
    AND aggregate.instances.instance_id = complete_without_events.instance_id
    AND aggregate.instances.stream_id IS NULL;

    PERFORM commandqueue.remove(complete_without_events.message_id);
END;
$$;

--------------------------------------------------------------------------------
-- The "complete_with_events" function appends events to an event stream,
-- optionally updates the aggregate instance snapshot, removes the originating
-- command from the queue, and binds a newly acquired stream if needed.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION aggregate.complete_with_events(
    message_id      uuid,
    handler_key     uuid,
    instance_id     text,
    stream_id       uuid,
    correlation_id  uuid,
    snapshot        bytea,
    events          eventstream.event[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    next_offset bigint;
BEGIN
    -- If the instance has no bound stream, acquire one and bind it.
    IF complete_with_events.stream_id IS NULL THEN
        complete_with_events.stream_id := eventstream.acquire();

        UPDATE aggregate.instances SET
            stream_id = complete_with_events.stream_id
        WHERE aggregate.instances.handler_key = complete_with_events.handler_key
        AND aggregate.instances.instance_id = complete_with_events.instance_id;
    END IF;

    -- Append events to the stream.
    next_offset := eventstream.append(
        complete_with_events.stream_id,
        complete_with_events.correlation_id,
        complete_with_events.handler_key,
        complete_with_events.instance_id,
        complete_with_events.events
    );

    -- Update the snapshot if one was provided.
    IF complete_with_events.snapshot IS NOT NULL THEN
        UPDATE aggregate.instances SET
            snapshot_offset = next_offset - 1,
            snapshot = complete_with_events.snapshot
        WHERE aggregate.instances.handler_key = complete_with_events.handler_key
        AND aggregate.instances.instance_id = complete_with_events.instance_id;
    END IF;

    -- Remove the command from the queue.
    PERFORM commandqueue.remove(complete_with_events.message_id);
END;
$$;
