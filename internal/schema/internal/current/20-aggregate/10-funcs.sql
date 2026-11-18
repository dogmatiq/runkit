--------------------------------------------------------------------------------
-- The "save_aggregate_instance" function appends events to an event stream,
-- optionally updates the aggregate instance snapshot, and binds a newly
-- acquired stream if needed.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION runkit.save_aggregate_instance(
    handler_key     uuid,
    instance_id     text,
    stream_id       uuid,
    correlation_id  uuid,
    snapshot        bytea,
    events          runkit.event[]
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    next_offset bigint;
BEGIN
    -- If the instance has no bound stream, acquire one and bind it.
    IF save_aggregate_instance.stream_id IS NULL THEN
        save_aggregate_instance.stream_id := runkit.acquire_event_stream();

        UPDATE runkit.aggregate_instances AS i SET
            stream_id = save_aggregate_instance.stream_id
        WHERE i.handler_key = save_aggregate_instance.handler_key
        AND i.instance_id = save_aggregate_instance.instance_id;
    END IF;

    -- Append events to the stream.
    next_offset := runkit.append_events_to_specific_stream(
        save_aggregate_instance.stream_id,
        save_aggregate_instance.correlation_id,
        save_aggregate_instance.handler_key,
        save_aggregate_instance.instance_id,
        save_aggregate_instance.events
    );

    -- Update the snapshot if one was provided.
    IF save_aggregate_instance.snapshot IS NOT NULL THEN
        UPDATE runkit.aggregate_instances AS i SET
            snapshot_offset = next_offset - 1,
            snapshot = save_aggregate_instance.snapshot
        WHERE i.handler_key = save_aggregate_instance.handler_key
        AND i.instance_id = save_aggregate_instance.instance_id;
    END IF;
END;
$$;
