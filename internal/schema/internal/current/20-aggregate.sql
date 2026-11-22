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

--------------------------------------------------------------------------------
-- The "aggregate_stream_binding_counts" table holds, for each aggregate handler
-- and event stream, the number of the handler's instances that are bound to the
-- stream.
--
-- It exists only to report the AggregateInstanceEventStreamBindings metric
-- without counting the rows of "aggregate_instances". An instance is bound to
-- a stream once, and is never unbound or deleted after that, so a count only
-- grows.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.aggregate_stream_binding_counts (
    handler_key uuid   NOT NULL,
    stream_id   uuid   NOT NULL REFERENCES runkit.event_streams(id),
    instances   bigint NOT NULL CHECK (instances > 0),

    PRIMARY KEY (handler_key, stream_id)
);

--------------------------------------------------------------------------------
-- The "save_aggregate_instance" function appends events to an event stream,
-- optionally updates the aggregate instance snapshot, and binds a newly
-- acquired stream if needed, counting the binding in
-- "aggregate_stream_binding_counts".
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

        -- Count the binding. The transaction holds the lock on the stream's
        -- row, which it took when it acquired the stream, so no other
        -- transaction updates this count at the same time.
        INSERT INTO runkit.aggregate_stream_binding_counts AS c (
            handler_key,
            stream_id,
            instances
        ) VALUES (
            save_aggregate_instance.handler_key,
            save_aggregate_instance.stream_id,
            1
        )
        ON CONFLICT ON CONSTRAINT aggregate_stream_binding_counts_pkey
        DO UPDATE SET instances = c.instances + 1;
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
