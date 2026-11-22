--------------------------------------------------------------------------------
-- Create the "aggregate_stream_binding_counts" table, and fill it with the
-- number of instances of each aggregate handler that are already bound to each
-- event stream.
--------------------------------------------------------------------------------
CREATE TABLE runkit.aggregate_stream_binding_counts (
    handler_key uuid   NOT NULL,
    stream_id   uuid   NOT NULL REFERENCES runkit.event_streams(id),
    instances   bigint NOT NULL CHECK (instances > 0),

    PRIMARY KEY (handler_key, stream_id)
);

INSERT INTO runkit.aggregate_stream_binding_counts (
    handler_key,
    stream_id,
    instances
)
SELECT
    handler_key,
    stream_id,
    COUNT(*)
FROM runkit.aggregate_instances
WHERE stream_id IS NOT NULL
GROUP BY handler_key, stream_id;
