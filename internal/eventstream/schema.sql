CREATE SCHEMA IF NOT EXISTS eventstream;

-- The next_offset table holds the next offset to be assigned to an event.
CREATE TABLE IF NOT EXISTS eventstream.next_offset (
    next_offset bigint NOT NULL DEFAULT 0
);

-- next_event_singleton is a unique index that ensures that next_offset contains
-- at most one row.
--
-- This allows us to use UPSERT to atomically read and update the next offset
-- when appending new events to the stream.
CREATE UNIQUE INDEX IF NOT EXISTS next_event_singleton
ON eventstream.next_offset (
    (TRUE)
);

-- The events table contains all events recorded by aggregate and integration
-- handlers.
CREATE TABLE IF NOT EXISTS eventstream.events (
    "offset"              bigint PRIMARY KEY,
    correlation_id        uuid   NOT NULL,
    message_type_id       uuid   NOT NULL,
    envelope              bytea  NOT NULL,
    aggregate_handler_key uuid,
    aggregate_instance_id text,

    -- Ensure that either both aggregate_handler_key and aggregate_instance_id
    -- are NULL (an event from an integration handler), or neither are NULL (an
    -- event from an aggregate handler).
    CHECK (
        (aggregate_handler_key IS NULL     AND aggregate_instance_id IS NULL)
     OR (aggregate_handler_key IS NOT NULL AND aggregate_instance_id IS NOT NULL)
    )
);

-- events_by_correlation_id is an index that allows us to efficiently query
-- events by their correlation ID. That is, events somewhere in the causal chain
-- of the same initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS events_by_correlation_id
ON eventstream.events (
    correlation_id
);

-- events_by_type is an index that allows us to efficiently query events by
-- their message type ID.
--
-- It is used to filter events by type when routing to process and projection
-- handlers.
CREATE INDEX IF NOT EXISTS events_by_type
ON eventstream.events (
    message_type_id
);

-- events_by_aggregate_instance is an index that allows us to efficiently query
-- events by the aggregate instance that recorded them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS events_by_aggregate_instance
ON eventstream.events (
    aggregate_handler_key,
    aggregate_instance_id,
    "offset"
)
WHERE aggregate_handler_key IS NOT NULL;
