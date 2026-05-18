--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregate_instances (
    handler_key                   uuid   NOT NULL,
    instance_id                   text   NOT NULL CHECK (instance_id != ''),
    event_offset_after_last_event bigint NOT NULL DEFAULT 0 CHECK (event_offset_after_last_event >= 0),
    event_offset_after_snapshot   bigint NOT NULL DEFAULT 0 CHECK (event_offset_after_snapshot >= 0),
    snapshot                      bytea,

    PRIMARY KEY (handler_key, instance_id),

    CONSTRAINT has_complete_snapshot
    CHECK (
        CASE
            WHEN event_offset_after_snapshot = 0
            THEN snapshot IS NULL
            ELSE snapshot IS NOT NULL
        END
    ),

    CONSTRAINT snapshot_offset_in_range
    CHECK (
        event_offset_after_snapshot <= event_offset_after_last_event
    )
);

--------------------------------------------------------------------------------
-- The "command_queue" table is the queue of commands awaiting handling.
-- Rows are deleted once the command is successfully handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_queue (
    message_id                      uuid        PRIMARY KEY,
    correlation_id                  uuid        NOT NULL,
    message_type_id                 uuid        NOT NULL,
    routed_to_handler_key           uuid,
    routed_to_aggregate_instance_id text        CHECK (routed_to_aggregate_instance_id != ''),
    next_attempt_at                 timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count                   bigint      NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    envelope                        bytea       NOT NULL,

    -- An aggregate instance ID can't be set without a handler.
    --
    -- However, it may remain NULL when routed_to_handler_key is set for
    -- handlers to represent commands that have been routed to integration
    -- handlers.
    CONSTRAINT has_complete_route
    CHECK (
        CASE
            WHEN routed_to_handler_key IS NULL
            THEN routed_to_aggregate_instance_id IS NULL
            ELSE TRUE
        END
    ),

    -- Commands routed to aggregate handlers must reference an existing
    -- aggregate_instance row, so that it can be exclusively locked.
    CONSTRAINT fk_aggregate_instance
    FOREIGN KEY (routed_to_handler_key, routed_to_aggregate_instance_id)
    REFERENCES aggregate_instances (handler_key, instance_id)
);

-- Create an index that allows us to efficiently find the next unrouted command
-- of a given message type.
CREATE INDEX IF NOT EXISTS idx_command_queue_by_type
ON command_queue (
    message_type_id,
    next_attempt_at
)
WHERE routed_to_handler_key IS NULL;

-- Create an index that allows us to efficiently find the next command to
-- process for a given handler / instance.
CREATE INDEX IF NOT EXISTS idx_command_queue_by_route
ON command_queue (
    routed_to_handler_key,
    routed_to_aggregate_instance_id,
    next_attempt_at
)
WHERE routed_to_handler_key IS NOT NULL;

-- Create an index that allows us to efficiently query pending commands by their
-- correlation ID. That is, commands somewhere in the causal chain of the same
-- initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS idx_command_queue_by_correlation_id
ON command_queue (
    correlation_id
);

--------------------------------------------------------------------------------
-- The "command_idempotency_keys" table is an append-only list of idempotency
-- keys specified by dogma.WithIdempotencyKey(), it is used to deduplicate
-- commands even after they have been handled and deleted from the
-- "command_queue" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != '')
);

--------------------------------------------------------------------------------
-- The "event_stream_offset" table holds the next "unused" offset on the event
-- stream.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_stream_offset (
    next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0)
);

-- Create a unique index that ensures that the "event_stream_offset" table
-- contains at most one row.
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_stream_offset_singleton
ON event_stream_offset (
    (TRUE)
);

--------------------------------------------------------------------------------
-- The "event_stream" table contains all events recorded by aggregate and
-- integration handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_stream (
    event_offset          bigint PRIMARY KEY CHECK (event_offset >= 0),
    correlation_id        uuid   NOT NULL,
    message_type_id       uuid   NOT NULL,
    envelope              bytea  NOT NULL,
    aggregate_handler_key uuid,
    aggregate_instance_id text   CHECK (aggregate_instance_id != ''),

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
    )
);

-- Create an index that allows us to efficiently query events by their message
-- type ID.
--
-- It is used to filter events by type when routing to process and projection
-- handlers.
CREATE INDEX IF NOT EXISTS idx_event_stream_by_type
ON event_stream (
    message_type_id
);

-- Create an index that allows us to efficiently query events by the aggregate
-- instance that recorded them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS idx_event_stream_by_aggregate_instance
ON event_stream (
    aggregate_handler_key,
    aggregate_instance_id,
    event_offset
)
WHERE aggregate_handler_key IS NOT NULL;

-- Create an index that allows us to efficiently query events by their
-- correlation ID. That is, events somewhere in the causal chain of the same
-- initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS idx_event_stream_by_correlation_id
ON event_stream (
    correlation_id
);
