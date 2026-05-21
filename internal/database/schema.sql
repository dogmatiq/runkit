--------------------------------------------------------------------------------
-- The "pending_commands" table is the queue of commands awaiting handling.
-- Rows are deleted once the command is successfully handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_commands (
    message_id      uuid        PRIMARY KEY,
    correlation_id  uuid        NOT NULL,
    message_type_id uuid        NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count   bigint      NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    envelope        bytea       NOT NULL
);

-- Create an index that allows us to efficiently find the next due command of a
-- given message type.
CREATE INDEX IF NOT EXISTS idx_pending_commands_by_type
ON pending_commands (
    message_type_id,
    next_attempt_at
);

-- Create an index that allows us to efficiently query pending commands by their
-- correlation ID. That is, commands somewhere in the causal chain of the same
-- initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS idx_pending_commands_by_correlation_id
ON pending_commands (
    correlation_id
);

--------------------------------------------------------------------------------
-- The "command_idempotency_keys" table is an append-only list of idempotency
-- keys specified by dogma.WithIdempotencyKey(), it is used to deduplicate
-- commands even after they have been handled and deleted from the
-- "pending_commands" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != '')
);

--------------------------------------------------------------------------------
-- The "event_streams" is the set of all event streams, and their next unused
-- offset.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_streams (
    event_stream_id uuid        PRIMARY KEY,
    next_offset     bigint      NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

--------------------------------------------------------------------------------
-- The "events" table contains all events recorded by aggregate and integration
-- handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    event_stream_id       uuid   NOT NULL REFERENCES event_streams (event_stream_id),
    event_offset          bigint NOT NULL CHECK (event_offset >= 0),
    correlation_id        uuid   NOT NULL,
    message_type_id       uuid   NOT NULL,
    envelope              bytea  NOT NULL,
    aggregate_handler_key uuid,
    aggregate_instance_id text   CHECK (aggregate_instance_id != ''),

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
    )
);

-- Create an index that allows us to efficiently query events by their message
-- type ID within a specific stream.
--
-- It is used to filter events by type when routing to process and projection
-- handlers.
CREATE INDEX IF NOT EXISTS idx_events_by_type
ON events (
    event_stream_id,
    message_type_id,
    event_offset
);

-- Create an index that allows us to efficiently query events by the aggregate
-- instance that recorded them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS idx_events_by_aggregate_instance
ON events (
    event_stream_id,
    aggregate_handler_key,
    aggregate_instance_id,
    event_offset
)
WHERE aggregate_handler_key IS NOT NULL;

-- Create an index that allows us to efficiently query events by their
-- correlation ID within a stream. That is, events somewhere in the causal chain
-- of the same initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS idx_events_by_correlation_id
ON events (
    event_stream_id,
    correlation_id,
    event_offset
);

--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregate_instances (
    handler_key                   uuid   NOT NULL,
    instance_id                   text   NOT NULL CHECK (instance_id != ''),
    event_stream_id               uuid   NOT NULL REFERENCES event_streams (event_stream_id),
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
-- The "aggregate_command_routes" table maps commands to the aggregate instance
-- that will handle them.
--
-- A row is inserted when the aggregate controller routes a command to a
-- specific instance, and deleted when the command is acked.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregate_command_routes (
    message_id  uuid PRIMARY KEY REFERENCES pending_commands (message_id) ON DELETE CASCADE,
    handler_key uuid NOT NULL,
    instance_id text NOT NULL CHECK (instance_id != ''),

    FOREIGN KEY (handler_key, instance_id)
    REFERENCES aggregate_instances (handler_key, instance_id)
);

-- Create an index that allows us to efficiently find routed commands for a
-- given aggregate instance.
CREATE INDEX IF NOT EXISTS idx_aggregate_command_routes_by_instance
ON aggregate_command_routes (
    handler_key,
    instance_id
);

--------------------------------------------------------------------------------
-- The "integration_locks" table provides a mechanism for enforcing that at most
-- one worker processes commands for each integration handler that uses
-- dogma.MinimizeConcurrency as its concurrency preference.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration_locks (
    handler_key uuid PRIMARY KEY
);
