--------------------------------------------------------------------------------
-- The "event_streams" table is the set of all event streams.
--
-- The "next_offset" column is the next unused offset on the stream, which is
-- equivalent to the number of events already on the stream.
--
-- The "created_at" column provides the age of each stream.
--
-- When binding a new aggregate instance to a stream, the age and number of
-- events on the stream are used to compute the stream's "events-per-second"
-- metric, allowing selection of the stream with the lowest activity.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_streams (
    event_stream_id uuid        PRIMARY KEY,
    next_offset     bigint      NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

--------------------------------------------------------------------------------
-- The "aggregate_instances" table stores meta-data about each aggregate
-- instance, and a snapshot of its state, if one is available.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aggregate_instances (
    handler_key                   uuid   NOT NULL,
    instance_id                   text   NOT NULL CHECK (instance_id != ''),
    event_stream_id               uuid,
    event_offset_after_last_event bigint NOT NULL DEFAULT 0 CHECK (event_offset_after_last_event >= 0),
    event_offset_after_snapshot   bigint NOT NULL DEFAULT 0 CHECK (event_offset_after_snapshot >= 0),
    snapshot                      bytea,

    PRIMARY KEY (handler_key, instance_id),

    -- Ensure that an instance must be bound to an event stream before it can
    -- make reference to any event offsets.
    CONSTRAINT event_offsets_have_stream_id
    CHECK (
        CASE
            WHEN event_stream_id IS NULL
            THEN event_offset_after_last_event = 0
                AND event_offset_after_snapshot = 0
        END
    ),

    -- Ensure that a snapshot cannot be present unless the instance is bound to
    -- an event stream, and the snapshot offset is also present.
    CONSTRAINT has_complete_snapshot
    CHECK (
        CASE
            WHEN event_offset_after_snapshot = 0
            THEN snapshot IS NULL
            ELSE snapshot IS NOT NULL
        END
    ),

    -- Ensure that the snapshot offset is never greater than the last event
    -- offset. Otherwise, it is claiming to represent a snapshot of state that
    -- has not yet been reached.
    CONSTRAINT snapshot_offset_in_range
    CHECK (
        event_offset_after_snapshot <= event_offset_after_last_event
    ),

    -- This unique constraint is required so that the events table can declare a
    -- composite foreign key that includes event_stream_id, ensuring each
    -- aggregate instance's events are always on the correct stream. It is
    -- otherwise logically redundant because the PK already ensures there can be
    -- only one row for a given instance
    CONSTRAINT unique_instance_stream
    UNIQUE (
        handler_key,
        instance_id,
        event_stream_id
    ),

    CONSTRAINT fk_event_stream
    FOREIGN KEY (event_stream_id)
    REFERENCES event_streams (event_stream_id)
    ON DELETE RESTRICT -- can't delete streams with aggregate instances bound to them
    ON UPDATE RESTRICT -- can't change a stream's ID
);

--------------------------------------------------------------------------------
-- The "events" table contains all events recorded by aggregate and integration
-- handlers.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    event_stream_id       uuid   NOT NULL,
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
    ),

    CONSTRAINT fk_event_stream
    FOREIGN KEY (event_stream_id)
    REFERENCES event_streams (event_stream_id)
    ON DELETE RESTRICT  -- can't delete streams with events on them
    ON UPDATE RESTRICT, -- can't change a stream's ID

    CONSTRAINT fk_aggregate_instance
    FOREIGN KEY (
        aggregate_handler_key,
        aggregate_instance_id,

        -- Include the "event_stream_id" column in the constraint to ensure that
        -- any event produced by an aggregate instance can only be recorded
        -- against the stream that the instance is bound to.
        event_stream_id
    )
    REFERENCES aggregate_instances (
        handler_key,
        instance_id,
        event_stream_id
    )
    ON DELETE RESTRICT -- can't delete aggregate instances with events
    ON UPDATE RESTRICT -- can't change an instance's ID or stream ID
);

-- Create an index for finding events by their message type ID within a specific
-- stream.
--
-- It is used to implement event consumers, which always query for a specific
-- subset of event types.
CREATE INDEX IF NOT EXISTS idx_events_by_type
ON events (
    event_stream_id,
    event_offset,
    message_type_id
);

-- Create an index for finding events by the aggregate instance that recorded
-- them.
--
-- It is used to reload aggregate root state. The WHERE clause excludes events
-- recorded by integration handlers from the index.
CREATE INDEX IF NOT EXISTS idx_events_by_aggregate_instance
ON events (
    event_stream_id,
    event_offset,
    aggregate_handler_key,
    aggregate_instance_id
)
WHERE aggregate_handler_key IS NOT NULL;

-- Create an index for finding events by their correlation ID. That is, events
-- somewhere in the causal chain of the same initial command.
--
-- It is used to discover events recorded in the same causal chain as a command
-- executed with the dogma.WithEventObserver() option.
CREATE INDEX IF NOT EXISTS idx_events_by_correlation_id
ON events (
    event_stream_id,
    correlation_id,
    event_offset
);

--------------------------------------------------------------------------------
-- The "pending_commands" table is a queue of commands messages that have not
-- yet been handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pending_commands (
    message_id            uuid        PRIMARY KEY,
    correlation_id        uuid        NOT NULL,
    handler_key           uuid        NOT NULL,
    aggregate_instance_id text        CHECK (aggregate_instance_id != ''),
    attempt_count         bigint      NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    envelope              bytea       NOT NULL,

    CONSTRAINT fk_aggregate_instance
    FOREIGN KEY (
        handler_key,
        aggregate_instance_id
    )
    REFERENCES aggregate_instances (
        handler_key,
        instance_id
    )
    ON DELETE RESTRICT -- can't delete aggregate instances with commands routed to it
    ON UPDATE RESTRICT -- can't change an instance's ID
);

-- Create an index for finding commands by their intended handler (and aggregate
-- instance, if applicable).
--
-- It is used by aggregate.Controller and integration.Controller to find
-- commands that they should handle.
CREATE INDEX IF NOT EXISTS idx_pending_commands_by_route
ON pending_commands (
    handler_key,
    aggregate_instance_id
);

-- Create an index for finding commands by their correlation ID. That is,
-- commands somewhere in the causal chain of the same initial command.
--
-- It is used to discover if there are any pending commands, and hence any
-- possibility of future events, in the same causal chain as a command executed
-- with the dogma.WithEventObserver() option.
CREATE INDEX IF NOT EXISTS idx_pending_commands_by_correlation_id
ON pending_commands (
    correlation_id
);

--------------------------------------------------------------------------------
-- The "command_idempotency_keys" table is a write-only list of idempotency keys
-- specified when executing a command with the dogma.WithIdempotencyKey()
-- option.
--
-- It is used to deduplicate commands with the same key, even after they have
-- been handled and deleted from the "pending_commands" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS command_idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != '')
);


--------------------------------------------------------------------------------
-- The "integration_locks" table provides a mechanism for enforcing that at most
-- one worker processes commands for each integration handler that uses
-- dogma.MinimizeConcurrency as its concurrency preference.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS integration_locks (
    handler_key uuid PRIMARY KEY
);
