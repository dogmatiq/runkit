CREATE SCHEMA IF NOT EXISTS process;

--------------------------------------------------------------------------------
-- The "handlers" table keeps track of handlers that exist or have existed
-- within the application.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS process.handlers (
    handler_key            uuid PRIMARY KEY,
    has_checkpoint_offsets boolean NOT NULL DEFAULT false
);

--------------------------------------------------------------------------------
-- The "instances" table stores the state of each process instance.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS process.instances (
    handler_key uuid    NOT NULL,
    instance_id text    NOT NULL CHECK (instance_id != ''),
    ended       boolean NOT NULL DEFAULT false,
    state       bytea,

    PRIMARY KEY (handler_key, instance_id),
    CHECK (NOT ended OR state IS NULL)
);

--------------------------------------------------------------------------------
-- The "deadlines" table is a queue of pending deadline messages that have not
-- yet been delivered to their target process instance.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS process.deadlines (
    message_id      uuid        PRIMARY KEY,
    message_type_id uuid        NOT NULL,
    handler_key     uuid        NOT NULL,
    instance_id     text        NOT NULL CHECK (instance_id != ''),
    envelope        bytea       NOT NULL,
    failures        int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
    deliver_at      timestamptz NOT NULL
);

-- Create an index for finding deadlines that are ready to be processed by a
-- specific handler.
CREATE INDEX IF NOT EXISTS pending_deadlines_by_handler
ON process.deadlines (
    handler_key,
    deliver_at
);
