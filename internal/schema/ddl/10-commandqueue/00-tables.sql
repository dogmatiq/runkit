CREATE SCHEMA IF NOT EXISTS commandqueue;

--------------------------------------------------------------------------------
-- The "commands" table is a queue of commands messages that have not yet been
-- handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commandqueue.commands (
    message_id            uuid        PRIMARY KEY,
    correlation_id        uuid        NOT NULL,
    message_type_id       uuid        NOT NULL,
    envelope              bytea       NOT NULL,
    failures              int         NOT NULL DEFAULT 0 CHECK (failures >= 0),
    execute_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Create an index for finding commands that are ready to be processed by their
-- type.
CREATE INDEX IF NOT EXISTS pending_commands_by_type
ON commandqueue.commands (
    message_type_id,
    execute_at
);

-- Create an index for finding commands that are part of a particular causal
-- chain; that is they have a specific correlation ID.
CREATE INDEX IF NOT EXISTS pending_commands_by_correlation_id
ON commandqueue.commands (
    correlation_id,
    execute_at
);

--------------------------------------------------------------------------------
-- The "idempotency_keys" table is a write-only list of idempotency keys
-- specified when executing a command with the dogma.WithIdempotencyKey()
-- option.
--
-- It is used to deduplicate commands with the same key, even after they have
-- been handled and deleted from the "pending_commands" table.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commandqueue.idempotency_keys (
    idempotency_key text PRIMARY KEY CHECK (idempotency_key != ''),
    message_id      uuid NOT NULL UNIQUE
);
