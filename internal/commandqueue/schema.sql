CREATE SCHEMA IF NOT EXISTS commandqueue;

-- The commands table is the queue of commands awaiting handling.
--
-- Rows are deleted on successful handling.
CREATE TABLE IF NOT EXISTS commandqueue.commands (
    message_id      uuid        PRIMARY KEY,
    correlation_id  uuid        NOT NULL,
    message_type_id uuid        NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count   int         NOT NULL DEFAULT 0,
    envelope        bytea       NOT NULL
);

-- commands_by_type is an index that allows us to efficiently find
-- the next command of a given message type.
--
-- It is used by aggregate and integration controllers when claiming the next
-- command to dispatch.
CREATE INDEX IF NOT EXISTS commands_by_type
ON commandqueue.commands (
    message_type_id,
    next_attempt_at
);

-- commands_by_correlation_id is an index that allows us to efficiently
-- query pending commands by their correlation ID. That is, commands somewhere
-- in the causal chain of the same initial command.
--
-- It is used to implement dogma.WithEventObserver().
CREATE INDEX IF NOT EXISTS commands_by_correlation_id
ON commandqueue.commands (
    correlation_id
);

-- The idempotency_keys is an append-only table that holds the idempotency keys
-- specified using dogma.WithIdempotencyKey() when executing a command.
CREATE TABLE IF NOT EXISTS commandqueue.idempotency_keys (
    idempotency_key text PRIMARY KEY
);
