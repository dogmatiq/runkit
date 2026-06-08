CREATE SCHEMA IF NOT EXISTS dogma;

--------------------------------------------------------------------------------
-- The "pending_commands" table is a queue of commands messages that have not
-- yet been handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.pending_commands (
    message_id      uuid        PRIMARY KEY,
    message_type_id uuid        NOT NULL,
    envelope        bytea       NOT NULL,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count   int         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)
);
