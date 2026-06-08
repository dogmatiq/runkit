CREATE SCHEMA IF NOT EXISTS dogma;

--------------------------------------------------------------------------------
-- The "pending_commands" table is a queue of commands messages that have not
-- yet been handled.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dogma.pending_commands (
    message_id      uuid  PRIMARY KEY,
    message_type_id uuid  NOT NULL,
    envelope        bytea NOT NULL
);
