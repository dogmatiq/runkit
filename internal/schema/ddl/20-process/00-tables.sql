CREATE SCHEMA IF NOT EXISTS process;

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
