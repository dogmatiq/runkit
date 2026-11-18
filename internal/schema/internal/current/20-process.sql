--------------------------------------------------------------------------------
-- The "process_handlers" table keeps track of process handlers that exist or
-- have existed within the application.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.process_handlers (
    handler_key            uuid PRIMARY KEY,
    has_checkpoint_offsets boolean NOT NULL DEFAULT false
);

--------------------------------------------------------------------------------
-- The "process_instances" table stores the state of each process instance.
--
-- "deadline_queue_id" is the queue that the instance's deadlines are added to,
-- or NULL if the instance has not been bound to a queue.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.process_instances (
    handler_key       uuid    NOT NULL,
    instance_id       text    NOT NULL CHECK (instance_id != ''),
    ended             boolean NOT NULL DEFAULT false,
    state             bytea,
    deadline_queue_id uuid,

    PRIMARY KEY (handler_key, instance_id),
    CHECK (NOT ended OR state IS NULL)
);
