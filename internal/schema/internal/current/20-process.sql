--------------------------------------------------------------------------------
-- The "process_handlers" table keeps track of process handlers that exist or
-- have existed within the application.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.process_handlers (
    handler_key            uuid PRIMARY KEY,
    has_checkpoint_offsets boolean NOT NULL DEFAULT false
);

--------------------------------------------------------------------------------
-- The "process_handler_checkpoints" table tracks per-stream checkpoint progress
-- for each process handler.
--
-- A checkpoint row is locked while applying event effects to serialize
-- processing for a specific handler/stream pair. Stream IDs may refer to local
-- or remote streams.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.process_handler_checkpoints (
    handler_key       uuid   NOT NULL,
    stream_id         uuid   NOT NULL,
    checkpoint_offset bigint NOT NULL DEFAULT 0 CHECK (checkpoint_offset >= 0),

    PRIMARY KEY (handler_key, stream_id)
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

--------------------------------------------------------------------------------
-- The "process_instances_by_deadline_queue" index covers only the instances that
-- have not ended.
--
-- It exists only to report the ProcessInstanceQueueBindings metric, which
-- counts the in-progress instances of a handler for each queue. The count
-- reads the index alone, so its cost follows the number of in-progress
-- instances and not the number of ended ones, which are never deleted.
--
-- CREATE INDEX locks its table even when the index already exists, and the
-- engine's transactions lock "process_instances" and "queued_messages" in both
-- orders, so locking this table could deadlock with them. The index is created
-- only if it doesn't exist, so an upgrade that finds it takes no lock.
--------------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('runkit.process_instances_by_deadline_queue') IS NULL THEN
        CREATE INDEX process_instances_by_deadline_queue
        ON runkit.process_instances (handler_key, deadline_queue_id)
        WHERE NOT ended;
    END IF;
END $$;
