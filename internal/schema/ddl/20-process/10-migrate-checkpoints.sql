-- Preserve existing progress while removing the dependency on local streams.
-- Engines using the previous schema must be stopped before this migration.
DO $$
BEGIN
    IF to_regclass('eventstream.handler_checkpoints') IS NOT NULL THEN
        INSERT INTO process.checkpoints (handler_key, stream_id, checkpoint_offset)
        SELECT handler_key, stream_id, checkpoint_offset
        FROM eventstream.handler_checkpoints
        ON CONFLICT (handler_key, stream_id) DO UPDATE
        SET checkpoint_offset = GREATEST(
            process.checkpoints.checkpoint_offset,
            EXCLUDED.checkpoint_offset
        );

        DROP TABLE eventstream.handler_checkpoints;
    END IF;
END;
$$;

-- Handler registration was only needed to seed checkpoints at the stream end.
DROP TABLE IF EXISTS process.handlers;
