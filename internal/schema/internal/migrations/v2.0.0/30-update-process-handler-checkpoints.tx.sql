--------------------------------------------------------------------------------
-- Allow checkpoints for the streams of other applications, which aren't in the
-- local "event_streams" table.
--------------------------------------------------------------------------------
ALTER TABLE runkit.process_handler_checkpoints
DROP CONSTRAINT handler_checkpoints_stream_id_fkey;

--------------------------------------------------------------------------------
-- Drop the "failures" and "resume_at" columns, which the engine doesn't use.
--------------------------------------------------------------------------------
ALTER TABLE runkit.process_handler_checkpoints
DROP COLUMN failures,
DROP COLUMN resume_at;
