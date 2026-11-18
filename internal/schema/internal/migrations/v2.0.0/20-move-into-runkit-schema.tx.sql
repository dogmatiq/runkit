--------------------------------------------------------------------------------
-- Drop the old triggers and functions. "current/", which is applied after this
-- migration, creates them again in the "runkit" schema, under new names.
--------------------------------------------------------------------------------
DROP TRIGGER signal_create ON eventstream.streams;
DROP TRIGGER signal_append ON eventstream.events;

DROP FUNCTION eventstream.signal_create();
DROP FUNCTION eventstream.signal_append();
DROP FUNCTION eventstream.append_any(uuid, uuid, text, eventstream.event[]);
DROP FUNCTION eventstream.append(uuid, uuid, uuid, text, eventstream.event[]);
DROP FUNCTION eventstream.acquire_for_write();

--------------------------------------------------------------------------------
-- Move the "event" type, which keeps its name.
--------------------------------------------------------------------------------
ALTER TYPE eventstream.event SET SCHEMA runkit;

--------------------------------------------------------------------------------
-- Rename the tables whose names would clash in one schema, or that get a more
-- descriptive name.
--
-- When PostgreSQL creates a constraint that has no explicit name, it derives
-- the name from the table's name. Renaming the table doesn't rename the
-- constraint, or the index of a primary key. They are renamed here, to match
-- the names in a new database.
--
-- Each table is renamed before it moves, so that the primary key indexes of
-- "aggregate.instances" and "process.instances", which are both named
-- "instances_pkey", don't clash in the "runkit" schema.
--------------------------------------------------------------------------------
ALTER TABLE eventstream.streams RENAME TO event_streams;
ALTER TABLE eventstream.event_streams RENAME CONSTRAINT streams_pkey TO event_streams_pkey;
ALTER TABLE eventstream.event_streams RENAME CONSTRAINT streams_id_not_null TO event_streams_id_not_null;
ALTER TABLE eventstream.event_streams RENAME CONSTRAINT streams_next_offset_not_null TO event_streams_next_offset_not_null;
ALTER TABLE eventstream.event_streams RENAME CONSTRAINT streams_next_offset_check TO event_streams_next_offset_check;

ALTER TABLE aggregate.instances RENAME TO aggregate_instances;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_pkey TO aggregate_instances_pkey;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_handler_key_not_null TO aggregate_instances_handler_key_not_null;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_instance_id_not_null TO aggregate_instances_instance_id_not_null;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_instance_id_check TO aggregate_instances_instance_id_check;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_stream_id_fkey TO aggregate_instances_stream_id_fkey;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_snapshot_offset_check TO aggregate_instances_snapshot_offset_check;
ALTER TABLE aggregate.aggregate_instances RENAME CONSTRAINT instances_check TO aggregate_instances_check;

ALTER TABLE process.handlers RENAME TO process_handlers;
ALTER TABLE process.process_handlers RENAME CONSTRAINT handlers_pkey TO process_handlers_pkey;
ALTER TABLE process.process_handlers RENAME CONSTRAINT handlers_handler_key_not_null TO process_handlers_handler_key_not_null;
ALTER TABLE process.process_handlers RENAME CONSTRAINT handlers_has_checkpoint_offsets_not_null TO process_handlers_has_checkpoint_offsets_not_null;

ALTER TABLE process.instances RENAME TO process_instances;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_pkey TO process_instances_pkey;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_handler_key_not_null TO process_instances_handler_key_not_null;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_instance_id_not_null TO process_instances_instance_id_not_null;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_instance_id_check TO process_instances_instance_id_check;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_ended_not_null TO process_instances_ended_not_null;
ALTER TABLE process.process_instances RENAME CONSTRAINT instances_check TO process_instances_check;

ALTER TABLE eventstream.handler_checkpoints RENAME TO process_handler_checkpoints;
ALTER TABLE eventstream.process_handler_checkpoints RENAME CONSTRAINT handler_checkpoints_pkey TO process_handler_checkpoints_pkey;
ALTER TABLE eventstream.process_handler_checkpoints RENAME CONSTRAINT handler_checkpoints_handler_key_not_null TO process_handler_checkpoints_handler_key_not_null;
ALTER TABLE eventstream.process_handler_checkpoints RENAME CONSTRAINT handler_checkpoints_stream_id_not_null TO process_handler_checkpoints_stream_id_not_null;
ALTER TABLE eventstream.process_handler_checkpoints RENAME CONSTRAINT handler_checkpoints_checkpoint_offset_not_null TO process_handler_checkpoints_checkpoint_offset_not_null;
ALTER TABLE eventstream.process_handler_checkpoints RENAME CONSTRAINT handler_checkpoints_checkpoint_offset_check TO process_handler_checkpoints_checkpoint_offset_check;

ALTER TABLE projection.handlers RENAME TO projection_handlers;
ALTER TABLE projection.projection_handlers RENAME CONSTRAINT handlers_pkey TO projection_handlers_pkey;
ALTER TABLE projection.projection_handlers RENAME CONSTRAINT handlers_handler_key_not_null TO projection_handlers_handler_key_not_null;
ALTER TABLE projection.projection_handlers RENAME CONSTRAINT handlers_last_compacted_at_not_null TO projection_handlers_last_compacted_at_not_null;

--------------------------------------------------------------------------------
-- Move each table into the "runkit" schema. A table moves with its rows,
-- indexes and constraints, and no rows are copied.
--------------------------------------------------------------------------------
ALTER TABLE application.identity SET SCHEMA runkit;
ALTER TABLE eventstream.event_streams SET SCHEMA runkit;
ALTER TABLE eventstream.events SET SCHEMA runkit;
ALTER TABLE eventstream.process_handler_checkpoints SET SCHEMA runkit;
ALTER TABLE aggregate.aggregate_instances SET SCHEMA runkit;
ALTER TABLE process.process_handlers SET SCHEMA runkit;
ALTER TABLE process.process_instances SET SCHEMA runkit;
ALTER TABLE projection.projection_handlers SET SCHEMA runkit;

--------------------------------------------------------------------------------
-- Drop the old schemas. They're dropped without CASCADE, so if one still holds
-- anything, such as a table of the application's, the migration fails instead
-- of dropping it.
--------------------------------------------------------------------------------
DROP SCHEMA
    application,
    eventstream,
    commandqueue,
    aggregate,
    integration,
    process,
    projection;
