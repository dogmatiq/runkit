--------------------------------------------------------------------------------
-- This file creates the table that stores the schema version of the database.
--
-- "Apply" runs it before it changes anything and before any migration, so no
-- migration can change what it creates. A change here has to be a statement
-- that is safe to run again and that an existing table survives, such as
-- "ADD COLUMN IF NOT EXISTS".
--------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS runkit;

--------------------------------------------------------------------------------
-- The "schema_version" table contains the schema version of the database.
--
-- The table contains at most one row. The unique index on the constant "true"
-- gives every row the same index entry, so PostgreSQL rejects a second row.
--
-- "version" is the schema version that the database has reached, such as
-- "v2.0.0", and "applied_at" is the time that it was reached.
--
-- "migrating_to" and "migration_steps_completed" are both NULL, or both set
-- while a migration is in progress. "migrating_to" is the schema version of the
-- migration in progress, and "migration_steps_completed" is how many of its
-- steps have completed. "version" doesn't change until every migration has
-- finished.
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runkit.schema_version (
    version                   text        NOT NULL,
    applied_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    migrating_to              text,
    migration_steps_completed int         CHECK (migration_steps_completed > 0),

    CHECK ((migrating_to IS NULL) = (migration_steps_completed IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS schema_version_singleton
ON runkit.schema_version ((TRUE));
