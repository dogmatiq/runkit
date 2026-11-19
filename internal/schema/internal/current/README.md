# The current schema

This directory holds the SQL that defines the engine's tables, functions and
triggers as they are at the schema version that this runkit version needs. That
version is `TargetVersion`, in [`version.go`].

A new database gets only this SQL. Every upgrade of an existing database applies
all of it too, after any [migrations], so each statement must work on a database
that already has what it creates. For example, use `CREATE TABLE IF NOT EXISTS`
and `CREATE OR REPLACE FUNCTION`.

## Order

The engine's tables, functions and triggers are all in one PostgreSQL schema,
`runkit`, which [`bootstrap.sql`] creates before this SQL runs.

The engine applies every SQL file in one transaction, in lexical order of the
file names. A file that uses an object from another file must come later in
that order. For example, `20-aggregate.sql` uses the `event` type from
`00-eventstream.sql`.

An upgrade runs this SQL while engines are using the database. A statement that
creates an index or a trigger locks its table, even when the index or trigger
already exists, so the order of the statements is also the order in which the
upgrade locks tables. Make that order match the order in which the engine's
transactions lock the same tables. Otherwise, an upgrade and an engine can each
hold the lock on one table and wait for the lock on the other, and PostgreSQL
ends one of them with an error. For example, an engine that appends events locks
the `event_streams` table, then the `events` table, so `00-eventstream.sql`
locks `event_streams` before any statement that locks `events`.

<!-- references -->

[`bootstrap.sql`]: ../bootstrap/bootstrap.sql
[`version.go`]: ../../version.go
[migrations]: ../migrations/
