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
`runkit`, which [`bootstrap.sql`] creates before this SQL runs. The
subdirectories only group the SQL by the component of the engine that uses it.

The engine applies every file in one transaction. It goes through the
subdirectories in lexical order of their names, and through the files in each
subdirectory in lexical order of theirs. A file that uses an object from another
file must come later in that order. For example, `20-aggregate/10-funcs.sql`
uses the `event` type from `00-eventstream/10-funcs.sql`.

Only the files in the subdirectories are applied. Files directly in this
directory, such as this one and `embed.go`, aren't.

<!-- references -->

[`bootstrap.sql`]: ../bootstrap/bootstrap.sql
[`version.go`]: ../../version.go
[migrations]: ../migrations/
