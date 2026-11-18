# The current schema

This directory holds the SQL that defines the engine's tables, functions and
triggers as they are at the schema version that this runkit version needs. That
version is `TargetVersion`, in [`version.go`].

A new database gets only this SQL. Every upgrade of an existing database applies
all of it too, after any [migrations], so each statement must work on a database
that already has what it creates. For example, use `CREATE TABLE IF NOT EXISTS`
and `CREATE OR REPLACE FUNCTION`.

## Order

Each subdirectory holds the SQL for one PostgreSQL schema. Its name is a
two-digit number, a hyphen and the name of that PostgreSQL schema, such as
`10-messagequeue`. Each file in it has a name made of a two-digit number, a
hyphen and a short description, such as `00-tables.sql`.

The engine applies every file in one transaction, in the order of their paths:
the subdirectories in the order of their names, and each subdirectory's files in
the order of theirs. So the numbers decide the order.

- A PostgreSQL schema that another one refers to has a lower number. For
  example, `20-aggregate` uses the `event` type from `00-eventstream`.
- PostgreSQL schemas that don't refer to each other can share a number, such as
  `20-aggregate` and `20-process`. Those are applied in alphabetical order.
- Within a subdirectory, tables come first, then functions, then triggers:
  `00-tables.sql`, `10-funcs.sql` and `20-triggers.sql`.

Only the files in the subdirectories are applied. Files directly in this
directory, such as this one and `embed.go`, aren't.

<!-- references -->

[`version.go`]: ../../version.go
[migrations]: ../migrations/
