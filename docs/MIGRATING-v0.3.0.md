# Migrating to version 0.3.0

## Overview

Version 0.3.0 stores commands and deadlines in a single table,
`messagequeue.messages`, in place of `commandqueue.commands` and
`process.deadlines`. Calling `CreateSchema()` moves any pending commands and
deadlines into the new table, and then drops the old tables.

The upgrade requires downtime. Every engine that uses the database must be
stopped before `CreateSchema()` runs, and stay stopped until it has finished.

## Before you upgrade

- **Stop every engine.** An engine from an earlier version that is still
  running reads and writes the old tables. A command it adds while the
  migration runs can be lost, and once the old tables are dropped, it fails.
- **Back up the database.** The migration has no way back, because it drops the
  old tables. See [backing up].

## Upgrading

1. Stop every engine that uses the database.
2. Back up the database.
3. Deploy version 0.3.0, and call `CreateSchema()` against the database before
   any engine starts. If your application calls `CreateSchema()` at startup, as
   the [README] shows, starting the first engine does this. Calls from several
   engines at once are safe, since each waits for the one before it.
4. Start the remaining engines.

## Backing up

Back up the whole database, not only the engine's schemas. The engine's state
and any data your application keeps beside it need to stay consistent with each
other if you restore. Take the backup after every engine has stopped, so that
nothing changes while it runs.

If your PostgreSQL provider offers snapshots, such as Amazon RDS, Google Cloud
SQL or Azure Database for PostgreSQL, take a manual snapshot. Manual snapshots
are usually kept until you delete them, unlike automatic backups, which expire.

Otherwise, use `pg_dump`:

```sh
pg_dump --format=custom --file=before-0.3.0.dump "$DATABASE_URL"
```

Use the `pg_dump` from the same major version of PostgreSQL as the server, or a
newer one. Check that the backup restores before you rely on it, for example by
restoring it into a scratch database.

## What the migration does

- Each pending command keeps its message ID, its failure count and the time of
  its next attempt. Its queue ID is its message type ID, and its scheduled time
  is when it was added to the queue.
- Each pending deadline keeps its message ID, its handler key and instance ID,
  its failure count and the time of its next attempt. Its queue ID is its
  handler's key.
- These queue IDs differ from those of new messages, which use the ID of the
  node that produced them. They spread the migrated messages across the
  running engines, rather than leaving them all to one.
- Idempotency keys are kept, so a command executed again with the same key
  after the upgrade is still deduplicated.
- A message that had already failed is recorded as failing since the time of its
  next attempt.
- A deadline that had already failed is recorded as scheduled for the time of
  its next attempt, because earlier versions stored its original scheduled
  time only inside its envelope. The envelope still has the original time.

## Checking the result

Before upgrading, count the pending commands and deadlines:

```sql
SELECT COUNT(*) FROM commandqueue.commands;
SELECT COUNT(*) FROM process.deadlines;
```

After `CreateSchema()` returns, the same counts are:

```sql
SELECT COUNT(*) FROM messagequeue.messages WHERE handler_key IS NULL;
SELECT COUNT(*) FROM messagequeue.messages WHERE handler_key IS NOT NULL;
```

The first counts commands and the second counts deadlines. If engines started
before you ran these queries, the counts may already be lower, since delivered
messages are removed.

## Rolling back

There's no automatic way back to an earlier version. Stop every engine, and
restore the backup you took before the upgrade. Restoring it also discards
everything the engines recorded after the upgrade.

To restore a snapshot, follow your provider's instructions. Most restore a
snapshot into a new database server, so point the engines at it before starting
them.

To restore a `pg_dump` backup into the same database, first drop the schema
that the upgrade created, since `pg_restore` only replaces what's in the
backup:

```sh
psql "$DATABASE_URL" --command='DROP SCHEMA messagequeue CASCADE'
pg_restore --clean --if-exists --dbname="$DATABASE_URL" before-0.3.0.dump
```

<!-- anchors -->

[backing up]: #backing-up

<!-- references -->

[readme]: ../README.md
