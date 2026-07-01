# Guard checkpoint updates with expected offset

When updating the `checkpoint_offset` on `eventstream.handler_checkpoints`,
include the expected current offset in the `WHERE` clause (or use an assertion
via a stored function) so that the `UPDATE` fails loudly if the row's offset
doesn't match what the pump believes it should be.

In principle this can't happen — the row is locked `FOR UPDATE SKIP LOCKED`
during acquisition and the same transaction performs the update, so no other
transaction can advance the offset out from under us. But an incorrect update
here silently drops or replays events, and the defensive check is cheap:

```sql
UPDATE eventstream.handler_checkpoints SET
    checkpoint_offset = $1,
    failures = 0
WHERE handler_key = $2
    AND stream_id = $3
    AND checkpoint_offset = $4 -- expected current offset
```

Combined with `xsql.ExecOne`, a mismatch surfaces immediately as "expected 1
row affected" rather than as inexplicable downstream corruption.

Applies to both the projection and process pumps (and any future consumer
that persists a checkpoint).

## Possibly related

- [internal/projection/eventpump.go](../../internal/projection/eventpump.go)
- [internal/process/eventpump.go](../../internal/process/eventpump.go)
- [internal/x/xsql/exec.go](../../internal/x/xsql/exec.go)
