# Ack/Nack should fail if they affect 0 rows

`commandqueue.Ack()` and `commandqueue.Nack()` currently ignore the case where
the UPDATE/DELETE affects zero rows. This silently succeeds even if the command
doesn't exist (e.g., already acked, or bad message ID).

They should check `RowsAffected()` and return an error if zero rows were
affected, indicating a programming error in the caller.

## Possibly related

- [internal/commandqueue/queue.go](../../internal/commandqueue/queue.go)
