When a deadline fires, we don't verify that the timeout message type is still in the
process handler's route list. If a handler is updated to remove a timeout type, any
previously-scheduled deadlines of that type would still fire and be dispatched to the
handler, likely causing a panic or unrouted-message error at runtime.

This mirrors the broader question of what happens when message types are removed from
a handler's routes while instances are in-flight. Deadlines may be the sharpest case
because they can be scheduled far in the future.

## Resolution

The deadline pump's acquisition query now filters by `d.message_type_id = ANY($3)`,
passing the handler's current `DeadlineTypeIDs`. Deadlines whose type is no longer in
the route list are silently skipped rather than delivered.

Deletion is intentionally not performed: an unroutable deadline might have been
created by a node running newer code during a rolling restart, or by a version that
was later rolled back. Skipping preserves those deadlines so they can be delivered if
the route is reinstated.
