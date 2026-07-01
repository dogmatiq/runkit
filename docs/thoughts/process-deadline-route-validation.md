When a deadline fires, we don't verify that the timeout message type is still in the
process handler's route list. If a handler is updated to remove a timeout type, any
previously-scheduled deadlines of that type would still fire and be dispatched to the
handler, likely causing a panic or unrouted-message error at runtime.

This mirrors the broader question of what happens when message types are removed from
a handler's routes while instances are in-flight. Deadlines may be the sharpest case
because they can be scheduled far in the future.

Possible mitigations:
- Validate the timeout type against the current route list when the deadline pump
  dequeues a deadline, and discard (or dead-letter) unroutable ones.
- Treat removed timeout types as a schema migration concern and document the
  operator's responsibility to drain or cancel outstanding deadlines before removing a
  route.
