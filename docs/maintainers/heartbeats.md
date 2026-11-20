# Heartbeats

This guide says what a heartbeat may contain, and how to change it so that nodes
of different runkit releases keep working together.

A heartbeat is a message by which a node tells the whole cluster, itself
included, that it is live. Each node sends one every heartbeat interval, as a
PostgreSQL notification on the `heartbeat` channel. A node knows which other
nodes are live by receiving their heartbeats, and it removes a node from its live
nodes when that node's heartbeat has expired.

## The format

The payload of the notification is the base64 encoding of the binary form of a
`Heartbeat` message, defined in [`heartbeat.proto`]. The encoding is base64
because the payload of a notification is text. PostgreSQL allows a payload of
fewer than 8000 bytes, and a heartbeat is about 40 characters.

A heartbeat has these fields:

- `node_id` is the ID of the sender.
- `expires_after` is how long after a node receives the heartbeat it removes the
  sender from its live nodes, unless it receives another heartbeat from the
  sender first. The receiver counts from when it receives the heartbeat, not
  from the sender's clock, which may differ from its own. So nodes don't need to
  share one heartbeat interval or timeout.

A receiver ignores a heartbeat that isn't valid: one that isn't base64 or a
`Heartbeat` message, has a missing, invalid or nil `node_id`, or has an
`expires_after` that is missing, invalid or not positive. It logs the heartbeat as a WARN.

## Changing the format

During a rolling upgrade, nodes of two releases exchange heartbeats. A node that
can't read another release's heartbeats counts that release's nodes as not live,
and both groups then own the workloads with the same rendezvous keys until the
upgrade ends. So a change to a heartbeat follows these rules:

- Add a field with a new field number. Never reuse a field number, and never
  change a field's type, meaning or unit. When a field is removed, mark its
  number and name `reserved` in the `.proto` file.
- A new field is optional. A heartbeat without it comes from a release that
  doesn't send it, or from a sender that has nothing to report, so a receiver
  must handle both.
- A receiver ignores unknown fields. It decodes a heartbeat with the binary
  form of the message, which skips them. Don't decode it with `protojson`, which
  rejects them by default.
- `node_id` and `expires_after` are all that every release needs to count a node as
  live, so they never change. A change to what a receiver does with another
  field, such as a node's capacity, mustn't change which nodes it counts as
  live.
- Keep the payload well under 8000 bytes.

A change that can't follow these rules, such as sending heartbeats on a
different channel, needs a transitional release that sends each heartbeat in
both the old and the new form. A release that stops every engine, such as one
with a major schema upgrade, can make the change without it.

<!-- references -->

[`heartbeat.proto`]: ../../internal/cluster/internal/heartbeatpb/heartbeat.proto
