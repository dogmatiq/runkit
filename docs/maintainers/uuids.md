# UUIDs

This guide says how runkit's code holds UUIDs, such as message IDs, stream IDs,
queue IDs and handler keys.

runkit holds every UUID as a [`uuid.UUID`], from Go's standard library. A
[`*uuidpb.UUID`] appears only as a field of a protobuf message. Envelopes,
identities and gRPC requests use `*uuidpb.UUID`, but that's no reason for other
code to use it.

## The protobuf boundary

Code that takes a UUID out of a protobuf message calls `AsStd()` on it, and
code that builds a protobuf message calls `uuidpb.FromStd()`. `AsStd()`
doesn't allocate. These are the places where IDs cross the boundary:

- envelopes — the message ID, type ID, causation ID and correlation ID, and
  the keys in the source identities;
- `identitypb.Identity.Key`, such as the identity key of a handler or of the
  application;
- the event stream gRPC API — `ReadEventsRequest.event_stream_id`,
  `EventStreamSegment.event_stream_id`, the type IDs in `MessageFilter`, and
  `EventStreamPosition.stream_id`;
- `Heartbeat.node_id`, the node ID in a heartbeat.

A UUID that comes from a gRPC peer can be invalid, so check it with
`Validate()` before calling `AsStd()`.

The standard library can't make a version 5 UUID, so rendezvous keys are still
made with `uuidpb.Derive()`, and the result is converted with `AsStd()`.

## Collections of UUIDs

Use a [`uuidset.Set`] for a set of UUIDs, and a `map[uuid.UUID]T` for a map.
When code shares a set with other code that mustn't change it, it shares a
read-only view, a `*uuidset.SetR`, from `Set.R()`. The view shows changes that
the owner of the set makes later, so a doc comment still says when the owner
mustn't change the set.

## The database

Pass a UUID to a SQL statement with [`xsql.UUID`], and scan one into a
`*uuid.UUID` with the same function. It writes the nil UUID as NULL, and reads
NULL as the nil UUID. runkit's IDs are version 4 or version 5 UUIDs, so the nil
UUID is never a real ID. A column that may be NULL, such as an aggregate
instance's stream ID, is compared with `uuid.Nil()` in Go.

In logs, use [`xslog.UUID`].

<!-- references -->

[`*uuidpb.UUID`]: https://pkg.go.dev/github.com/dogmatiq/protobuf/uuidpb#UUID
[`uuid.UUID`]: https://pkg.go.dev/uuid#UUID
[`uuidset.Set`]: ../../internal/uuidset/set.go
[`xslog.UUID`]: ../../internal/x/xslog/attr.go
[`xsql.UUID`]: ../../internal/x/xsql/marshal.go
