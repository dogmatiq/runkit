# Plan: Heartbeats on their own channel, in a format later releases can extend (#115)

## Where the work stands

Nothing is started. The work goes on a new branch, `heartbeat-channel`, from
`main` (PR #133 is merged, so `dogma-v0.26` is finished). When execution
starts, this plan is copied to `docs/.plans/2026-11-20-heartbeat-channel.md`
and staged, and its tasks are ticked off there.

## Context

0.3.0 already needs every engine to be stopped for its schema upgrade. A change
to how nodes exchange heartbeats splits a cluster during a rolling upgrade,
because each version counts only its own nodes as live. So the heartbeat format
must be settled in 0.3.0, and it must let later releases add to it without
another stop. **Forward compatibility is the goal of this work; the capacity
score is not.**

Today a heartbeat is a `signal.Bus` signal on the shared `signal` channel, with
the topic `heartbeat.<node ID>`. It carries nothing else, every node must use
the same timeout, and `signal.Bus` holds a second `LISTEN` connection only for
heartbeats. The send goes through the pool with no deadline (#89), on the
goroutine that also expires nodes.

## Decided in planning

- The capacity score, the sequence number and reading peers' scores move to a
  follow-up issue. This work changes only how heartbeats are delivered.
- The payload is binary protobuf, encoded as base64, because `NOTIFY` payloads
  are text. Field numbers and `reserved` make the "only add fields" rule part
  of the schema. All payloads are far under the 8000-byte limit: about 40
  characters today (one byte more with `Duration`), and about 57 once a score and a sequence number are added.
- `Node.HeartbeatTimeout` stays a duration, and the node sends it as the
  `ExpiresAfter` of each heartbeat. It doesn't become a multiplier, as #115 says.

## Settled design

- **Channel.** Heartbeats are sent with `pg_notify('heartbeat', <payload>)`.
  `Node.listen` in `internal/cluster/signal.go` runs `LISTEN` on both `signal`
  and `heartbeat` on its one connection, and routes each notification by
  `notification.Channel`. A heartbeat is handed to the heartbeat loop through a
  buffered Go channel of `heartbeatBufferSize`, and dropped when that is full,
  as today.
- **Payload: protobuf, with fields only ever added.** A message in
  `internal/cluster/internal/heartbeatpb/heartbeat.proto` (`edition = "2024"`, as in
  enginekit) holds the sender's node ID as a `uuidpb.UUID`, and a `google.protobuf.Duration`, `expires_after`, saying how long the
  heartbeat keeps the sender live. It is sent as
  `base64.StdEncoding` of the binary encoding. Binary decoding skips
  unknown fields, so a node of this release accepts a heartbeat from a later
  release that adds fields. (`protojson` would reject them by default, so it
  isn't used.) The heartbeat joins the protobuf boundaries listed in
  `docs/maintainers/uuids.md`: the node ID crosses it with `uuidpb.FromStd()`
  and `AsStd()`.
- **Code generation.** The `Makefile` includes
  `.makefiles/pkg/protobuf/v2/Makefile` and
  `.makefiles/pkg/protobuf/v2/with-primo.mk`, as enginekit's does. The
  generated `heartbeat.pb.go` and `heartbeat_primo.pb.go` are committed, and
  the makefiles add them to `GENERATED_FILES`. The Go code builds the message
  with Primo's builder, and encodes and decodes it with Primo's
  `MarshalBinary` and `UnmarshalBinary`.
- **Compatibility rules**, written in a new `docs/maintainers/heartbeats.md`:
  - never reuse a field number, or change a field's type, meaning or unit; a
    removed field's number and name are `reserved`;
  - a new field is optional, and its absence means the sender doesn't report
    it, so a receiver must handle heartbeats from older releases;
  - a receiver ignores fields it doesn't know, so it decodes with binary
    decoding, never with `protojson` defaults;
  - the node ID and `ExpiresAfter` are what every release needs to count a node
    as live, so they never change;
  - a change that can't follow these rules needs a transitional release that
    sends on both the old channel and a new one;
  - a payload must stay under PostgreSQL's 8000-byte `NOTIFY` limit.
- **Invalid heartbeats.** A payload that isn't base64 or protobuf, or has no valid
  node ID, the nil UUID, or a non-positive `ExpiresAfter`, is logged as a WARN and
  ignored, as an invalid node ID is today.
- **Expiry counted from receipt.** A receiver removes a peer when the
  `ExpiresAfter` of its most recent heartbeat has passed, counted from when the
  receiver read it, because clocks differ. `Node.heartbeats` maps each node ID
  to the time its heartbeat expires, instead of the time it was received.
- **First heartbeat once listening.** The loop sends a heartbeat each time the
  listening connection becomes ready, instead of once before the loop. That
  replaces the startup send, makes sure a joining node hears the replies of the
  existing nodes, and announces the node again after a reconnect. The tests'
  `setup` workaround for a bus that isn't yet listening goes away.
- **Send deadline (#89's heartbeat part).** Each send runs under
  `context.WithTimeout(ctx, n.HeartbeatInterval)`. A failed send is already
  retried on the next tick. The rest of #89 stays open.
- **One `LISTEN` connection.** `signal.Bus` and `internal/signal` are deleted,
  with `Node.Signals` and the engine's `signals` field.

## Words

- **`heartbeat` channel**: the PostgreSQL notification channel for heartbeats.
- **heartbeat** (the protobuf message `heartbeatpb.Heartbeat`, which the code
  uses directly): "A heartbeat is a message by which a node tells the whole
  cluster, itself included, that it is live."
- **ExpiresAfter** (`expires_after` on the wire, `ExpiresAfter` in Go): "How
  long the heartbeat keeps its sender live. A node that receives the heartbeat
  counts that time from the moment of receipt, and removes the sender from its
  live nodes if the sender sends no newer heartbeat within that time." Chosen in
  review, over `ValidFor` and `TTL`. It is a `google.protobuf.Duration`, so the
  name has no unit.

## Tasks

### [x] The heartbeat payload and its compatibility rules (05abba8)

Before this task, write down the rule that Go code generated from a Dogmatiq
repository's own `.proto` files always uses Primo. It goes in a skill in
dogmatiq/agent-config, such as the building skill, as a separate small change
made in a clone that the agent creates, and reviewed on its own. Then add the protobuf code generation, with Primo, to the `Makefile`,
`internal/cluster/internal/heartbeatpb/heartbeat.proto` and its generated code,
`internal/cluster/heartbeat.go` with the encoding and validation,
`docs/maintainers/heartbeats.md` with the rules above, and the heartbeat to the
boundaries in `docs/maintainers/uuids.md`. Nothing
sends or receives the new payload yet, so behavior doesn't change. The decoding
is tested through `Node` in the next task, not on its own, so it stays
unexported.

### [x] Send and receive heartbeats on the `heartbeat` channel

Change `internal/cluster/node.go` and `signal.go` as the settled design says:
the second `LISTEN`, the routing, `pg_notify` with the payload and the
deadline, expiry from each heartbeat's `ExpiresAfter`, and the first heartbeat once
listening. `Node.Signals` is removed, and so is every use of `signal.Bus` outside
its own package: the engine no longer creates or runs one, and the tests in
`internal/cluster`, `internal/messagequeue` and `internal/eventstream` no longer
construct one. `Node.HeartbeatTimeout`'s doc says it is the `ExpiresAfter` that
this node sends.

Tests in `internal/cluster/node_test.go` inject heartbeats with `pg_notify` and
observe them with a `LISTEN` connection of the test's own. Changed and new
subtests:

- it sends heartbeats on the heartbeat channel, with its ID and `ExpiresAfter`;
- it counts a peer as live for the `ExpiresAfter` in the peer's heartbeat, not its
  own timeout (one peer with a long `ExpiresAfter`, one with a short one);
- it accepts a heartbeat that has fields it doesn't know;
- it ignores a heartbeat that isn't valid (table of the cases above);
- the existing nil-UUID, failed-send, rejoin and "learns about existing nodes"
  subtests, adapted.

The send deadline isn't covered by a test, because the test can't make a
connection hang; its comment says so, as `drainHeartbeats` does.

### [ ] Delete `signal.Bus`

Delete `internal/signal`, which nothing imports any more.

### [ ] Changelog and the issues

Add a changelog entry only if the maintaining-changelogs skill says the change
is visible to users; the existing downtime notice already covers the upgrade.
File the follow-up issue for the capacity score, the sequence number and
reading peers' scores, using the filing-an-issue skill. Update #115 to say
what this PR does and link the follow-up.

## Out of scope

- The capacity score, how the consumer measures waiting work, the sequence
  number, and reading peers' scores from `cluster.Node`. They go in a
  follow-up issue, and arrive as new optional fields under the rules above.
  Old nodes ignore them, and a score-based allocator treats a node without a
  score as the follow-up decides.
- The rest of #89, and #125.

## Verification

- Before presenting a large task and before committing, `make precommit`
  (with `-p 1`, as the Makefile sets). After small review fixes, only
  `go test ./internal/cluster/...` through `make`.
- After the deletion task, `grep -rn 'internal/signal' --include='*.go' .`
  finds nothing, and the engine and cluster tests in `internal/projection`
  (`distribution_test.go`) and `internal/messagequeue` pass.
- By hand: run two engines against one database, and check with `LISTEN
  heartbeat;` in `psql` that heartbeats arrive about once a second from each
  engine, and decode one with `base64 -d | protoc --decode_raw`.
