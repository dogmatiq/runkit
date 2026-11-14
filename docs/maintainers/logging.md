# Logging

This guide holds the rules for every log record the engine writes. Follow it
when you add or change a log call, when you handle a failure, and when you
write code that retries work. It's written for both people and agents. Each
rule says what to do and why, and the checklist at the end covers a new log
call.

## Terms

This guide uses the terms from `log/slog`.

- A **record** is one entry in the log. It has a level, a log message and
  attributes.
- The **log message** is a record's text. It's called "log message" here, never
  just "message", to keep it apart from the messages that handlers produce and
  handle.
- An **attribute** is a key and a value attached to a record. Durations,
  counts, errors and identities are always attributes, never part of the log
  message.

## Layers

Each record belongs to one of two layers.

- **Application records** are a record of what the application did. They cover
  what a handler explicitly does through its scope, which is producing a
  message, calling `Log()`, and mutating or ending a process instance. They
  also cover commands sent through a `CommandExecutor`.
- **Engine records** are everything else, including everything about handling
  a message, such as a handler failing, a delivery being postponed, or a
  command being deduplicated.

Every record carries a `layer` attribute, with the value `application` or
`engine`. A log site never sets it. Instead the engine builds two loggers from
one base that carries `node_id` and the application's identity, and each
logger adds its own `layer`.

The name of a field, parameter or variable that holds a logger includes the
logger's layer. Name a field `EngineLogger` or `AppLogger`, or `engineLogger`
or `appLogger` if it's unexported, and name a parameter or variable
`engineLogger` or `appLogger`. Never use the name `Logger` or `logger`, even
where only one logger is in scope.

A logger carries only the context that's true of every record written through
it, such as the layer, the node and the application. Any other context, such as
which handler or which message a record is about, is added by the code that
knows it, at the point where it knows it. That keeps each attribute in one
place, and lets one logger serve code that works for many handlers.

When other code decides whether some work counts, such as by committing its
transaction, the code doing the work doesn't keep an application logger of its
own. The deciding code passes one in with each call, so that it controls when
the records are written.

## Attributes

| Attribute | Engine records | Application records |
| --- | --- | --- |
| `layer` | `engine` | `application` |
| `node_id` | The ID of the node that wrote the record | The ID of the node that wrote the record |
| Application identity | The application's name and key | The application's name and key |
| Handler identity | The handler's name, key and type, on records about a handler | The handler's name, key and type, on records from a handler |
| `handled_message` | The metadata of the message being handled, on records about a delivery | The metadata of the message being handled, on records from a handler |
| `produced_message` | The metadata of the message produced, on records about producing it | The metadata of the message produced |
| Instance ID | The instance's ID, on records about an instance | The instance's ID, on records from an instance |
| `delivery` group | The ID of the message being delivered, on records about a delivery | — |
| Stream | The stream's ID and offset, on records about a stream | — |

A message is always described by its metadata, which is the attributes written
by `xslog.Envelope`. They are the message's ID, causation ID, correlation ID,
description, and type, including its kind, but never its body. A record never
refers to a message by its ID alone.

The metadata is identical in both layers, and its key is the message's role,
never its kind. The message being handled is always `handled_message`, and a
message that a handler or a `CommandExecutor` produces is always
`produced_message`. A record about producing a message has both. A search on
`handled_message.message_id` finds everything about handling a message and
everything it caused, in both layers.

Attributes and log messages name things that a person running the engine knows
about, such as handlers, streams, messages and nodes. Never name internal
packages or the parts of the engine's code.

## Application records

An application record is written only if what it records actually happened. A
handler attempt that is rolled back or retried writes no application records.

While a handler attempt runs, the two layers say different things.

- **Engine, DEBUG, written as it happens.** What the handler did through the
  scope, such as recording an event, executing a command, scheduling a
  deadline, mutating or ending the instance, or calling `Log()`. These records
  are written even if the attempt fails, which makes them the place to look
  when it does. The record for a produced message has that message's metadata.
  The record for a `Log()` call has the text the handler logged as an
  attribute.
- **Application, INFO, written once it has happened.** A record of what the
  application did, in the order the handler did it. The record for each
  message it produced uses the message's own description as its log message,
  and has that message's metadata. `Log()` calls, and a process instance being
  mutated or ending, are recorded too. Ending is always last, because a scope
  rejects further effects once `End()` has been called.

Every application record from a handler has the metadata of the message being
handled, so a search on that message's ID finds everything it caused.

The code that decides whether the handling of a message counts buffers the
application records from that handling. It might decide by committing a
transaction, or by seeing that the handler has moved its checkpoint past the
message. It writes the records together if the handling counts, keeping the
time of each call, and discards them if it doesn't.

| Where | When the application records are written |
| --- | --- |
| Aggregate and integration handlers, and process deadlines | After an attempt completes the delivery and its transaction commits |
| Process events | After the attempt's transaction commits |
| Projection `HandleEvent` | After `HandleEvent` returns successfully and moves the checkpoint past the event |
| Projection `Compact` | Immediately, because the application compacts in its own storage, outside the engine's transaction |
| `CommandExecutor` | After the command is in the queue |

Where application records are written immediately, no engine record repeats
the same call.

An instance beginning isn't recorded. The first message that an instance
handles creates it implicitly, which isn't something the handler did.

## Levels

### ERROR

ERROR means the engine can't do something it's meant to do and isn't
recovering on its own. Use it for:

- a deterministic failure, as soon as it happens
- any other failure once its streak has lasted as long as the error log threshold,
  counted from the first failure in the streak

### WARN

WARN means the engine received something it couldn't make sense of, such as a
malformed signal or bad data from a peer or a remote engine, so it didn't act
on it. Because it was never understood, the engine can't say whether ignoring
it is a fault. It's evidence that something outside this node is misbehaving,
possibly maliciously.

The engine's own failures never use WARN. When bad input also stops work, log
both. The WARN record describes the sender, and the failure rules below
describe what this node can't do.

### INFO

INFO is used for all application records. An engine record uses INFO only if
its event meets both of these criteria.

- It changes what this node is doing or can do.
- It happens rarely, about once per process lifetime or per incident.

Examples are the engine starting and stopping, a server listening on an
address, a remote engine being added or removed, and the recovery that ends an
ERROR. A change that happens per stream, per message or per instance fails the
second criterion, so it's DEBUG.

### DEBUG

DEBUG is everything else, including expected events, anything that happens per
stream, per message or per instance, and failures that haven't yet earned an
ERROR.

## Failures

A failure is logged once, by the code that decides what to do about it, which
means retrying it, giving up or ignoring it. Code that only propagates an
`error` to its caller doesn't log it. Give the deciding code's logger the
context it needs, or make the `error` carry it.

An expected event isn't a failure, even if the code that reports it returns an
`error`. For example, consuming a stream returns an `error` if the stream
leaves the catalog, but that is a normal change. The code consuming the stream
waits for it to come back and logs nothing above DEBUG.

### Streaks

A *streak* is a run of consecutive failures of the same work, from its first
failure until it recovers. *Recovery* is the point at which the work is known
to be succeeding again. How recovery is detected depends on the work and how
it's retried.

A *deterministic* failure is one that the same attempt meets every time, until
something outside the engine changes, such as a handler's code, its data or a
remote engine. Retrying alone can't clear it. The code that decides about a
failure classifies it as deterministic only when it knows this from the kind of
failure. Any failure it can't classify is treated as transient.

| Event | Level |
| --- | --- |
| A deterministic failure, if the streak hasn't logged an ERROR | ERROR |
| The first failure once the streak is as old as the error log threshold, if it hasn't logged an ERROR | ERROR |
| Any other failure | DEBUG |
| Recovery, after the streak logged an ERROR | INFO |
| Recovery, after a streak that logged no ERROR | DEBUG |
| Shutdown during a streak | — |

A streak logs at most one ERROR. Its log message describes the failure itself,
like any other failure record.

The error log threshold and the delays between retries are set by the *failure
policy* of each kind of work. The engine options `WithEngineFailurePolicy`,
`WithMessageQueueDeliveryFailurePolicy`, `WithEventStreamDispatchFailurePolicy`
and `WithProjectionCompactionFailurePolicy` set them. By default, the error
threshold is 30 seconds.

Failure and recovery records have these attributes, as well as the attributes
that identify the work. Each duration starts or ends at the record's own time.

| Attribute | Failure records | Recovery records |
| --- | --- | --- |
| `error` | The error | — |
| `failures` | The number of failures in the streak, including this one | The number of failures in the streak |
| `failing_since` | The time of the streak's first failure | The time of the streak's first failure |
| `failing_for` | The time from the streak's first failure to this record | The time from the streak's first failure to this record |
| `next_attempt_at` | The time of the next attempt | — |
| `next_attempt_in` | The time from this record to the next attempt | — |
| `succeeding_since` | — | The time of the first success after the last failure, if the work must succeed for a quiet period to recover |
| `succeeding_for` | — | The time from that success to this record |

A streak's ERROR is followed by its recovery record when the work succeeds
again. Until then, the work is still failing, and later failures in the streak
are logged as DEBUG records.

A reader must be able to match a streak's ERROR record to its recovery record,
and tell that pair apart from every other streak, including earlier streaks of
the same work. Both records have the attributes that identify the work, plus
one that distinguishes this streak, such as when it started or an attempt
number. What that is depends on the work, but it must be there. When the work
can only fail in one streak, such as delivering one message, the attributes
that identify the work also identify the streak.

### Many units failing together

When one problem makes many units of work fail at once, such as every stream a
handler consumes, prefer a single ERROR. Where the code is already structured
so that one piece of code coordinates those units, that code decides what to
do about their failures, so it logs the ERROR. A unit logs its own failure, at
DEBUG, only when that adds context the coordinator's record lacks, such as
which stream or instance failed. Where no coordinator exists, each unit logs
its own ERROR. Don't restructure code just to create a coordinator.

## How the engine applies these rules

These are some of the decisions that follow from the rules above, for the
engine's main components. They aren't a complete list. For anything not
covered here, the rules above decide.

### Message queue

Each engine runs one consumer, which delivers the commands and deadlines for
every handler.

- The consumer holds the engine's base loggers. It passes a logger for each
  layer to the sink with each call, with the delivery's attributes added, so a
  queue sink holds no loggers of its own.
- Each sink describes itself with a log attribute, which identifies its
  handler. The consumer adds it to its own records about deliveries to that
  sink, and to the loggers it passes the sink, so a sink's records carry it
  without the sink adding it.
- The consumer commits each attempt, so it buffers that attempt's application
  records, and writes them only if the attempt completes the delivery and its
  transaction commits.
- Every record that the consumer or a sink writes about an attempt has the
  `delivery` group. A delivery can fail in only one streak, because its message
  is removed when it completes, so the group also identifies the streak.
- Any error from a sink, except a lock that another transaction holds, is a
  failure of the delivery, and the delivery's streak logs it. An error from the
  consumer's own work, such as finding the next messages or postponing a
  delivery, is logged as a failure to consume the queues instead.

### Projection compaction

The application compacts in its own storage, outside the engine's transaction,
so the compactor writes its application records immediately.

## Log messages

- An engine record's log message is a constant, so that every occurrence of an
  event can be found with one exact search, grouped and counted. Everything
  that varies goes in attributes.
- An application record's log message is the application's own text, which is
  a produced message's description or the text passed to `Log()`. That text is
  the content of the application's record, so it stays readable as it is.
- Say what was observed, not what it might mean. A recovery record says how
  long the work was failing and after how many failures it succeeded, and it
  doesn't claim that the work is healthy.
- Name things that a person running the engine knows about.
- Put values in attributes, not in the log message.

## Testing

Never assert on log records. Logging isn't tested.

Log records describe what the engine does, but they aren't part of its
behavior. Tests cover the behavior itself. A test that asserts on records ties
itself to levels, log messages and attributes, which then can't change without
breaking tests that say nothing about whether the engine works. Logging is
checked by review against this guide instead.

## Checklist for a new log call

1. Which layer is it? Use that layer's logger, and don't set `layer` by hand.
2. If it's about a failure, is this the code that decides what to do about it?
   If not, return the `error` and don't log it.
3. Is it an expected event? Then it's not a failure, so use DEBUG, or INFO if
   it's a rare change in what this node does.
4. Does it follow the streak rules, with at most one ERROR, and a recovery
   record that can be matched to it?
5. For an engine record, is the log message a constant?
6. Are values in attributes, and do the attributes match the table and name
   things a person running the engine would know about?
