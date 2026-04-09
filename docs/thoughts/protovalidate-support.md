# protovalidate Support

If a user's protobuf message or aggregate root type has [protovalidate] constraints
defined, the engine should validate the message automatically at the system boundary
-- before any handler is invoked and before a message is accepted into the engine's
internal pipelines.

This means users who adopt protovalidate on their types get validation "for free"
without having to wire it in themselves.

## Open Questions

- Where exactly is the right validation point -- at command submission, event
  recording, or both?
- Should validation failures surface as a distinct error type so callers can
  differentiate a constraint violation from other engine errors?
- Do we validate aggregate root state changes, or only messages?
- Should validation be opt-in (e.g., the user must provide a validator via an
  engine option) or automatic when protovalidate annotations are present?

[protovalidate]: https://github.com/bufbuild/protovalidate
