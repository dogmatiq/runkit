# runkit — Engine as Platform (Discussion Notes)

This document captures open questions about a potential future direction: hosting Dogma
application handlers via a language-agnostic gRPC protocol, making runkit a platform rather
than a library. This is explicitly out of scope for the current implementation phases, but
one decision (Open Question 6) must be made before Phase 3 implementation begins.

## The Idea

Today, a Dogma application is a Go binary that embeds runkit and calls `engine.New(...)`.
The "engine as platform" direction would allow application handlers to be hosted as separate
processes — connected to a runkit engine over a bidirectional gRPC stream, similar to how
Terraform provider plugins connect to the Terraform core binary.

The analogy:

| Terraform         | runkit (platform)                   |
| ----------------- | ----------------------------------- |
| Core binary       | runkit engine                       |
| Provider plugin   | Dogma application (any language)    |
| Provider protocol | Dogma handler protocol (gRPC/proto) |

The Go `dogma.Application` path would remain supported. The gRPC path would be an additional
input form. runkit becomes a server binary that can host handlers written in Go, Python,
TypeScript, Java, or anything that can speak gRPC.

The interaction model is bidirectional streaming: the engine and handler communicate over a
long-lived bidi stream with the handler acting as the server (engine connects to handler, or
handler connects to engine — see OQ2).

---

## Open Questions

### OQ1 — Who owns aggregate state?

Two models are possible:

**Option A — Engine-managed state**
The engine maintains the aggregate's persisted event stream. On each command, it replays
events, sends `(current_state, command)` to the handler, and receives `(new_state, events)`
back. The handler is stateless between calls.

**Option B — Handler-managed state**
The handler maintains aggregate instances in memory (as in normal Go dogma today). The engine
sends only the command; the handler replies with the events. The engine trusts the handler to
have called `HandleCommand` correctly.

Option A is safer (engine controls the source of truth, handler can be restarted freely) but
adds round-trip latency and requires a state serialisation protocol. Option B is simpler but
requires the engine to trust the handler's in-memory state, which breaks on handler restart.

Questions to resolve:

- Is Option A necessary for correctness guarantees, or can we require handler stickiness?
- If Option A, what is the state representation? Opaque bytes? Structured proto?
- If Option B, how does the engine handle handler restart mid-aggregate-lifetime?

---

### OQ2 — Connection model

Who initiates the stream, and what is the scope of a single stream?

**Initiation:**

- _Handler connects to engine (pull model)_: handler binary starts, dials the engine's gRPC
  address, and announces itself. Engine assigns work to the connected handler.
- _Engine connects to handler (push model)_: engine is given the handler's address at
  registration time and connects to it. Engine has full control over lifecycle.

The pull model is friendlier for containerised deployments (handler doesn't need a stable
address). The push model gives the engine more control (it can choose when to connect and
disconnect).

**Stream scope:**

- Per application? (one stream per dogma.Application equivalent)
- Per handler type? (one stream per aggregate root type, etc.)
- Per partition? (stream carries only work for a specific partition)
- Per instance? (one stream per aggregate instance — probably too fine-grained)

Narrower scope = better load distribution. Broader scope = fewer connections, simpler
multiplexing.

**Reconnection:**

- If the handler stream drops, does the engine retry? Wait for the handler to reconnect?
- What happens to in-flight commands during a disconnect?

---

### OQ3 — Application registration and configuration

How does the engine know what handler types a connected handler process serves?

- Does the handler send a config/announce message on connect declaring its handler keys,
  message type routes, and dogma.Application identity?
- How are message types identified over the wire? Options:
  - Protobuf fully-qualified name
  - UUID (stable, language-agnostic, but requires a registry)
  - String name (human-readable, but fragile under rename)
- Handler key in dogma is a UUID — this translates cleanly to the wire.
- What is the equivalent of `dogma.Application.Configure(*dogma.ApplicationConfigurer)` in
  the language-agnostic protocol?

---

### OQ4 — Protocol ownership

Where does the Dogma handler protocol (proto files, client/server stubs) live?

- **In `runkit`**: engine-specific, simpler to iterate — but language SDKs depend on a
  production engine repo.
- **In `enginekit`**: engine-agnostic — appropriate if the goal is a language ecosystem
  around Dogma that could work with any compliant engine.
- **Dedicated repo** (e.g. `dogmatiq/dogma-protocol`): cleanest separation, but another
  repo to maintain.

If the goal is to build a language ecosystem around Dogma (not just runkit), the protocol
belongs in a shared, stable home. If runkit is the only engine that will ever support this,
keeping it in runkit is fine.

---

### OQ5 — Deployment model implications

If runkit becomes a platform:

- The engine needs a listener address (`WithListenerAddress(...)` option or similar).
- `dogma.Application` becomes one of several input forms (Go-native vs. remote handler).
- Distribution format: runkit ships as a server binary (Docker image, Helm chart) rather
  than a library import.
- Versioning: engine and handler SDK must agree on the protocol version. How do we handle
  version skew? Minimum protocol version negotiation on connect?
- The "embed runkit in your binary" model still works, but becomes optional rather than
  the only option.

---

### OQ6 — Impact on current phases _(decision needed before Phase 3)_

**This is the only question with a near-term implication.**

The current Phase 3 plan (aggregate subsystem) will need to invoke
`dogma.AggregateMessageHandler`. The decision is:

**Option A — Direct call**
Phase 3 calls `handler.HandleCommand(...)` directly. Simple, no abstraction overhead.
Later, if remote handlers are added, everything that calls `HandleCommand` must be refactored
to go through an interface.

**Option B — Location-transparent interface**
Phase 3 introduces a thin `AggregateExecutor` interface (or similar). Today's only
implementation calls `dogma.AggregateMessageHandler` directly (in-process). A future
implementation speaks gRPC. The interface is defined in Phase 3; the gRPC implementation
is deferred.

Option B is a small upfront cost with large future flexibility. Option A is simpler now but
forecloses the platform path without an expensive refactor.

_Recommendation:_ Choose Option B. The interface will be thin (one or two methods) and the
in-process implementation is trivial. The cost is low; the benefit is keeping the platform
path open.
