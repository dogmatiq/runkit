# Event consumption test ownership

The shared consumer owns cluster assignment, stream lifecycle, the catalog read
loop, and one supervisor per stream. `internal/eventstream/consumer_test.go`
contains the lifecycle cases moved from projections: stream appearance,
responsibility becoming available, removal/reappearance, and source handoff.
The internal runner tests cover checkpoint reload after partial success,
filtered progress, fatal errors, and cancellation without requiring PostgreSQL.

The catalog and source suites continue to own filtering, cached/historical
reads, local watching, remote transport, and source retry behavior. The
extraction does not add another local/remote test matrix.

Projection tests retain checkpoint loading, skipped-event checkpoint behavior,
OCC, dispatch/routing, decoding failures, and concurrency preferences. The
existing distribution tests also exercise the interaction of shared assignment
with projection OCC during node joins and failover; retain these integration
checks rather than replacing their projection-specific assertions with a Sink
stub. The foreign-event tests retain engine wiring and the one-remote-watch
assertion, rather than duplicating transport tests in the consumer suite.

Process consumption is unchanged. Future process tests should focus on its
transactional effects and wiring instead of repeating shared lifecycle and
transport coverage.
