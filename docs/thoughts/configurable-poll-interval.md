# Make poll interval configurable on Engine

The 25ms `time.After` in the projection `handle()` loop (and likely similar in aggregate/integration controllers) is hardcoded. Expose a `PollInterval` field on `Engine` with a sensible default when zero, same pattern as `CompactInterval`.

This would let tests run faster and operators tune for their workload.

## Possibly related

- `internal/projection/controller.go` — hardcoded `25 * time.Millisecond` in `handle()`
- `docs/thoughts/configurable-backoff.md` — same pattern for backoff config
