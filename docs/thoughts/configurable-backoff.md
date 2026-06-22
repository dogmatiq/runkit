# Make backoff base/cap configurable on Engine

Same pattern as `CompactInterval`: expose `BackoffBase` and `BackoffCap` fields on the `Engine` struct with defaults when zero. Currently they are hardcoded consts in `newControllerForHandler`.

This would let tests (and eventually operators) tune retry behavior without code changes.

## Possibly related

- `engine.go` — `newControllerForHandler` where `backoffBase`/`backoffCap` are defined as consts
- `internal/x/xtesting/engine.go` — `RunEngines` currently has no way to set these
