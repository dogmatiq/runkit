# Dynamic process instance description in log lines

Currently the `process_instance.description` is computed once after
`loadInstance()` and baked into the logger for the rest of the delivery.
Because the handler can mutate the root via `scope.Mutate()`, the description
may be stale by the time later log lines (e.g. `scope.Log()` calls, "unable to
handle") are emitted.

It would be better to compute the description fresh for each log line — i.e.
evaluate `root.ProcessInstanceDescription(scope.ended)` at log time rather than
at logger-construction time.

`slog` doesn't natively support computed/lazy attribute values per-log-call
without a custom handler or a `LogValuer`. One approach: implement
`slog.LogValuer` on a small wrapper that holds a pointer to the root and the
ended flag, so the description is resolved lazily when the log record is
processed.

This is a larger change and should be done separately from the current audit
pass.

---
