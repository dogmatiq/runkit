# Scopes should panic on unrouted outbound messages

Scope implementations (aggregate, integration, projection) should panic if a
handler attempts to produce a message whose type is not declared in the handler's
outbound routes.

This is a programming error in the handler — it means the handler is producing
messages it didn't declare. Panicking makes this immediately visible during
development/testing rather than silently allowing undeclared messages to propagate.

## Possibly related

- [internal/aggregate/scope.go](../../internal/aggregate/scope.go)
- [internal/integration/scope.go](../../internal/integration/scope.go)
- [internal/projection/scope.go](../../internal/projection/scope.go)
