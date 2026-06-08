package testhook

import (
	"context"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// ExecuteCommand is emitted when a command is executed, before it is
// added to the queue.
type ExecuteCommand struct {
	CommandEnvelope *envelopepb.Envelope
}

// Append adds a hook to the context that will be called with a value of type T
// when [Invoke] is called with a value of that type.
func Append[T any](
	ctx context.Context,
	hook func(T),
) context.Context {
	key := contextKey[T]{}

	if prior, ok := ctx.Value(key).(func(T)); ok {
		next := hook
		hook = func(t T) {
			prior(t)
			next(t)
		}
	}

	return context.WithValue(
		ctx,
		key,
		hook,
	)
}

// Invoke calls any hooks in the context that match the type of v, only if
// testing is active.
func Invoke[T any](ctx context.Context, v T) {
	if testing.Testing() {
		if hook, ok := ctx.Value(contextKey[T]{}).(func(T)); ok {
			hook(v)
		}
	}
}

type contextKey[T any] struct{}
