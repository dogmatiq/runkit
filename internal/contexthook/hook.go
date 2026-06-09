package contexthook

import (
	"context"
	"testing"
)

// With returns a context derived from ctx that includes fn as a hook for values
// of type T.
//
// fn is called when [Invoke] is called on the resulting context with a value of
// type T. If multiple hooks are added for the same type, they will be called in
// the order they were added.
func With[T any](
	ctx context.Context,
	fn func(T),
) context.Context {
	if prev, ok := get[T](ctx); ok {
		next := fn
		fn = func(v T) {
			prev(v)
			next(v)
		}
	}

	return context.WithValue(
		ctx,
		contextKey[T]{},
		fn,
	)
}

// Invoke calls any hooks in ctx that accept values of type T.
func Invoke[T any](ctx context.Context, v T) {
	if testing.Testing() {
		if hook, ok := get[T](ctx); ok {
			hook(v)
		}
	}
}

// contextKey is the type of the key used to store hooks in contexts.
//
// T is the type of the value that the hook accepts.
type contextKey[T any] struct{}

// get returns the hook function for values of type T in ctx, if one exists.
func get[T any](ctx context.Context) (func(T), bool) {
	hook, ok := ctx.Value(contextKey[T]{}).(func(T))
	return hook, ok
}
