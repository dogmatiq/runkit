package xerrors_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
)

func TestRecover(t *testing.T) {
	t.Run("it returns nil on success", func(t *testing.T) {
		err := xerrors.Recover(func() {})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it returns a PanicError when user code panics with a string", func(t *testing.T) {
		err := xerrors.Recover(func() {
			panicWithString()
		})

		panicErr, ok := errors.AsType[xerrors.PanicError](err)
		if !ok {
			t.Fatalf("expected PanicError, got %T: %v", err, err)
		}

		if panicErr.Value != "<panic>" {
			t.Fatalf("unexpected panic value: got %v, want %q", panicErr.Value, "<panic>")
		}

		if !strings.Contains(panicErr.StackTrace, "panicWithString") {
			t.Fatalf("expected stack trace to contain panicWithString, got:\n%s", panicErr.StackTrace)
		}
	})

	t.Run("it returns a PanicError that unwraps when user code panics with an error", func(t *testing.T) {
		cause := errors.New("<error>")
		err := xerrors.Recover(func() {
			panicWithError(cause)
		})

		if _, ok := errors.AsType[xerrors.PanicError](err); !ok {
			t.Fatalf("expected PanicError, got %T: %v", err, err)
		}

		if !errors.Is(err, cause) {
			t.Fatalf("expected error to unwrap to cause, got: %v", err)
		}
	})

	t.Run("it excludes engine frames from the stack trace", func(t *testing.T) {
		err := xerrors.Recover(func() {
			panicWithString()
		})

		panicErr, ok := errors.AsType[xerrors.PanicError](err)
		if !ok {
			t.Fatalf("expected PanicError, got %T: %v", err, err)
		}

		if !strings.Contains(panicErr.StackTrace, "panicWithString") {
			t.Fatalf("expected stack trace to contain panicWithString, got:\n%s", panicErr.StackTrace)
		}

		// The stack should not contain Recover itself.
		if strings.Contains(panicErr.StackTrace, "xerrors.Recover") {
			t.Fatalf("expected stack trace to exclude xerrors.Recover, got:\n%s", panicErr.StackTrace)
		}
	})

	t.Run("it propagates panics that occur directly in the closure", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic to propagate")
			} else if r != "<panic>" {
				t.Fatalf("unexpected panic value: got %v, want %q", r, "<panic>")
			}
		}()

		xerrors.Recover(func() {
			panic("<panic>")
		})
	})
}

func panicWithString() {
	panic("<panic>")
}

func panicWithError(err error) {
	panic(err)
}
