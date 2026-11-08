package supervisor_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/spruce"
)

func TestFatalf(t *testing.T) {
	cause := errors.New("<cause>")

	err := Run(
		t.Context(),
		spruce.NewTestLogger(t),
		"<work>",
		func(context.Context) error {
			return Fatalf("<error %d>: %w", 1, cause)
		},
	)

	if err == nil || err.Error() != "<error 1>: <cause>" {
		t.Fatalf("unexpected error: got %v, want %q", err, "<error 1>: <cause>")
	}

	if !errors.Is(err, cause) {
		t.Fatal("expected the error to wrap the cause")
	}
}

func TestDeterministicf(t *testing.T) {
	cause := errors.New("<cause>")
	err := Deterministicf("<error %d>: %w", 1, cause)

	if err.Error() != "<error 1>: <cause>" {
		t.Fatalf("unexpected error: got %v, want %q", err, "<error 1>: <cause>")
	}

	if !errors.Is(err, cause) {
		t.Fatal("expected the error to wrap the cause")
	}
}
