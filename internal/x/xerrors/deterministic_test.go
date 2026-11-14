package xerrors_test

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/dogmatiq/runkit/internal/x/xerrors"
)

func TestDeterministic(t *testing.T) {
	t.Run("it returns a deterministic error that wraps the given error", func(t *testing.T) {
		cause := errors.New("<cause>")
		err := Deterministic(cause)

		if err.Error() != "<cause>" {
			t.Fatalf("unexpected error: got %v, want %q", err, "<cause>")
		}

		if !errors.Is(err, cause) {
			t.Fatal("expected the error to wrap the cause")
		}

		if !IsDeterministic(err) {
			t.Fatal("expected the error to be deterministic")
		}
	})

	t.Run("it returns a deterministic error unchanged", func(t *testing.T) {
		want := fmt.Errorf("<context>: %w", Deterministic(errors.New("<cause>")))

		if err := Deterministic(want); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
	})

	t.Run("it returns nil if the error is nil", func(t *testing.T) {
		if err := Deterministic(nil); err != nil {
			t.Fatalf("unexpected error: got %v, want nil", err)
		}
	})
}

func TestIsDeterministic(t *testing.T) {
	cases := []struct {
		Name string
		Err  error
		Want bool
	}{
		{
			Name: "an error created by Deterministic",
			Err:  Deterministic(errors.New("<error>")),
			Want: true,
		},
		{
			Name: "an error that wraps one created by Deterministic",
			Err:  fmt.Errorf("<context>: %w", Deterministic(errors.New("<error>"))),
			Want: true,
		},
		{
			Name: "a panic",
			Err: PanicError{
				Value: "<panic>",
			},
			Want: true,
		},
		{
			Name: "an error that wraps a panic",
			Err: fmt.Errorf(
				"<context>: %w",
				PanicError{
					Value: "<panic>",
				},
			),
			Want: true,
		},
		{
			Name: "any other error",
			Err:  errors.New("<error>"),
			Want: false,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := IsDeterministic(c.Err); got != c.Want {
				t.Fatalf("unexpected result: got %t, want %t", got, c.Want)
			}
		})
	}
}
