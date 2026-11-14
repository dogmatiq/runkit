package xerrors_test

import (
	"errors"
	"fmt"
	"testing"

	. "github.com/dogmatiq/runkit/internal/x/xerrors"
)

func TestFatal(t *testing.T) {
	t.Run("it returns a fatal error that wraps the given error", func(t *testing.T) {
		cause := errors.New("<cause>")
		err := Fatal(cause)

		if err.Error() != "<cause>" {
			t.Fatalf("unexpected error: got %v, want %q", err, "<cause>")
		}

		if !errors.Is(err, cause) {
			t.Fatal("expected the error to wrap the cause")
		}

		if !IsFatal(err) {
			t.Fatal("expected the error to be fatal")
		}
	})

	t.Run("it returns a fatal error unchanged", func(t *testing.T) {
		want := fmt.Errorf("<context>: %w", Fatal(errors.New("<cause>")))

		if err := Fatal(want); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}
	})

	t.Run("it returns nil if the error is nil", func(t *testing.T) {
		if err := Fatal(nil); err != nil {
			t.Fatalf("unexpected error: got %v, want nil", err)
		}
	})
}

func TestIsFatal(t *testing.T) {
	cases := []struct {
		Name string
		Err  error
		Want bool
	}{
		{
			Name: "an error created by Fatal",
			Err:  Fatal(errors.New("<error>")),
			Want: true,
		},
		{
			Name: "an error that wraps one created by Fatal",
			Err:  fmt.Errorf("<context>: %w", Fatal(errors.New("<error>"))),
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
			if got := IsFatal(c.Err); got != c.Want {
				t.Fatalf("unexpected result: got %t, want %t", got, c.Want)
			}
		})
	}
}
