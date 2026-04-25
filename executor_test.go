package runkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
)

func TestCommandExecutor_ExecuteCommand(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
		},
	}

	t.Run("it blocks until Run() is called, then panics for unrouted commands", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		e := New(
			WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceProvider(newProvider(t)),

			WithApplication(app),
		)
		x := e.ExecutorFor(app)

		panicked := make(chan struct{})
		go func() {
			defer func() {
				if recover() != nil {
					close(panicked)
				}
			}()
			x.ExecuteCommand(ctx, stubs.CommandA1) //nolint:errcheck
		}()

		select {
		case <-panicked:
			t.Fatal("ExecuteCommand() panicked before Run() was called")
		case <-time.After(10 * time.Millisecond):
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- e.Run(ctx)
		}()

		select {
		case <-panicked:
			// expected
		case <-time.After(time.Second):
			t.Fatal("ExecuteCommand() did not panic after Run() was called")
		}

		cancel()

		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Errorf("Run() returned unexpected error: %v", err)
		}
	})
}
