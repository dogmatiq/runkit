package runkit_test

import (
	"context"
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

	t.Run("it blocks until Run() is called, then returns nil", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		e := New(
			WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceProvider(newProvider(t)),

			WithApplication(app),
		)
		x := e.ExecutorFor(app)

		result := make(chan error, 1)
		go func() {
			result <- x.ExecuteCommand(ctx, stubs.CommandA1)
		}()

		select {
		case err := <-result:
			t.Fatalf("ExecuteCommand() returned before Run() was called: %v", err)
		case <-time.After(10 * time.Millisecond):
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- e.Run(ctx)
		}()

		if err := <-result; err != nil {
			t.Fatal(err)
		}

		cancel()

		if err := <-runDone; err != context.Canceled {
			t.Errorf("Run() returned unexpected error: %v", err)
		}
	})
}
