package runkit_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
)

func TestExecutorFor(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
		},
	}
	e := New(WithApplication(app))

	t.Run("it returns an executor for a registered application", func(t *testing.T) {
		x := e.ExecutorFor(app)
		if x == nil {
			t.Fatal("expected non-nil CommandExecutor")
		}
	})

	t.Run("it panics for an unregistered application", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		unregistered := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("other", "a4b8c2d6-e0f1-4a3b-b7c8-d9e0f1a2b3c4")
			},
		}
		e.ExecutorFor(unregistered)
	})
}

func TestExecuteCommand(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
		},
	}

	t.Run("it blocks until Run() is called, then returns nil", func(t *testing.T) {
		e := New(WithApplication(app))
		x := e.ExecutorFor(app)

		result := make(chan error, 1)
		go func() {
			result <- x.ExecuteCommand(t.Context(), stubs.CommandA1)
		}()

		select {
		case err := <-result:
			t.Fatalf("ExecuteCommand() returned before Run() was called: %v", err)
		case <-time.After(10 * time.Millisecond):
		}

		if err := e.Run(t.Context()); err != nil {
			t.Error(err)
		}

		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
}
