package runkit_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/persistence/driver/memory"
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

func TestRun(t *testing.T) {
	t.Run("it panics if no site identity is configured", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		e := New()
		e.Run(t.Context())
	})

	t.Run("it panics if no persistence provider is configured", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		e := New(
			WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
		)
		e.Run(t.Context())
	})

	t.Run("it panics if an advertise address is configured without a listen address", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		e := New(
			WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistence(memory.Driver),
			WithAdvertiseAddress("10.0.0.1:7831"),
		)
		e.Run(t.Context())
	})

	t.Run("it starts and stops cleanly", func(t *testing.T) {
		app := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("app", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
			},
		}

		e := New(
			WithSite("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistence(memory.Driver),
			WithListenAddress("127.0.0.1:0"),
			WithApplication(app),
		)

		ctx, cancel := context.WithCancel(t.Context())

		done := make(chan error, 1)
		go func() {
			done <- e.Run(ctx)
		}()

		cancel()

		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("Run() returned unexpected error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run() did not return after context cancellation")
		}

		cancel()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
	})
}
