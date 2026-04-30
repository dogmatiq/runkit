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

func TestNew(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "4272e43c-08f5-4eff-b804-733a795468c3")
		},
	}

	t.Run("it panics if the application is nil", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		New(nil)
	})

	t.Run("it returns an error if no site identity is configured", func(t *testing.T) {
		_, err := New(
			app,
			WithoutEnvironment(),
			WithPersistenceDriver(newProvider(t)),
		)
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error if no persistence driver is configured", func(t *testing.T) {
		_, err := New(
			app,
			WithoutEnvironment(),
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
		)
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it uses the advertise address as the listen address when no listen address is configured", func(t *testing.T) {
		_, err := New(
			app,
			WithoutEnvironment(),
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
			WithAdvertiseAddress("10.0.0.1:7831"),
		)
		if err != nil {
			t.Fatal(err)
		}

		t.Skip("construction succeeds but there is no way to inspect the resolved listen address yet")
	})
}

func TestRun(t *testing.T) {
	t.Run("it starts and stops cleanly", func(t *testing.T) {
		e, err := New(
			&stubs.ApplicationStub{
				ConfigureFunc: func(c dogma.ApplicationConfigurer) {
					c.Identity("app", "91b5f738-14e1-4a92-b740-2e2e8b88c835")
				},
			},
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
			WithListenAddress("127.0.0.1:0"),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())

		done := make(chan error, 1)
		go func() {
			done <- e.Run(ctx)
		}()

		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() returned unexpected error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run() did not return after context cancellation")
		}
	})
}
