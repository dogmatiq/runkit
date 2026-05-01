package runkit_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
)

func TestCommandExecutor_ExecuteCommand(t *testing.T) {
	t.Run("it blocks until Run() is called", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			e, err := New(
				&stubs.ApplicationStub{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("app", "c563d2a7-1e4b-4f39-8d72-5a9f0b3e6c18")
					},
				},
				WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
				WithPersistenceDriver(newProvider(t)),
			)
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan struct{})
			go func() {
				defer func() {
					recover()
					close(done)
				}()
				if err := e.ExecuteCommand(ctx, stubs.CommandA1); err != nil {
					t.Log(err)
				}
			}()

			// Wait for ExecuteCommand to block on the ready latch.
			synctest.Wait()

			select {
			case <-done:
				t.Fatal("ExecuteCommand() returned before Run() was called")
			default:
			}

			go e.Run(ctx)

			// Wait for Run to set the ready latch and ExecuteCommand to proceed.
			synctest.Wait()

			select {
			case <-done:
				// ExecuteCommand unblocked after Run was called
			default:
				t.Fatal("ExecuteCommand() did not return after Run() was called")
			}
		})
	})

	t.Run("it panics for unrouted commands", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			e, err := New(
				&stubs.ApplicationStub{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("app", "c563d2a7-1e4b-4f39-8d72-5a9f0b3e6c18")
					},
				},
				WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
				WithPersistenceDriver(newProvider(t)),
			)
			if err != nil {
				t.Fatal(err)
			}

			go e.Run(ctx)

			defer func() {
				if recover() == nil {
					t.Fatal("ExecuteCommand() did not panic for unrouted command")
				}
			}()

			if err := e.ExecuteCommand(ctx, stubs.CommandA1); err != nil {
				t.Log(err)
			}
		})
	})
}
