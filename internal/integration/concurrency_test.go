package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestConcurrency_handlerIsInvokedConcurrentlyWithMaximizeConcurrencyPreference
// verifies that a handler with the MaximizeConcurrency preference handles
// commands concurrently.
func TestConcurrency_handlerIsInvokedConcurrentlyWithMaximizeConcurrencyPreference(t *testing.T) {
	// barrier is used to prove concurrency: one invocation sends, the other
	// receives. If the handler is not invoked concurrently, the send blocks
	// forever and the test times out.
	barrier := make(chan struct{})

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "51f5b056-9aec-4479-ab41-0c9965dd73e3")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					)
					c.ConcurrencyPreference(dogma.MaximizeConcurrency)
				},
				HandleCommandFunc: func(
					ctx context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case barrier <- struct{}{}:
						return nil
					case <-barrier:
						return nil
					}
				},
			},
		),
	)
}

// TestConcurrency_handlerIsNotInvokedConcurrentlyWithMinimizeConcurrencyPreference
// verifies that a handler with the MinimizeConcurrency preference handles
// commands serially.
func TestConcurrency_handlerIsNotInvokedConcurrentlyWithMinimizeConcurrencyPreference(t *testing.T) {
	var concurrent atomic.Int32

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			for range 10 {
				xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "c420278a-76ee-4d13-b1db-e88e31c25ef5")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					)
					c.ConcurrencyPreference(dogma.MinimizeConcurrency)
				},
				HandleCommandFunc: func(
					context.Context,
					dogma.IntegrationCommandScope,
					dogma.Command,
				) error {
					n := concurrent.Add(1)
					defer concurrent.Add(-1)

					if n > 1 {
						t.Errorf("handler invoked concurrently: %d simultaneous calls", n)
					}

					// Hold the handler open long enough for a concurrent
					// dispatch to be observable.
					time.Sleep(5 * time.Millisecond)

					return nil
				},
			},
		),
	)
}
