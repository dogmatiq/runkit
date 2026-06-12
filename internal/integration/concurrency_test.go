package integration_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestHandlersAreInvokedConcurrentlyWhenConcurrencyPreferenceIsMaximize(t *testing.T) {
	// barrier is used to prove concurrency: one handler sends, the other
	// receives. If both handlers are not running concurrently, the send blocks
	// forever and the test times out.
	barrier := make(chan struct{})

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
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

func TestHandlersAreNotInvokedConcurrentlyWhenConcurrencyPreferenceIsMinimize(t *testing.T) {
}
