package runkit_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestEngine(t *testing.T) {
	t.Run("it returns an error if it can not listen for gRPC requests", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		// Occupy a port so that the engine can not listen on it.
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()

		app := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app>", "a2b5f0ae-4b1a-4c6e-9d26-0c5a2e5a6b3d")
				c.Routes(
					dogma.ViaIntegration(
						&stubs.IntegrationMessageHandlerStub{
							ConfigureFunc: func(c dogma.IntegrationConfigurer) {
								c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
								c.Routes(
									dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
									dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
								)
							},
						},
					),
				)
			},
		}

		engine := runkit.New(
			app,
			db,
			runkit.WithListenAddress(occupied.Addr().String()),
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err = engine.Run(ctx)
		if err == nil || errors.Is(err, ctx.Err()) {
			t.Fatalf("expected a listen error, got %v", err)
		}

		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != "listen" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
