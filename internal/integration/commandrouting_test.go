package integration_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestCommandRouting_commandsAreRoutedToTheCorrectHandler verifies that
// commands are routed to the correct handler based on their message type.
func TestCommandRouting_commandsAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute two commands of different types, each targetting a
			// different handler.
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			xtesting.ExecuteCommand(t, engine, stubs.CommandB1)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler-a>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					m dogma.Command,
				) error {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						// ok
					default:
						t.Errorf("unexpected command type routed to handler-a: %T", m)
					}

					return nil
				},
			},
		),
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler-b>", "3d55fb34-c7b6-4dea-a5de-8da4a006d64d")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					m dogma.Command,
				) error {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeB]:
						// ok
					default:
						t.Errorf("unexpected command type routed to handler-b: %T", m)
					}

					return nil
				},
			},
		),
	)
}
