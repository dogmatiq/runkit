package dogmaengine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestCommandWithDuplicateIdempotencyKeyIsIgnored(t *testing.T) {
	var handled xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)

			xtesting.ExpectEventCount(t, engine.DB, 1)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					if handled.IsSet() {
						t.Error("unexpected second call to handler")
					} else {
						handled.Set()
						s.RecordEvent(stubs.EventA1)
					}
					return nil
				},
			},
		),
	)
}

func TestDifferentIdempotencyKeysDoNotInterfereWithEachOther(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key-1>"),
			); err != nil {
				t.Fatal(err)
			}

			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key-2>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)

			xtesting.ExpectEventCount(t, engine.DB, 2)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					s.RecordEvent(stubs.EventA1)
					return nil
				},
			},
		),
	)
}

func TestExecuteCommandInvokesEventObservers(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			var called bool

			if err := engine.ExecuteCommand(
				t.Context(),
				&stubs.CommandStub[stubs.TypeA]{Content: "observe"},
				dogma.WithEventObserver(
					func(
						_ context.Context,
						e *stubs.EventStub[stubs.TypeA],
					) (bool, error) {
						called = true
						return e.Content == "event", nil
					},
				),
			); err != nil {
				t.Fatal(err)
			}

			if !called {
				t.Fatal("observer was not called")
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event"})
					return nil
				},
			},
		),
	)
}

func TestExecuteCommandReturnsAnErrorWhenNoEventObserversAreSatisfied(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			err := engine.ExecuteCommand(
				t.Context(),
				&stubs.CommandStub[stubs.TypeA]{Content: "unsatisfied"},
				dogma.WithEventObserver(
					func(context.Context, *stubs.EventStub[stubs.TypeA]) (bool, error) {
						return false, nil
					},
				),
			)

			if !errors.Is(err, dogma.ErrEventObserverNotSatisfied) {
				t.Fatalf("got %v, want ErrEventObserverNotSatisfied", err)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event"})
					return nil
				},
			},
		),
	)
}

func TestExecuteCommandReturnsAnErrorWhenNoEventsAreRecorded(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			err := engine.ExecuteCommand(
				t.Context(),
				&stubs.CommandStub[stubs.TypeA]{Content: "no-events"},
				dogma.WithEventObserver(
					func(context.Context, *stubs.EventStub[stubs.TypeA]) (bool, error) {
						t.Fatal("unexpected call")
						return true, nil
					},
				),
			)

			if !errors.Is(err, dogma.ErrEventObserverNotSatisfied) {
				t.Fatalf("got %v, want ErrEventObserverNotSatisfied", err)
			}
		},
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
}

func TestExecuteCommandInvokesObserversWhenCommandIsDeduplicated(t *testing.T) {
	var handled xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)

			// Second call is deduplicated, but observer sees the original events.
			var called bool
			if err := engine.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
				dogma.WithEventObserver(
					func(
						_ context.Context,
						e *stubs.EventStub[stubs.TypeA],
					) (bool, error) {
						called = true
						return e.Content == "event", nil
					},
				),
			); err != nil {
				t.Fatal(err)
			}

			if !called {
				t.Fatal("observer was not called")
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					if handled.IsSet() {
						t.Error("unexpected second call to handler")
					} else {
						handled.Set()
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event"})
					}
					return nil
				},
			},
		),
	)
}
