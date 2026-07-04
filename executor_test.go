package dogmaengine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestCommandExecutor_duplicateIdempotencyKeyIsIgnored verifies that executing
// a command with the same idempotency key twice results in the handler being
// invoked only once.
func TestCommandExecutor_duplicateIdempotencyKeyIsIgnored(t *testing.T) {
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

// TestCommandExecutor_differentIdempotencyKeysDoNotInterfere verifies that
// commands with different idempotency keys are handled independently.
func TestCommandExecutor_differentIdempotencyKeysDoNotInterfere(t *testing.T) {
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

// TestCommandExecutor_eventObserverSeesEventsWithDirectCausation verifies that
// event observers are called with events recorded directly by the command that
// was executed.
func TestCommandExecutor_eventObserverSeesEventsWithDirectCausation(t *testing.T) {
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

// TestCommandExecutor_eventObserverSeesEventsWithIndirectCausation verifies
// that event observers are called with events recorded indirectly, that is, not
// directly by the command that was executed, but rather after a chain of
// causation that starts with the command.
func TestCommandExecutor_eventObserverSeesEventsWithIndirectCausation(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			err := engine.ExecuteCommand(
				t.Context(),
				&stubs.CommandStub[stubs.TypeA]{Content: "start"},
				dogma.WithEventObserver(
					func(_ context.Context, e *stubs.EventStub[stubs.TypeB]) (bool, error) {
						return e.Content == "from-deadline", nil
					},
				),
			)
			if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<trigger>", "8e6a3127-3a6f-4e58-9c34-7e1fb70e9c4f")
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
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<scheduler>", "27f4f0a8-9d4c-4f9c-9e34-1a52d4f6c3b7")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
					)
				},
				RouteEventToInstanceFunc: func(
					context.Context,
					dogma.Event,
				) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					_ dogma.Event,
				) error {
					s.ScheduleDeadline(stubs.DeadlineA1, time.Now())
					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					_ dogma.Deadline,
				) error {
					s.ExecuteCommand(&stubs.CommandStub[stubs.TypeB]{Content: "satisfy"})
					return nil
				},
			},
		),
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<satisfier>", "61e1f0c5-5d4a-4b8b-9c91-2f2a6c8a9b1d")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					s.RecordEvent(&stubs.EventStub[stubs.TypeB]{Content: "from-deadline"})
					return nil
				},
			},
		),
	)
}

// TestCommandExecutor_returnsAnErrorWhenNoEventObserverIsSatisfied verifies
// that ExecuteCommand returns ErrEventObserverNotSatisfied when no observer
// returns true.
func TestCommandExecutor_returnsAnErrorWhenNoEventObserverIsSatisfied(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()

			err := engine.ExecuteCommand(
				ctx,
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

// TestCommandExecutor_returnsAnErrorWhenNoEventsAreRecorded verifies that
// ExecuteCommand returns ErrEventObserverNotSatisfied when the handler does not
// record any events.
func TestCommandExecutor_returnsAnErrorWhenNoEventsAreRecorded(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()

			err := engine.ExecuteCommand(
				ctx,
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

// TestCommandExecutor_eventObserverIsInvokedWhenCommandIsDeduplicated verifies
// that event observers are invoked with the original events when a command is
// deduplicated via its idempotency key.
func TestCommandExecutor_eventObserverIsInvokedWhenCommandIsDeduplicated(t *testing.T) {
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
