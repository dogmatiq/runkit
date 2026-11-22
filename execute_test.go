package runkit_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

// TestExecuteCommand_duplicateIdempotencyKeyIsIgnored verifies that executing
// a command with the same idempotency key twice results in the handler being
// invoked only once.
func TestExecuteCommand_duplicateIdempotencyKeyIsIgnored(t *testing.T) {
	var handled cosyne.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, db)

			xtesting.ExpectEventCount(t, db, 1)
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

// TestExecuteCommand_differentIdempotencyKeysDoNotInterfere verifies that
// commands with different idempotency keys are handled independently.
func TestExecuteCommand_differentIdempotencyKeysDoNotInterfere(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key-1>"),
			); err != nil {
				t.Fatal(err)
			}

			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key-2>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, db)

			xtesting.ExpectEventCount(t, db, 2)
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

// TestExecuteCommand_eventObserverSeesEventsWithDirectCausation verifies that
// event observers are called with events recorded directly by the command that
// was executed.
func TestExecuteCommand_eventObserverSeesEventsWithDirectCausation(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			var called bool

			if err := cluster.ExecuteCommand(
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

// TestExecuteCommand_eachEventObserverSeesEachEventOnce verifies that each
// event observer sees each event exactly once, even when there are more events
// than one poll of the database returns.
func TestExecuteCommand_eachEventObserverSeesEachEventOnce(t *testing.T) {
	const eventCount = 250

	// observe records that an observer saw the event with the given content. It
	// returns an error if the observer saw that event before.
	observe := func(seen map[string]bool, content string) error {
		if seen[content] {
			return fmt.Errorf("observer saw event %q more than once", content)
		}

		seen[content] = true

		return nil
	}

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			var (
				seenA = map[string]bool{}
				seenB = map[string]bool{}
			)

			// The handler records every event of type A before any event of
			// type B, so the observer of type A sees all of its events before
			// the observer of type B is satisfied.
			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithEventObserver(
					func(
						_ context.Context,
						e *stubs.EventStub[stubs.TypeA],
					) (bool, error) {
						return false, observe(seenA, string(e.Content))
					},
				),
				dogma.WithEventObserver(
					func(
						_ context.Context,
						e *stubs.EventStub[stubs.TypeB],
					) (bool, error) {
						err := observe(seenB, string(e.Content))
						return len(seenB) == eventCount, err
					},
				),
			); err != nil {
				t.Fatal(err)
			}

			if len(seenA) != eventCount {
				t.Fatalf("observer of type A saw %d of %d events", len(seenA), eventCount)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "5c0e2f7a-8d1b-4b6e-9f3a-2e7c4d9b1a60")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					for i := range eventCount {
						s.RecordEvent(
							&stubs.EventStub[stubs.TypeA]{
								Content: stubs.TypeA(fmt.Sprint(i)),
							},
						)
					}

					for i := range eventCount {
						s.RecordEvent(
							&stubs.EventStub[stubs.TypeB]{
								Content: stubs.TypeB(fmt.Sprint(i)),
							},
						)
					}

					return nil
				},
			},
		),
	)
}

// TestExecuteCommand_eventObserverSeesEventsWithIndirectCausation verifies
// that event observers are called with events recorded indirectly, that is, not
// directly by the command that was executed, but rather after a chain of
// causation that starts with the command.
func TestExecuteCommand_eventObserverSeesEventsWithIndirectCausation(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			err := cluster.ExecuteCommand(
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

// TestExecuteCommand_returnsTheDeadlineErrorWhenNoEventObserverIsSatisfied
// verifies that ExecuteCommand returns context.DeadlineExceeded when the
// context's deadline passes before an observer returns true.
func TestExecuteCommand_returnsTheDeadlineErrorWhenNoEventObserverIsSatisfied(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			ctx, expire := xtesting.ContextWithManualDeadline(t.Context())

			err := cluster.ExecuteCommand(
				ctx,
				&stubs.CommandStub[stubs.TypeA]{Content: "unsatisfied"},
				dogma.WithEventObserver(
					func(context.Context, *stubs.EventStub[stubs.TypeA]) (bool, error) {
						// The event has been observed without satisfying the
						// observer; stop waiting as though the deadline had
						// passed.
						expire()
						return false, nil
					},
				),
			)

			if err != context.DeadlineExceeded {
				t.Fatalf("got %v, want %v", err, context.DeadlineExceeded)
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

// TestExecuteCommand_returnsTheDeadlineErrorWhenNoEventsAreRecorded verifies
// that ExecuteCommand returns context.DeadlineExceeded when the context's
// deadline passes and the handler has recorded no events.
func TestExecuteCommand_returnsTheDeadlineErrorWhenNoEventsAreRecorded(t *testing.T) {
	var expire func()

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			var ctx context.Context
			ctx, expire = xtesting.ContextWithManualDeadline(t.Context())

			err := cluster.ExecuteCommand(
				ctx,
				&stubs.CommandStub[stubs.TypeA]{Content: "no-events"},
				dogma.WithEventObserver(
					func(context.Context, *stubs.EventStub[stubs.TypeA]) (bool, error) {
						t.Fatal("unexpected call")
						return true, nil
					},
				),
			)

			if err != context.DeadlineExceeded {
				t.Fatalf("got %v, want %v", err, context.DeadlineExceeded)
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
					context.Context,
					dogma.IntegrationCommandScope,
					dogma.Command,
				) error {
					// The command has been handled without recording any
					// events; stop waiting as though the deadline had passed.
					expire()
					return nil
				},
			},
		),
	)
}

// TestExecuteCommand_eventObserverIsInvokedWhenCommandIsDeduplicated verifies
// that event observers are invoked with the original events when a command is
// deduplicated via its idempotency key.
func TestExecuteCommand_eventObserverIsInvokedWhenCommandIsDeduplicated(t *testing.T) {
	var handled cosyne.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			if err := cluster.ExecuteCommand(
				t.Context(),
				stubs.CommandA1,
				dogma.WithIdempotencyKey("<idempotency-key>"),
			); err != nil {
				t.Fatal(err)
			}

			xtesting.WaitForEmptyCommandQueue(t, db)

			// Second call is deduplicated, but observer sees the original events.
			var called bool
			if err := cluster.ExecuteCommand(
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

// TestExecuteCommand_returnsAnErrorWhenTheEngineStopsWhileWaiting verifies
// that ExecuteCommand stops waiting for event observers when the engine stops,
// rather than waiting until the context's deadline.
func TestExecuteCommand_returnsAnErrorWhenTheEngineStopsWhileWaiting(t *testing.T) {
	var handled cosyne.Latch

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
						HandleCommandFunc: func(
							context.Context,
							dogma.IntegrationCommandScope,
							dogma.Command,
						) error {
							handled.Set()
							return nil
						},
					},
				),
			)
		},
	}

	engine := runkit.New(
		app,
		xtesting.NewDatabase(t),
		runkit.WithLogger(spruce.NewTestLogger(t)),
	)

	engineCtx, stopEngine := context.WithCancel(t.Context())
	defer stopEngine()

	stopped := make(chan error, 1)
	go func() {
		stopped <- engine.Run(engineCtx)
	}()

	// The deadline is later than the test waits, so the call returns
	// context.DeadlineExceeded only if it waits for the deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 2*xtesting.WaitTimeout)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- engine.ExecuteCommand(
			ctx,
			stubs.CommandA1,
			dogma.WithEventObserver(
				func(context.Context, *stubs.EventStub[stubs.TypeA]) (bool, error) {
					return false, nil
				},
			),
		)
	}()

	// The handler records no events, so once the command is handled, the call
	// waits for events that are never recorded.
	select {
	case <-handled.Chan():
	case err := <-result:
		t.Fatalf("ExecuteCommand returned before the command was handled: %v", err)
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("the command was not handled")
	}

	stopEngine()

	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error from Run: %v", err)
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected an error")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want an error that says the engine stopped", err)
		}

	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("ExecuteCommand did not return after the engine stopped")
	}
}
