package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"

	. "github.com/dogmatiq/reference-engine"
)

func TestExecuteCommand(t *testing.T) {
	db, dsn := database.NewTestDB(t)

	handlerIdentity := identitypb.MustParse("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")

	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<app>", "54d6569d-740c-4454-8d0d-d1bc03ae1b6c")
			c.Routes(
				dogma.ViaAggregate(
					&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
						ConfigureFunc: func(c dogma.AggregateConfigurer) {
							c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						RouteCommandToInstanceFunc: func(cmd dogma.Command) string {
							switch c := cmd.(type) {
							case *stubs.CommandStub[stubs.TypeA]:
								return string(c.Content)
							case *stubs.CommandStub[stubs.TypeB]:
								return string(c.Content)
							default:
								panic(dogma.UnexpectedMessage)
							}
						},
						HandleCommandFunc: func(
							r *stubs.AggregateRootStub,
							s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
							cmd dogma.Command,
						) {
							switch cmd.(type) {
							case *stubs.CommandStub[stubs.TypeA]:
								s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
									Content: "event",
								})
							case *stubs.CommandStub[stubs.TypeB]:
								// no-op: produces no events
							}
						},
					},
				),
			)
		},
	}

	engine := &Engine{
		App: app,
		DSN: dsn,
	}

	go func() {
		if err := engine.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	t.Run("it handles a command", func(t *testing.T) {
		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "no-options",
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		xtesting.WaitQuery(
			t,
			db,
			"command queue empty",
			`SELECT COUNT(*) = 0
			FROM command_queue`,
		)

		xtesting.ExpectEvents(
			t,
			eventstream.ReadByAggregateInstance(
				t.Context(),
				db,
				0,
				handlerIdentity.GetKey(),
				"no-options",
			),
			&stubs.EventStub[stubs.TypeA]{
				Content: "event",
			},
		)
	})

	t.Run("it invokes event observers", func(t *testing.T) {
		var called bool

		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "observer-satisfied",
			},
			dogma.WithEventObserver(
				func(_ context.Context, e *stubs.EventStub[stubs.TypeA]) (bool, error) {
					called = true
					return e.Content == "event", nil
				},
			),
		)
		if err != nil {
			t.Fatal(err)
		}

		if !called {
			t.Fatal("observer was not called")
		}
	})

	t.Run("it returns dogma.ErrEventObserverNotSatisfied if none of the event observers are satisfied", func(t *testing.T) {
		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "observer-non-satisfied",
			},
			dogma.WithEventObserver(
				func(_ context.Context, e *stubs.EventStub[stubs.TypeA]) (bool, error) {
					return false, nil
				},
			),
		)

		if !errors.Is(err, dogma.ErrEventObserverNotSatisfied) {
			t.Fatalf("got %v, want ErrEventObserverNotSatisfied", err)
		}
	})

	t.Run("it returns dogma.ErrEventObserverNotSatisfied when there are no events to observe", func(t *testing.T) {
		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeB]{
				Content: "observer-not-satisfied",
			},
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
	})

	t.Run("it deduplicates commands with the same idempotency key", func(t *testing.T) {
		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "idempotent",
			},
			dogma.WithIdempotencyKey("dedup-key"),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for the first command to be handled.
		xtesting.WaitQuery(
			t,
			db,
			"command queue empty",
			`SELECT COUNT(*) = 0
			FROM command_queue`,
		)

		// Send the same key again; it should succeed without producing another
		// event.
		err = engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "idempotent",
			},
			dogma.WithIdempotencyKey("dedup-key"),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Ensure the queue is still empty.
		xtesting.WaitQuery(
			t,
			db,
			"command queue empty",
			`SELECT COUNT(*) = 0
			FROM command_queue`,
		)

		// Verify only one event was produced for this instance.
		xtesting.ExpectEvents(
			t,
			eventstream.ReadByAggregateInstance(
				t.Context(),
				db,
				0,
				handlerIdentity.GetKey(),
				"idempotent",
			),
			&stubs.EventStub[stubs.TypeA]{
				Content: "event",
			},
		)
	})

	t.Run("it returns ErrEventObserverNotSatisfied when a command is deduplicated", func(t *testing.T) {
		err := engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "dedup-observer",
			},
			dogma.WithIdempotencyKey("dedup-observer-key"),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for the first command to be handled.
		xtesting.WaitQuery(
			t,
			db,
			"command queue empty",
			`SELECT COUNT(*) = 0
			FROM command_queue`,
		)

		// Second call: deduplicated, observer can never be satisfied.
		err = engine.ExecuteCommand(
			t.Context(),
			&stubs.CommandStub[stubs.TypeA]{
				Content: "dedup-observer",
			},
			dogma.WithIdempotencyKey("dedup-observer-key"),
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
	})
}
