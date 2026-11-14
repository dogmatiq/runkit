package process_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestInstance_stateIsNotPersistedIfMutateIsNotUsed verifies that the
// process instance state is not persisted if the [dogma.ProcessScope].Mutate()
// method is not called.
func TestInstance_stateIsNotPersistedIfMutateIsNotUsed(t *testing.T) {
	var done cosyne.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1
					default:
						return stubs.EventX1
					}
				},
				2,
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "5ca74d02-8dde-4148-8f35-cec0e4c3b241")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						// Modify the root directly without calling s.Mutate().
						r.Value = "A"
					case *stubs.EventStub[stubs.TypeX]:
						defer done.Set()

						if r.Value != nil {
							t.Errorf("process state was persisted without Mutate(): got %v, want nil", r.Value)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstance_stateIsPersisted verifies that the process instance state
// is correctly persisted across multiple events that target that instance.
func TestInstance_stateIsPersisted(t *testing.T) {
	var done cosyne.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1
					case 1:
						return stubs.EventB1
					default:
						return stubs.EventX1
					}
				},
				3, // one stream, three events
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "dbe022b0-b52c-489a-b855-638e68f6b709")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = "A"
						})
					case *stubs.EventStub[stubs.TypeB]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = root.Value.(string) + "B"
						})
					case *stubs.EventStub[stubs.TypeX]:
						defer done.Set()

						if got, want := r.Value, "AB"; got != want {
							t.Errorf("process state is not persisted between events: got %v, want %v", got, want)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstance_instancesAreIsolated verifies that the state of one
// instance is not affected by events handled by another instance.
func TestInstance_instancesAreIsolated(t *testing.T) {
	var (
		instanceAChecked cosyne.Latch
		instanceBChecked cosyne.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return &stubs.EventStub[stubs.TypeA]{Content: "A"} // handled by instance A
					case 1:
						return &stubs.EventStub[stubs.TypeA]{Content: "B"} // handled by instance B
					case 2:
						return &stubs.EventStub[stubs.TypeX]{Content: "A"} // assert about instance A
					default:
						return &stubs.EventStub[stubs.TypeX]{Content: "B"} // assert about instance B
					}
				},
				4,
			)

			xtesting.WaitForLatches(
				t,
				&instanceAChecked,
				&instanceBChecked,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "2b711639-5838-429d-9884-7c687bb06a75")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					_ context.Context,
					m dogma.Event,
				) (string, bool, error) {
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return "instance:" + string(m.Content), true, nil
					case *stubs.EventStub[stubs.TypeX]:
						return "instance:" + string(m.Content), true, nil
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleEventFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = s.InstanceID()
						})
					case *stubs.EventStub[stubs.TypeX]:
						if s.InstanceID() == "instance:A" {
							defer instanceAChecked.Set()
						} else {
							defer instanceBChecked.Set()
						}

						if got, want := r.Value, s.InstanceID(); got != want {
							t.Errorf("process state is not isolated per instance: got %v, want %v", got, want)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstance_writesAreSerialized verifies that concurrent events and
// deadlines targeting the same process instance are serialized, such that each
// handler invocation observes the cumulative state produced by all prior
// invocations, regardless of whether they originated from the event consumer or the
// deadline sink.
func TestInstance_writesAreSerialized(t *testing.T) {
	const handlerKey = "d5fce239-6c7f-4153-895c-ecb047693319"

	const (
		streamCount     = 4
		eventsPerStream = 5
		totalEvents     = streamCount * eventsPerStream
		totalIncrements = totalEvents * 2 // each event, plus one deadline per event
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			counts := make([]uint64, streamCount)
			for i := range counts {
				counts[i] = eventsPerStream
			}

			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return stubs.EventA1
				},
				counts...,
			)

			// Wait for all events to be consumed before publishing the final
			// event that will assert that the process state is cumulative
			// across all events and deadlines.
			xtesting.WaitForHandlerToConsumeAllEvents(t, db, handlerKey)
			xtesting.WaitForNoPendingDeadlines(t, db)

			// Publish the final event that will assert that the process state
			// is cumulative across all events and deadlines.
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventX1
				},
				1,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					e dogma.Event,
				) error {
					switch e.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							if r.Value == nil {
								r.Value = 1
							} else {
								r.Value = r.Value.(float64) + 1
							}
						})

						// Schedule a deadline for immediate delivery so that the
						// deadline sink and event consumer race on the same instance.
						s.ScheduleDeadline(stubs.DeadlineA1, time.Now())

					case *stubs.EventStub[stubs.TypeX]:
						if got, want := r.Value, float64(totalIncrements); got != want {
							t.Errorf("process state is not cumulative across events and deadlines: got %v, want %v", got, want)
						}

					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					_ dogma.Deadline,
				) error {
					s.Mutate(func(root *stubs.ProcessRootStub) {
						if r.Value == nil {
							r.Value = 1
						} else {
							r.Value = r.Value.(float64) + 1
						}
					})

					return nil
				},
			},
		),
	)
}

// TestInstance_deadlineQueue verifies that each process instance is bound
// to a queue for its deadlines.
func TestInstance_deadlineQueue(t *testing.T) {
	const handlerKey = "8f0f5b9e-27d6-4f3e-9b0a-6c1f7c1c8e44"

	// handler returns a process handler that routes every event to one
	// instance, and schedules a deadline when it handles an event of type A.
	handler := func() dogma.HandlerRoute {
		return dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					m dogma.Event,
				) error {
					if _, ok := m.(*stubs.EventStub[stubs.TypeA]); ok {
						s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))
					}
					return nil
				},
			},
		)
	}

	// insertInstance inserts the instance before the engine starts, bound to
	// the queue with the given ID, or unbound if it is nil.
	insertInstance := func(t *testing.T, db *sql.DB, deadlineQueueID *uuidpb.UUID) {
		t.Helper()

		var queueID any
		if deadlineQueueID != nil {
			queueID = deadlineQueueID.AsString()
		}

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO process.instances (
				handler_key,
				instance_id,
				deadline_queue_id
			) VALUES ($1, '<instance>', $2)`,
			handlerKey,
			queueID,
		); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("it binds a new instance when it first handles an event", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, _ *xtesting.Cluster, db *sql.DB) {
				xtesting.AppendToNewEventStream(t, db, stubs.EventB1)

				xtesting.WaitForQueryResult(
					t,
					"instance is bound",
					1,
					db,
					`SELECT COUNT(*)
					FROM process.instances
					WHERE deadline_queue_id IS NOT NULL`,
				)
			},
			handler(),
		)
	})

	t.Run("it binds an existing instance when it next handles an event", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		insertInstance(t, db, nil)

		xtesting.RunEnginesWithDB(
			t,
			db,
			func(t testing.TB, _ *xtesting.Cluster, db *sql.DB) {
				xtesting.AppendToNewEventStream(t, db, stubs.EventB1)

				xtesting.WaitForQueryResult(
					t,
					"instance is bound",
					1,
					db,
					`SELECT COUNT(*)
					FROM process.instances
					WHERE deadline_queue_id IS NOT NULL`,
				)
			},
			handler(),
		)
	})

	t.Run("it adds a deadline to its instance's queue", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, _ *xtesting.Cluster, db *sql.DB) {
				xtesting.AppendToNewEventStream(t, db, stubs.EventA1)

				xtesting.WaitForQueryResult(
					t,
					"deadline is in its instance's queue",
					1,
					db,
					`SELECT COUNT(*)
					FROM messagequeue.messages AS m
					INNER JOIN process.instances AS i
						ON i.handler_key = m.handler_key
						AND i.instance_id = m.instance_id
					WHERE m.queue_id = i.deadline_queue_id`,
				)
			},
			handler(),
		)
	})

	t.Run("it keeps a binding that was changed by hand", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		queueID := uuidpb.Generate()
		insertInstance(t, db, queueID)

		xtesting.RunEnginesWithDB(
			t,
			db,
			func(t testing.TB, _ *xtesting.Cluster, db *sql.DB) {
				xtesting.AppendToNewEventStream(t, db, stubs.EventA1)

				xtesting.WaitForQueryResult(
					t,
					"deadline is in the queue set by hand",
					1,
					db,
					`SELECT COUNT(*)
					FROM messagequeue.messages
					WHERE queue_id = $1`,
					queueID.AsString(),
				)
			},
			handler(),
		)
	})
}

// TestInstance_end verifies how a process instance is ended.
func TestInstance_end(t *testing.T) {
	t.Run("it ends an instance without waiting for a deadline that another transaction has locked", func(t *testing.T) {
		t.Parallel()

		db := xtesting.NewDatabase(t)
		sink := newEventSink(t, db)
		sink.OutboundMessageTypes = map[reflect.Type]struct{}{
			reflect.TypeFor[*stubs.DeadlineStub[stubs.TypeA]](): {},
		}

		handler := sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
		handler.HandleEventFunc = func(
			_ context.Context,
			_ dogma.ProcessRoot,
			s dogma.ProcessEventScope[dogma.ProcessRoot],
			e dogma.Event,
		) error {
			switch e.(type) {
			case *stubs.EventStub[stubs.TypeA]:
				s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))
			case *stubs.EventStub[stubs.TypeB]:
				s.End()
			}
			return nil
		}

		streamID, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1, stubs.EventB1)

		// Handle the first event, which schedules a deadline.
		if _, _, err := sink.ConsumeSegment(
			t.Context(),
			0,
			eventstream.Segment{
				StreamID:  streamID,
				End:       1,
				Envelopes: envs[:1],
			},
		); err != nil {
			t.Fatal(err)
		}

		// Lock the deadline's row, as the consumer does when it claims the
		// deadline for an attempt.
		lockHolder, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lockHolder.Rollback()

		if err := xsql.ExecOne(
			t.Context(),
			lockHolder,
			`SELECT 1
			FROM messagequeue.messages
			WHERE handler_key = $1
			AND instance_id = '<instance>'
			FOR UPDATE`,
			xsql.UUID(sink.Identity.GetKey()),
		); err != nil {
			t.Fatal(err)
		}

		// Handle the second event, which ends the instance. If ending the
		// instance waits for the locked row, the context's deadline fails the
		// call instead of letting the test hang.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		if _, _, err := sink.ConsumeSegment(
			ctx,
			1,
			eventstream.Segment{
				StreamID:  streamID,
				Begin:     1,
				End:       2,
				Envelopes: envs[1:],
			},
		); err != nil {
			t.Fatal(err)
		}

		var ended bool
		if err := db.QueryRowContext(
			t.Context(),
			`SELECT ended
			FROM process.instances
			WHERE handler_key = $1
			AND instance_id = '<instance>'`,
			xsql.UUID(sink.Identity.GetKey()),
		).Scan(&ended); err != nil {
			t.Fatal(err)
		}

		if !ended {
			t.Fatal("expected the instance to be ended")
		}
	})
}

// TestInstance_marshalingFailure verifies that none of the effects of handling
// a message are committed when the instance's root can't be marshaled, and
// that they're committed once it can be.
func TestInstance_marshalingFailure(t *testing.T) {
	const handlerKey = "5d3a8f4c-1e2b-4c7d-9a6e-0f8b2c4d6e1a"

	commandTypeID := stubs.MessageTypeUUID[*stubs.CommandStub[stubs.TypeX]]()
	originalDeadlineTypeID := stubs.MessageTypeUUID[*stubs.DeadlineStub[stubs.TypeA]]()
	successorDeadlineTypeID := stubs.MessageTypeUUID[*stubs.DeadlineStub[stubs.TypeB]]()

	const (
		marshalSucceeds = iota
		marshalFails
		marshalPanics
	)

	// handler returns a process handler for one instance. Handling an event of
	// type A schedules a deadline of type A, due immediately. Handling that
	// deadline, or an event of type B, mutates the root, executes a command
	// and, for the deadline, schedules a deadline of type B.
	//
	// While mode is marshalFails or marshalPanics, the root can't be marshaled
	// once it has been mutated by anything but an event of type A.
	handler := func(mode *atomic.Int32, eventsHandled *atomic.Int32) dogma.HandlerRoute {
		return dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeB]](),
					)
				},
				NewFunc: func() *stubs.ProcessRootStub {
					r := &stubs.ProcessRootStub{}
					r.MarshalBinaryFunc = func() ([]byte, error) {
						if r.Value != "mutated by event A" {
							switch mode.Load() {
							case marshalFails:
								return nil, errors.New("<marshal error>")
							case marshalPanics:
								panic("<marshal panic>")
							}
						}
						return json.Marshal(r)
					}
					return r
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
					e dogma.Event,
				) error {
					eventsHandled.Add(1)

					switch e.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(r *stubs.ProcessRootStub) {
							r.Value = "mutated by event A"
						})
						s.ScheduleDeadline(stubs.DeadlineA1, time.Now())
					case *stubs.EventStub[stubs.TypeB]:
						s.Mutate(func(r *stubs.ProcessRootStub) {
							r.Value = "mutated by event B"
						})
						s.ExecuteCommand(stubs.CommandX1)
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					_ dogma.Deadline,
				) error {
					s.Mutate(func(r *stubs.ProcessRootStub) {
						r.Value = "mutated by deadline"
					})
					s.ExecuteCommand(stubs.CommandX1)
					s.ScheduleDeadline(stubs.DeadlineB1, time.Now().Add(time.Hour))
					return nil
				},
			},
		)
	}

	// expectQueued checks the number of queued messages of the given type.
	expectQueued := func(t testing.TB, db *sql.DB, description string, typeID *uuidpb.UUID, want int) {
		t.Helper()

		xtesting.ExpectQueryResult(
			t,
			description,
			want,
			db,
			`SELECT count(*)
			FROM messagequeue.messages
			WHERE message_type_id = $1`,
			xsql.UUID(typeID),
		)
	}

	// waitForDeadlineToFail waits until the deadline of type A has failed at
	// least once.
	waitForDeadlineToFail := func(t testing.TB, db *sql.DB) {
		t.Helper()

		xtesting.WaitForQueryResult(
			t,
			"the deadline to fail",
			true,
			db,
			`SELECT EXISTS (
				SELECT 1
				FROM messagequeue.messages
				WHERE message_type_id = $1
				AND failures >= 1
			)`,
			xsql.UUID(originalDeadlineTypeID),
		)
	}

	// storedValue returns the Value of the instance's stored root, or nil if
	// the instance has no stored state.
	storedValue := func(t testing.TB, db *sql.DB) any {
		t.Helper()

		var state []byte
		err := db.QueryRowContext(
			t.Context(),
			`SELECT state
			FROM process.instances
			WHERE handler_key = $1
			AND instance_id = '<instance>'`,
			handlerKey,
		).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		if state == nil {
			return nil
		}

		var r stubs.ProcessRootStub
		if err := json.Unmarshal(state, &r); err != nil {
			t.Fatal(err)
		}
		return r.Value
	}

	for _, c := range []struct {
		Name string
		Mode int32
	}{
		{"returns an error", marshalFails},
		{"panics", marshalPanics},
	} {
		t.Run("it commits none of a deadline's effects when marshaling the state "+c.Name, func(t *testing.T) {
			var mode, eventsHandled atomic.Int32
			mode.Store(c.Mode)

			xtesting.RunEngines(
				t,
				func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
					xtesting.PopulateEventStreams(
						t,
						db,
						func(*uuidpb.UUID, uint64) dogma.Event {
							return stubs.EventA1
						},
						1,
					)

					waitForDeadlineToFail(t, db)

					expectQueued(t, db, "commands", commandTypeID, 0)
					expectQueued(t, db, "successor deadlines", successorDeadlineTypeID, 0)

					if got, want := storedValue(t, db), "mutated by event A"; got != want {
						t.Fatalf("unexpected stored state: got %v, want %v", got, want)
					}
				},
				handler(&mode, &eventsHandled),
			)
		})
	}

	t.Run("it commits a deadline's effects once marshaling succeeds on a later attempt", func(t *testing.T) {
		var mode, eventsHandled atomic.Int32
		mode.Store(marshalFails)

		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				xtesting.PopulateEventStreams(
					t,
					db,
					func(*uuidpb.UUID, uint64) dogma.Event {
						return stubs.EventA1
					},
					1,
				)

				waitForDeadlineToFail(t, db)

				mode.Store(marshalSucceeds)

				xtesting.WaitForQueryResult(
					t,
					"the deadline to be delivered",
					0,
					db,
					`SELECT count(*)
					FROM messagequeue.messages
					WHERE message_type_id = $1`,
					xsql.UUID(originalDeadlineTypeID),
				)

				expectQueued(t, db, "commands", commandTypeID, 1)
				expectQueued(t, db, "successor deadlines", successorDeadlineTypeID, 1)

				if got, want := storedValue(t, db), "mutated by deadline"; got != want {
					t.Fatalf("unexpected stored state: got %v, want %v", got, want)
				}
			},
			handler(&mode, &eventsHandled),
		)
	})

	t.Run("it commits none of an event's effects when marshaling the state fails", func(t *testing.T) {
		var mode, eventsHandled atomic.Int32
		mode.Store(marshalFails)

		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				xtesting.PopulateEventStreams(
					t,
					db,
					func(*uuidpb.UUID, uint64) dogma.Event {
						return stubs.EventB1
					},
					1,
				)

				// The event is handled again after each failure, so a second
				// call means that the first attempt has ended.
				xtesting.WaitFor(
					t,
					"the event to be handled again",
					func() bool { return eventsHandled.Load() >= 2 },
				)

				expectQueued(t, db, "commands", commandTypeID, 0)

				if got := storedValue(t, db); got != nil {
					t.Fatalf("unexpected stored state: got %v, want none", got)
				}
			},
			handler(&mode, &eventsHandled),
		)
	})
}
