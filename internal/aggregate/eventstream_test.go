package aggregate_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestEventStream_instanceBoundToStream verifies that all events recorded by a
// single instance are appended to the same event stream. That is, the instance
// is immutably "bound" to a single stream upon creation.
func TestEventStream_instanceBoundToStream(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Force creation of multiple event streams so that the
			// implementation has multiple to choose from.
			xtesting.CreateEventStreams(t, engine.DB, 3)

			// Send multiple commands to two different instances, each command
			// records a new event.
			for range 3 {
				xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
				xtesting.ExecuteCommand(t, engine, stubs.CommandA2)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)

			// Verify that all events recorded by "instance-a" appear on a
			// single stream.
			xtesting.ExpectQueryResult(
				t,
				"distinct event stream count for instance A1",
				1,
				engine.DB,
				`SELECT COUNT(DISTINCT stream_id)
				FROM eventstream.events
				WHERE aggregate_instance_id = $1`,
				stubs.CommandA1.Content,
			)

			// Verify that all events recorded by "instance-b" appear on a
			// single stream.
			xtesting.ExpectQueryResult(
				t,
				"distinct event stream count for instance A2",
				1,
				engine.DB,
				`SELECT COUNT(DISTINCT stream_id)
				FROM eventstream.events
				WHERE aggregate_instance_id = $1`,
				stubs.CommandA2.Content,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "1735e561-a373-4aa0-9461-1d517a3f36c0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						return string(m.Content)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					s.RecordEvent(stubs.EventA1)
				},
			},
		),
	)
}

// TestEventStream_eventsAreAppendedInOrder verifies that events recorded by a single
// instance are appended to the event stream in the same order that they are
// recorded.
func TestEventStream_eventsAreAppendedInOrder(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Send three commands, waiting between each to ensure the order of
			// handling is deterministic. Each command produces two events.
			xtesting.ExecuteCommandsSequentially(
				t,
				engine,
				stubs.CommandA1,
				stubs.CommandA2,
				stubs.CommandA3,
			)

			// Find the stream that the instance is bound to.
			streamID := &uuidpb.UUID{}
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT stream_id
				FROM aggregate.instances`,
			)
			if err := row.Scan(xsql.UUID(streamID)); err != nil {
				t.Fatalf("unable to find event stream: %v", err)
			}

			// Each command records two events, which should be in order
			// relative to each other, as well as relative to the events
			// produced by the other commands.
			xtesting.ExpectContiguousEvents(
				t,
				engine.DB,
				streamID,
				0, // offset
				&stubs.EventStub[stubs.TypeA]{Content: "A1:a"},
				&stubs.EventStub[stubs.TypeA]{Content: "A1:b"},
				&stubs.EventStub[stubs.TypeA]{Content: "A2:a"},
				&stubs.EventStub[stubs.TypeA]{Content: "A2:b"},
				&stubs.EventStub[stubs.TypeA]{Content: "A3:a"},
				&stubs.EventStub[stubs.TypeA]{Content: "A3:b"},
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "81010834-6322-4dc7-9b9c-1b876e31864c")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(dogma.Command) string {
					return "<instance>"
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: m.Content + ":a",
						})
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: m.Content + ":b",
						})
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

// TestEventStream_eventsAreNotRecordedWhenHandlerPanics verifies that events
// recorded before the handler panics are discarded.
func TestEventStream_eventsAreNotRecordedWhenHandlerPanics(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			commandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			xtesting.WaitForCommandToBePostponed(
				t,
				engine.DB,
				commandEnvelope.GetBody().GetMessageId(),
			)

			xtesting.ExpectEventCount(t, engine.DB, 0)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "84a76232-e660-469f-bce0-714a21c6fad1")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(dogma.Command) string {
					return "<instance>"
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					s.RecordEvent(stubs.EventA1)
					panic("<panic>")
				},
			},
		),
	)
}
