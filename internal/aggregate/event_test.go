package aggregate_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventsAreAppendedToTheEventStreamInOrder(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			want := []dogma.Event{
				&stubs.EventStub[stubs.TypeA]{Content: "event-0"},
				&stubs.EventStub[stubs.TypeA]{Content: "event-1"},
				&stubs.EventStub[stubs.TypeA]{Content: "event-2"},
			}

			// Send commands sequentially, waiting between each so that the
			// order of handling is deterministic.
			for _, event := range want {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{
						Content: event.(*stubs.EventStub[stubs.TypeA]).Content,
					},
				)

				xtesting.ExpectEmptyCommandQueueEventually(
					t,
					engine.DB,
				)
			}

			// Find the stream used for this instance.
			streamID := &uuidpb.UUID{}
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT event_stream_id
				FROM dogma.aggregate_instances
				WHERE instance_id = '<instance>'`,
			)
			if err := row.Scan(xsql.UUID(streamID)); err != nil {
				t.Fatalf("unable to find event stream: %v", err)
			}

			xtesting.ExpectContiguousEvents(
				t,
				engine.DB,
				streamID,
				0,
				want...,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
						Content: m.(*stubs.CommandStub[stubs.TypeA]).Content,
					})
				},
			},
		),
	)
}

func TestAllEventsFromTheSameInstanceAreAppendedToTheSameStream(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Force creation of multiple event streams so that the
			// controller's use of [eventstream.Acquire] doesn't just create
			// a single stream and use it continuously.
			xtesting.Transact(
				t,
				engine.DB,
				func(tx *sql.Tx) {
					for range 3 {
						if _, err := eventstream.ForceCreate(t.Context(), tx); err != nil {
							t.Fatal(err)
						}
					}
				},
			)

			// Send multiple commands to two different instances, each
			// recording an event, to verify that events from the same
			// instance always end up on the same stream.
			for range 3 {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{
						Content: "<instance-a>",
					},
				)
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{
						Content: "<instance-b>",
					},
				)
			}

			// Wait until all commands are handled.
			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Verify that all events for each instance were appended to
			// the same event stream by checking that only one distinct
			// stream ID is present per instance.
			xtesting.ExpectQueryResult(
				t,
				"distinct event stream count for instance-a",
				1,
				engine.DB,
				`SELECT COUNT(DISTINCT event_stream_id)
				FROM dogma.events
				WHERE aggregate_instance_id = '<instance-a>'`,
			)

			xtesting.ExpectQueryResult(
				t,
				"distinct event stream count for instance-b",
				1,
				engine.DB,
				`SELECT COUNT(DISTINCT event_stream_id)
				FROM dogma.events
				WHERE aggregate_instance_id = '<instance-b>'`,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
				},
			},
		),
	)
}
