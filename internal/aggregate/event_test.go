package aggregate_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventsAreAppendedToTheEventStreamInOrder(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			want := []*stubs.EventStub[stubs.TypeA]{
				{Content: "event-0"},
				{Content: "event-1"},
				{Content: "event-2"},
			}

			// Send commands sequentially, waiting between each so that the
			// order of handling is deterministic.
			for _, event := range want {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{
						Content: event.Content,
					},
				)

				xtesting.ExpectEmptyCommandQueueEventually(
					t,
					engine.DB,
				)
			}

			// Verify that events appear on the stream in the order they
			// were recorded.
			rows, err := engine.DB.QueryContext(
				t.Context(),
				`SELECT
					event_stream_offset,
					envelope
				FROM dogma.events
				WHERE aggregate_instance_id = '<instance>'
				ORDER BY event_stream_offset
				LIMIT $1`,
				len(want),
			)
			if err != nil {
				t.Fatalf("unable to query events: %v", err)
			}
			defer rows.Close()

			var wantOffset eventstream.Offset

			for rows.Next() {
				var gotOffset eventstream.Offset
				env := &envelopepb.Envelope{}
				if err := rows.Scan(&gotOffset, xsql.Envelope(env)); err != nil {
					t.Fatalf("unable to scan event: %v", err)
				}

				if gotOffset != wantOffset {
					t.Fatalf(
						"unexpected event stream offset: got %d, want %d",
						gotOffset,
						wantOffset,
					)
				}

				got, err := envelopepb.Unpack[dogma.Event](env)
				if err != nil {
					t.Fatalf("unable to unpack event: %v", err)
				}

				wantEvent := want[0]
				want = want[1:]

				if !reflect.DeepEqual(got, wantEvent) {
					t.Logf("unexpected event at offset %d:", gotOffset)
					t.Logf("+++ got:\n%#v", got)
					t.Logf("--- want:\n%#v", wantEvent)
					t.FailNow()
				}

				wantOffset++
			}

			if len(want) != 0 {
				t.Fatalf("missing %d event(s)", len(want))
			}
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
