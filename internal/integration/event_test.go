package integration_test

import (
	"context"
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

func TestEventsRecordedByTheSameCommandAreAppendedToAnEventStreamInOrder(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Find the stream that was used for these events.
			streamID := &uuidpb.UUID{}
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT id
				FROM dogma.event_streams`,
			)
			if err := row.Scan(xsql.UUID(streamID)); err != nil {
				t.Fatalf("unable to find event stream: %v", err)
			}

			xtesting.ExpectContiguousEvents(
				t,
				engine.DB,
				streamID,
				0,
				&stubs.EventStub[stubs.TypeA]{Content: "event-0"},
				&stubs.EventStub[stubs.TypeA]{Content: "event-1"},
				&stubs.EventStub[stubs.TypeA]{Content: "event-2"},
			)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event-0"})
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event-1"})
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{Content: "event-2"})
					return nil
				},
			},
		),
	)
}

func TestEventsRecordedByDifferentCommandsAreDistributedAcrossStreams(t *testing.T) {
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

			// Send commands sequentially, waiting between each so that each
			// Acquire() call sees the updated next_offset from the prior
			// command's events.
			for range 3 {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectEmptyCommandQueueEventually(
					t,
					engine.DB,
				)
			}

			// Verify that events were distributed across more than one
			// stream.
			var distinctStreams int
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT COUNT(*)
				FROM dogma.event_streams
				WHERE next_offset > 0`,
			)
			if err := row.Scan(&distinctStreams); err != nil {
				t.Fatalf("unable to count distinct event streams: %v", err)
			}
			if distinctStreams <= 1 {
				t.Fatalf("expected events on more than 1 stream, got %d", distinctStreams)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					return nil
				},
			},
		),
	)
}
