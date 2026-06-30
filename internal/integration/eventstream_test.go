package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestEventStream_eventsAreAppendedInOrder verifies that events recorded by a
// single command are appended to an event stream in the order they are
// recorded.
func TestEventStream_eventsAreAppendedInOrder(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)

			// Find the stream that was used for these events.
			streamID := &uuidpb.UUID{}
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT id
				FROM eventstream.streams`,
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
					c.Identity("<handler>", "8d159878-4322-496c-bede-a3e0f4c12634")
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

// TestEventStream_eventsAreDistributedAcrossStreams verifies that events
// recorded by different commands are distributed across multiple event streams.
func TestEventStream_eventsAreDistributedAcrossStreams(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Force creation of multiple event streams so that the message pump
			// doesn't just create a single stream and use it continuously.
			xtesting.CreateEventStreams(t, engine.DB, 3)

			// Send commands sequentially, waiting between each so that each
			// Acquire() call sees the updated next_offset from the prior
			// command's events.
			xtesting.ExecuteCommandsSequentially(t, engine, stubs.CommandA1, stubs.CommandA1, stubs.CommandA1)

			// Verify that events were distributed across more than one
			// stream.
			var distinctStreams int
			row := engine.DB.QueryRowContext(
				t.Context(),
				`SELECT COUNT(*)
				FROM eventstream.streams
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
					c.Identity("<handler>", "1385128f-5ddb-4ae8-95c9-1d9e7ab3090b")
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

// TestEventStream_eventsAreNotRecordedWhenHandlerReturnsAnError verifies that
// events are discarded when the handler returns an error.
func TestEventStream_eventsAreNotRecordedWhenHandlerReturnsAnError(t *testing.T) {
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
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "7e36acd8-8d9a-4740-9732-0f6ebf819f48")
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
					return errors.New("<error>")
				},
			},
		),
	)
}
