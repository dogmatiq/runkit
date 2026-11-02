package eventstream_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestRemoteCatalog_streamIDs(t *testing.T) {
	t.Run("it reports streams that exist when it starts", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamIDs := xtesting.CreateEventStreams(t, db, 3)
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				expectStreamIDs(t, catalog, streamIDs...)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it reports streams that are created after it starts", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				first := xtesting.CreateEventStream(t, db)
				expectStreamIDs(t, catalog, first)

				second := xtesting.CreateEventStream(t, db)
				expectStreamIDs(t, catalog, first, second)
			},
			consumerAPIApp(),
		)
	})
}

func TestRemoteCatalog_subscribe(t *testing.T) {
	t.Run("it delivers historical then contemporary events in order", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamID, historical := xtesting.AppendToNewEventStream(
					t,
					db,
					stubs.EventA1,
					stubs.EventA2,
					stubs.EventA3,
				)

				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				batches := make(chan Batch, 10)
				_, unsubscribe := catalog.Subscribe(
					streamID,
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
					),
					batches,
				)
				defer unsubscribe()

				expectToReceiveEnvelopes(t, batches, historical...)

				contemporary := xtesting.AppendToEventStream(
					t,
					db,
					streamID,
					stubs.EventA1,
					stubs.EventA2,
				)

				checkpoint := expectToReceiveEnvelopes(t, batches, contemporary...)

				if checkpoint != 5 {
					t.Fatalf("unexpected checkpoint: got %d, want 5", checkpoint)
				}
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers only events of the requested types", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamID := xtesting.CreateEventStream(t, db)
				envelopes := xtesting.AppendToEventStream(
					t,
					db,
					streamID,
					stubs.EventA1,
					stubs.EventB1,
					stubs.EventA2,
				)

				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				batches := make(chan Batch, 10)
				_, unsubscribe := catalog.Subscribe(
					streamID,
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
					),
					batches,
				)
				defer unsubscribe()

				expectToReceiveEnvelopes(t, batches, envelopes[0], envelopes[2])
			},
			consumerAPIApp(),
		)
	})

	t.Run("it reports a subscription as lost when the application closes the stream", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamID, _ := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				// The application can not record events of type X, so it closes
				// the stream once it has delivered the existing events.
				lost, unsubscribe := catalog.Subscribe(
					streamID,
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeX]](),
					),
					make(chan Batch, 10),
				)
				defer unsubscribe()

				xtesting.ExpectToReceiveN(t, "lost subscription", lost, struct{}{})
			},
			consumerAPIApp(),
		)
	})

	t.Run("it does not report a subscription as lost when the stream is not found", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				lost, unsubscribe := catalog.Subscribe(
					uuidpb.Generate(),
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
					),
					make(chan Batch, 10),
				)
				defer unsubscribe()

				xtesting.ExpectNoReceive(t, "lost subscription", lost)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it reports subscriptions as lost when it stops", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamID := xtesting.CreateEventStream(t, db)
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				stop := xtesting.GoE(t, catalog.Run)

				lost, unsubscribe := catalog.Subscribe(
					streamID,
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
					),
					make(chan Batch, 10),
				)
				defer unsubscribe()

				xtesting.ExpectNoReceive(t, "lost subscription", lost)

				stop()

				xtesting.ExpectToReceiveN(t, "lost subscription", lost, struct{}{})
			},
			consumerAPIApp(),
		)
	})
}
