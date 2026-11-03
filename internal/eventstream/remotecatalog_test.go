package eventstream_test

import (
	"context"
	"database/sql"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestRemoteCatalog_streamIDs(t *testing.T) {
	t.Run("it reports streams that exist when it starts", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				streamIDs := xtesting.CreateEventStreams(t, db, 3)
				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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

	t.Run("it offers no streams if the server does not offer event streams", func(t *testing.T) {
		var calls atomic.Int32

		server := grpc.NewServer(
			grpc.UnknownServiceHandler(
				func(any, grpc.ServerStream) error {
					calls.Add(1)
					return status.Error(codes.Unimplemented, "unknown service")
				},
			),
		)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}

		go server.Serve(listener)
		t.Cleanup(server.Stop)

		conn, err := grpc.NewClient(
			listener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })

		catalog := &RemoteCatalog{
			Client:          messaginggrpc.NewEventStreamConsumerAPIClient(conn),
			RecheckInterval: time.Hour,
			Logger:          spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, catalog.Run)

		deadline := time.After(xtesting.WaitTimeout)
		for calls.Load() == 0 {
			select {
			case <-deadline:
				t.Fatal("timeout waiting for the catalog to list event streams")
			case <-time.After(10 * time.Millisecond):
			}
		}

		// A failure would be retried well within this period.
		time.Sleep(500 * time.Millisecond)

		if n := calls.Load(); n != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", n)
		}

		expectStreamIDs(t, catalog)
	})

	t.Run("it lists event streams again after the server reports that it does not offer them", func(t *testing.T) {
		streamID := uuidpb.Generate()

		var calls atomic.Int32

		client := &messaginggrpc.EventStreamConsumerAPIClientStub{
			ListEventStreamsFunc: func(
				ctx context.Context,
				_ *messaginggrpc.ListEventStreamsRequest,
				_ ...grpc.CallOption,
			) (messaginggrpc.EventStreamConsumerAPI_ListEventStreamsClient, error) {
				if calls.Add(1) == 1 {
					return nil, status.Error(codes.Unimplemented, "<unimplemented>")
				}

				return &streamClientStub[messaginggrpc.ListEventStreamsResponse]{
					Ctx: ctx,
					Responses: []*messaginggrpc.ListEventStreamsResponse{
						messaginggrpc.NewListEventStreamsResponseBuilder().
							WithEventStream(
								messaginggrpc.NewEventStreamBuilder().
									WithEventStreamId(streamID).
									Build(),
							).
							Build(),
					},
				}, nil
			},
		}

		catalog := &RemoteCatalog{
			Client:          client,
			RecheckInterval: 10 * time.Millisecond,
			Logger:          spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, catalog.Run)

		expectStreamIDs(t, catalog, streamID)
	})
}

func TestRemoteCatalog_subscribe(t *testing.T) {
	t.Run("it delivers historical then contemporary events in order", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				streamID, historical := xtesting.AppendToNewEventStream(
					t,
					db,
					stubs.EventA1,
					stubs.EventA2,
					stubs.EventA3,
				)

				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				streamID := xtesting.CreateEventStream(t, db)
				envelopes := xtesting.AppendToEventStream(
					t,
					db,
					streamID,
					stubs.EventA1,
					stubs.EventB1,
					stubs.EventA2,
				)

				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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

	t.Run("it keeps a subscription that the application will never deliver to", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				streamID, _ := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				// The application can not record events of type X, so it closes
				// the stream once it has delivered the existing events.
				batches := make(chan Batch, 10)
				lost, unsubscribe := catalog.Subscribe(
					streamID,
					0,
					uuidcollection.NewSet(
						stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeX]](),
					),
					batches,
				)
				defer unsubscribe()

				xtesting.ExpectToReceiveN(t, "batch", batches, Batch{
					StreamID:   streamID,
					Checkpoint: 1,
				})
				xtesting.ExpectNoReceive(t, "lost subscription", lost)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it consumes again from the checkpoint after the application closes the stream", func(t *testing.T) {
		streamID := uuidpb.Generate()
		checkpoints := make(chan uint64, 2)

		client := &messaginggrpc.EventStreamConsumerAPIClientStub{
			ConsumeEventsFunc: func(
				ctx context.Context,
				req *messaginggrpc.ConsumeEventsRequest,
				_ ...grpc.CallOption,
			) (messaginggrpc.EventStreamConsumerAPI_ConsumeEventsClient, error) {
				checkpoint := req.GetCheckpointOffset()
				checkpoints <- checkpoint

				stream := &streamClientStub[messaginggrpc.ConsumeEventsResponse]{
					Ctx: ctx,
					Responses: []*messaginggrpc.ConsumeEventsResponse{
						messaginggrpc.NewConsumeEventsResponseBuilder().
							WithCheckpointOffset(checkpoint + 1).
							Build(),
					},
				}

				if checkpoint == 0 {
					stream.Err = io.EOF
				}

				return stream, nil
			},
		}

		catalog := &RemoteCatalog{
			Client:          client,
			RecheckInterval: 10 * time.Millisecond,
			Logger:          spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, catalog.Run)

		batches := make(chan Batch, 10)
		lost, unsubscribe := catalog.Subscribe(
			streamID,
			0,
			uuidcollection.NewSet(uuidpb.Generate()),
			batches,
		)
		defer unsubscribe()

		xtesting.ExpectToReceiveN(t, "checkpoint", checkpoints, 0, 1)
		xtesting.ExpectToReceiveN(
			t,
			"batch",
			batches,
			Batch{
				StreamID:   streamID,
				Checkpoint: 1,
			},
			Batch{
				StreamID:   streamID,
				Checkpoint: 2,
			},
		)
		xtesting.ExpectNoReceive(t, "lost subscription", lost)
	})

	t.Run("it does not report a subscription as lost when the stream is not found", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				streamID := xtesting.CreateEventStream(t, db)
				client, _ := newConsumerClient(t, cluster)

				catalog := &RemoteCatalog{
					Client:          client,
					RecheckInterval: time.Hour,
					Logger:          spruce.NewTestLogger(t),
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

// streamClientStub is a server-streaming gRPC client that receives the given
// responses, then returns Err. If Err is nil, it waits until Ctx is canceled.
type streamClientStub[T any] struct {
	grpc.ClientStream

	Ctx       context.Context
	Responses []*T
	Err       error
}

func (s *streamClientStub[T]) Recv() (*T, error) {
	if len(s.Responses) > 0 {
		res := s.Responses[0]
		s.Responses = s.Responses[1:]
		return res, nil
	}

	if s.Err != nil {
		return nil, s.Err
	}

	<-s.Ctx.Done()
	return nil, s.Ctx.Err()
}
