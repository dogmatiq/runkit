package remoteengine_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/remoteengine"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
)

func TestConnector(t *testing.T) {
	typeA := uuidpb.Generate()
	typesA := uuidcollection.NewSet(typeA)

	// setup returns a connector that adds the event streams of the remote
	// engines it discovers to the returned catalog, watching them for events
	// of the given types.
	setup := func(
		t *testing.T,
		types *uuidcollection.Set,
		discover func(context.Context, func(string, grpc.ClientConnInterface) func()) error,
	) (*Connector, *eventstream.Catalog) {
		catalog := &eventstream.Catalog{
			CacheSize:    10,
			EngineLogger: spruce.NewTestLogger(t),
		}

		connector := &Connector{
			Discover:     discover,
			EventStreams: catalog,
			EventTypeIDs: types,
			EngineLogger: spruce.NewTestLogger(t),
		}

		return connector, catalog
	}

	// expectCalls waits until the given counter reaches the given value.
	expectCalls := func(t *testing.T, calls *atomic.Int32, want int32) {
		t.Helper()

		xtesting.WaitFor(
			t,
			fmt.Sprintf("%d calls", want),
			func() bool { return calls.Load() >= want },
		)
	}

	t.Run("it calls the discoverer again if it fails", func(t *testing.T) {
		var calls atomic.Int32

		connector, _ := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				_ func(string, grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					return errors.New("<error>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCalls(t, &calls, 2)
	})

	t.Run("it calls the discoverer again if it panics", func(t *testing.T) {
		var calls atomic.Int32

		connector, _ := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				_ func(string, grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					panic("<panic>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCalls(t, &calls, 2)
	})

	t.Run("it calls the discoverer again if it adds a nil connection", func(t *testing.T) {
		var calls atomic.Int32

		connector, _ := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					add("<engine>", nil)
				}

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCalls(t, &calls, 2)
	})

	t.Run("it adds the event streams of each remote engine to the catalog", func(t *testing.T) {
		engineA := &consumerAPIStub{StreamID: uuidpb.Generate()}
		engineB := &consumerAPIStub{StreamID: uuidpb.Generate()}

		connA := serve(t, engineA)
		connB := serve(t, engineB)

		connector, catalog := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				add("<engine-a>", connA)
				add("<engine-b>", connB)

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCatalogToHave(t, catalog, engineA.StreamID, engineB.StreamID)
	})

	t.Run("it watches each remote engine for the given event types", func(t *testing.T) {
		engine := &consumerAPIStub{
			StreamID: uuidpb.Generate(),
			Requests: make(chan *messaginggrpc.WatchEventsRequest, 10),
		}

		conn := serve(t, engine)

		connector, _ := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				add("<engine>", conn)

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the remote engine to be watched")
		case req := <-engine.Requests:
			if got := uuidcollection.NewSet(req.GetFilter().GetSpecific().GetIds()...); !got.IsEqual(typesA) {
				t.Fatalf("unexpected message types: got %s, want %s", got, typesA)
			}
		}
	})

	t.Run("it does not watch remote engines if there are no event types", func(t *testing.T) {
		engine := &consumerAPIStub{
			StreamID: uuidpb.Generate(),
			Requests: make(chan *messaginggrpc.WatchEventsRequest, 10),
		}

		conn := serve(t, engine)
		added := make(chan struct{})

		connector, _ := setup(
			t,
			uuidcollection.NewSet(),
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				add("<engine>", conn)
				close(added)

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		<-added

		xtesting.ExpectNoReceive(t, "request", engine.Requests)
	})

	t.Run("it removes a remote engine's event streams from the catalog when the remote engine is removed", func(t *testing.T) {
		engineA := &consumerAPIStub{StreamID: uuidpb.Generate()}
		engineB := &consumerAPIStub{StreamID: uuidpb.Generate()}

		connA := serve(t, engineA)
		connB := serve(t, engineB)

		removeA := make(chan func(), 1)

		connector, catalog := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				removeA <- add("<engine-a>", connA)
				add("<engine-b>", connB)

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCatalogToHave(t, catalog, engineA.StreamID, engineB.StreamID)

		remove := <-removeA
		remove()

		expectCatalogToHave(t, catalog, engineB.StreamID)
	})

	t.Run("it removes every remote engine's event streams from the catalog when the discoverer returns", func(t *testing.T) {
		engine := &consumerAPIStub{StreamID: uuidpb.Generate()}
		conn := serve(t, engine)

		var calls atomic.Int32
		fail := make(chan struct{})

		connector, catalog := setup(
			t,
			typesA,
			func(
				ctx context.Context,
				add func(string, grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					add("<engine>", conn)
					<-fail
					return errors.New("<error>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
		)
		xtesting.GoE(t, connector.Run)

		expectCatalogToHave(t, catalog, engine.StreamID)

		close(fail)

		expectCatalogToHave(t, catalog)
		expectCalls(t, &calls, 2)
	})
}

// consumerAPIStub is a remote engine's event stream consumer API that offers
// one empty event stream.
type consumerAPIStub struct {
	messaginggrpc.UnimplementedEventStreamConsumerAPIServer

	// StreamID is the ID of the event stream that the remote engine offers.
	StreamID *uuidpb.UUID

	// Requests, if non-nil, receives each WatchEvents request.
	Requests chan *messaginggrpc.WatchEventsRequest
}

func (s *consumerAPIStub) WatchEvents(
	req *messaginggrpc.WatchEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
	if s.Requests != nil {
		s.Requests <- req
	}

	if err := res.Send(
		messaginggrpc.NewEventStreamSegmentBuilder().
			WithEventStreamId(s.StreamID).
			Build(),
	); err != nil {
		return err
	}

	<-res.Context().Done()
	return res.Context().Err()
}

// serve serves the given API on a loopback port until the test ends, and
// returns a connection to it.
func serve(t *testing.T, api messaginggrpc.EventStreamConsumerAPIServer) *grpc.ClientConn {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()
	messaginggrpc.RegisterEventStreamConsumerAPIServer(server, api)

	go server.Serve(listener)
	t.Cleanup(server.Stop)

	return xtesting.DialGRPCAddr(t, listener.Addr().String())
}

// expectCatalogToHave waits for the catalog to have exactly the given streams.
func expectCatalogToHave(
	t testing.TB,
	c *eventstream.Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the catalog to have streams %s", expected),
		c.StreamIDs(),
		expected.IsEqual,
	)
}
