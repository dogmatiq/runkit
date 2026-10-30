package grpcserver_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	. "github.com/dogmatiq/runkit/internal/grpcserver"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	t.Run("it listens again on the same address when serving fails", func(t *testing.T) {
		listeners := make(chan net.Listener, 1)

		server := &Server{
			Address:                ":0",
			EventStreamConsumerAPI: messaginggrpc.UnimplementedEventStreamConsumerAPIServer{},
			Logger:                 spruce.NewTestLogger(t),
			Listen: func(network, address string) (net.Listener, error) {
				lis, err := net.Listen(network, address)
				if err == nil {
					select {
					case listeners <- lis:
					default:
					}
				}
				return lis, err
			},
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := server.Run(ctx); !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}
		}()
		defer func() {
			cancel()
			<-done
		}()

		addr, err := server.ListenAddr(ctx)
		if err != nil {
			t.Fatal(err)
		}

		expectServing(t, addr)

		// Close the listener behind the server's back, which causes serving
		// to fail.
		(<-listeners).Close()

		expectServing(t, addr)
	})
}

// expectServing asserts that a server responds to requests on addr within the
// test timeout.
//
// A new connection is made for each attempt, so that a response can only come
// from a server that is accepting connections.
func expectServing(t *testing.T, addr net.Addr) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for {
		err := request(t, addr, deadline)

		// The service is unimplemented, so a response of UNIMPLEMENTED means
		// the request reached the server.
		if status.Code(err) == codes.Unimplemented {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("server is not responding: %v", err)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// request makes a single request to the server at addr over a new connection
// and returns the error that results.
func request(t *testing.T, addr net.Addr, deadline time.Time) error {
	t.Helper()

	conn, err := grpc.NewClient(
		addr.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()

	client := messaginggrpc.NewEventStreamConsumerAPIClient(conn)

	stream, err := client.ListEventStreams(ctx, &messaginggrpc.ListEventStreamsRequest{})
	if err != nil {
		return err
	}

	_, err = stream.Recv()
	return err
}
