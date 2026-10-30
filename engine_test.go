package runkit_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestEngine(t *testing.T) {
	t.Run("it does not listen for gRPC requests by default", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- engine.Run(ctx)
		}()

		select {
		case <-engine.Ready():
		case err := <-done:
			t.Fatalf("engine stopped before it was ready: %v", err)
		}

		if addr, err := engine.ListenAddr(ctx); err == nil {
			t.Fatalf("expected an error, got address %s", addr)
		}

		cancel()

		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("it creates the gRPC server with the given options", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithListenAddress(
				"127.0.0.1:0",
				grpc.ChainStreamInterceptor(
					func(
						any,
						grpc.ServerStream,
						*grpc.StreamServerInfo,
						grpc.StreamHandler,
					) error {
						return status.Error(codes.PermissionDenied, "<denied>")
					},
				),
			),
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- engine.Run(ctx)
		}()
		defer func() {
			cancel()
			<-done
		}()

		addr, err := engine.ListenAddr(ctx)
		if err != nil {
			t.Fatal(err)
		}

		conn, err := grpc.NewClient(
			addr.String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		stream, err := messaginggrpc.
			NewEventStreamConsumerAPIClient(conn).
			ListEventStreams(ctx, &messaginggrpc.ListEventStreamsRequest{})
		if err == nil {
			_, err = stream.Recv()
		}

		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("expected the request to be denied by the interceptor, got %v", err)
		}
	})

	t.Run("it panics if the listen address is empty", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithListenAddress("")
	})

	t.Run("it returns an error if it can not listen for gRPC requests", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		// Occupy a port so that the engine can not listen on it.
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()

		engine := runkit.New(
			testApp(),
			db,
			runkit.WithListenAddress(occupied.Addr().String()),
			runkit.WithLogger(spruce.NewTestLogger(t)),
		)

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err = engine.Run(ctx)
		if err == nil || errors.Is(err, ctx.Err()) {
			t.Fatalf("expected a listen error, got %v", err)
		}

		var opErr *net.OpError
		if !errors.As(err, &opErr) || opErr.Op != "listen" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// testApp returns a minimal application for testing the engine.
func testApp() dogma.Application {
	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<app>", "a2b5f0ae-4b1a-4c6e-9d26-0c5a2e5a6b3d")
			c.Routes(
				dogma.ViaIntegration(
					&stubs.IntegrationMessageHandlerStub{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<handler>", "b2e26cc1-94ed-4752-8a04-1c78e6e4d6a0")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
					},
				),
			)
		},
	}
}
