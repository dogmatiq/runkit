// Package grpc serves the engine's gRPC services to other engines.
package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"google.golang.org/grpc"
)

// Server serves gRPC requests from other engines.
type Server struct {
	// Address is the TCP address to listen on.
	Address string

	// EventStreamConsumerAPI is the implementation of the
	// [messaginggrpc.EventStreamConsumerAPI] service to serve.
	EventStreamConsumerAPI messaginggrpc.EventStreamConsumerAPIServer

	// Logger is the target for log messages produced by the server.
	Logger *slog.Logger

	// Listen opens a listener. If it is nil, [net.Listen] is used.
	Listen func(network, address string) (net.Listener, error)

	addr xsync.Future[net.Addr]
}

// ListenAddr returns the address that the server is listening on, blocking
// until it has started listening or ctx is canceled.
func (s *Server) ListenAddr(ctx context.Context) (net.Addr, error) {
	return s.addr.WaitContext(ctx)
}

// Run serves gRPC requests until ctx is canceled.
//
// It must be called at most once.
func (s *Server) Run(ctx context.Context) error {
	return supervisor.Run(ctx, s.Logger, s.serve)
}

// serve accepts connections until ctx is canceled or serving fails.
//
// The first call listens on the configured address. Subsequent calls listen on
// the address that was bound by the first, so that a port chosen by the
// operating system is retained.
func (s *Server) serve(ctx context.Context) error {
	lis, err := s.listen()
	if err != nil {
		return err
	}
	defer lis.Close()

	server := grpc.NewServer()

	messaginggrpc.RegisterEventStreamConsumerAPIServer(server, s.EventStreamConsumerAPI)

	// Stop the server when ctx is canceled, and when serving fails, so that
	// no connections are left open on the previous server when serving is
	// attempted again.
	defer context.AfterFunc(ctx, server.Stop)()
	defer server.Stop()

	s.Logger.DebugContext(
		ctx,
		"listening for gRPC requests",
		slog.String("addr", lis.Addr().String()),
	)

	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("unable to serve gRPC requests: %w", err)
	}

	return nil
}

// listen opens a listener on the configured address the first time it is
// called, and on the address that was bound by the first call thereafter.
//
// The first bind validates the configured address, so any failure is fatal.
// Later binds are to an address that is known to be good, so failures are
// retried.
func (s *Server) listen() (lis net.Listener, err error) {
	listen := s.Listen
	if listen == nil {
		listen = net.Listen
	}

	if addr, ok := s.addr.Load(); ok {
		lis, err = listen("tcp", addr.String())
		if err != nil {
			return nil, err
		}
	} else {
		lis, err = listen("tcp", s.Address)
		if err != nil {
			return nil, supervisor.Fatal(err)
		}

		s.addr.Store(lis.Addr())
	}

	return lis, nil
}
