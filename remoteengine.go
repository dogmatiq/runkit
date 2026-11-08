package runkit

import (
	"context"
	"fmt"
	"slices"

	"google.golang.org/grpc"
)

// RemoteEngineDiscoverer discovers remote engines, which are other engines that
// this engine communicates with, such as to consume their events.
//
// A remote engine is any endpoint that serves Dogma gRPC services. For engines
// that have multiple replicas, it represents the logical engine as a whole,
// rather than any individual replica.
//
// It calls the add function for each remote engine as it becomes available,
// passing a description of the remote engine and a connection to it, and calls
// the remove function that it returns when the remote engine is no longer
// available. The add function panics if the connection is nil.
//
// The description identifies the remote engine in log records, such as by its
// address or service name.
//
// This engine uses the connection until the remove function is called, but
// never closes it. The remove function blocks until the engine has stopped
// using the connection, after which it may be closed. Calling the remove
// function more than once has no effect.
//
// If two remote engines offer the same resource, such as the same event stream,
// this engine chooses one of them to use. There are no guarantees about which
// is chosen, and the choice may change over time even if the set of available
// remote engines remains the same.
//
// The discoverer should run until ctx is canceled. If it returns before then,
// every remote engine that it added is removed and it is called again after a
// delay. The add function must not be called after the discoverer returns.
type RemoteEngineDiscoverer func(
	ctx context.Context,
	add func(description string, conn grpc.ClientConnInterface) (remove func()),
) error

// StaticRemoteEngines returns a [RemoteEngineDiscoverer] that adds each of the
// given connections, and never removes them.
//
// Each remote engine is described by its connection's target, such as when the
// connection is a [grpc.ClientConn], or otherwise by its position among the
// given connections.
//
// It panics if any of the connections is nil.
func StaticRemoteEngines(conns ...grpc.ClientConnInterface) RemoteEngineDiscoverer {
	for _, conn := range conns {
		if conn == nil {
			panic("remote engine connection must not be nil")
		}
	}

	conns = slices.Clone(conns)

	return func(
		ctx context.Context,
		add func(string, grpc.ClientConnInterface) func(),
	) error {
		for i, conn := range conns {
			description := fmt.Sprintf("static remote engine %d", i+1)
			if t, ok := conn.(targetedConn); ok {
				description = t.Target()
			}

			add(description, conn)
		}

		<-ctx.Done()
		return ctx.Err()
	}
}

// targetedConn is a gRPC client connection that reports the target it was
// created with, as [grpc.ClientConn] does.
type targetedConn interface {
	Target() string
}
