package runkit

import (
	"context"
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
// passing a connection to the remote engine, and calls the remove function that
// it returns when the remote engine is no longer available. The add function
// panics if the connection is nil.
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
	add func(conn grpc.ClientConnInterface) (remove func()),
) error

// StaticRemoteEngines returns a [RemoteEngineDiscoverer] that adds each of the
// given connections, and never removes them.
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
		add func(grpc.ClientConnInterface) func(),
	) error {
		for _, conn := range conns {
			add(conn)
		}

		<-ctx.Done()
		return ctx.Err()
	}
}
