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
// It calls available for each remote engine as it becomes available, and calls
// the returned unavailable function when it is no longer available, until ctx
// is canceled.
//
// The engine uses conn until unavailable is called, but never closes it.
// unavailable returns once the engine has stopped using conn, after which conn
// may be closed. Calling it more than once has no effect.
//
// Each remote engine must be reported once. Otherwise, the same resources, such
// as event streams, are offered by more than one remote engine, which stops
// this engine with an error.
//
// Once the discoverer returns, every remote engine that it reported available
// is treated as unavailable. If it returns an error other than ctx's, it is
// called again after a delay.
type RemoteEngineDiscoverer func(
	ctx context.Context,
	available func(conn grpc.ClientConnInterface) (unavailable func()),
) error

// StaticRemoteEngines returns a [RemoteEngineDiscoverer] that permanently
// reports each of the given connections as available.
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
		available func(grpc.ClientConnInterface) func(),
	) error {
		for _, conn := range conns {
			available(conn)
		}

		<-ctx.Done()
		return ctx.Err()
	}
}
