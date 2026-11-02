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
// It calls add for each remote engine as it becomes available, and calls the
// returned remove function when it is no longer available, until ctx is
// canceled.
//
// The engine uses conn until remove is called, but never closes it. remove
// returns once the engine has stopped using conn, after which conn may be
// closed. Calling it more than once has no effect.
//
// Each remote engine must be added once. Otherwise, the same resources, such
// as event streams, are offered by more than one remote engine, which stops
// this engine with an error.
//
// add must not be called after the discoverer returns, including from
// goroutines that it started. Once it returns, every remote engine that it
// added is removed. If it returns an error other than ctx's, it is called again
// after a delay.
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
