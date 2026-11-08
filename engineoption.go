package runkit

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

const (
	// DefaultProjectionCompactInterval is the default minimum time between
	// successful projection compactions.
	DefaultProjectionCompactInterval = 6 * time.Hour

	// DefaultProjectionCompactTimeout is the default maximum time a projection
	// is given to compact its data in a single attempt.
	DefaultProjectionCompactTimeout = 10 * time.Minute

	// DefaultHandleTimeout is the default maximum time an application message
	// handler is given to handle a single message.
	DefaultHandleTimeout = 1 * time.Minute

	// DefaultRouteTimeout is the default maximum time an application message
	// handler is given to decide where a message is delivered, or where
	// consumption of an event stream resumes.
	DefaultRouteTimeout = 10 * time.Second
)

// EngineOption is a function that configures an [Engine].
type EngineOption func(*Engine)

// WithLogger is an [EngineOption] that sets the logger the engine uses.
//
// If it is not provided [slog.Default] is used.
func WithLogger(logger *slog.Logger) EngineOption {
	return func(e *Engine) {
		e.engineLogger = logger
		e.appLogger = logger
	}
}

// WithListenAddress is an [EngineOption] that makes the engine listen on addr
// for gRPC requests from other engines, such as requests to consume the
// application's events.
//
// If it is not provided the engine does not listen for gRPC requests. It panics
// if addr is empty.
//
// The options are used to create the gRPC server. As with any gRPC server, it
// is insecure by default: unless transport credentials are supplied using
// [grpc.Creds], connections are neither encrypted nor authenticated. Any client
// that is able to connect can consume all of the application's events, so the
// credentials should authenticate clients, for example by using mutual TLS:
//
//	runkit.WithListenAddress(
//		":50555",
//		grpc.Creds(
//			credentials.NewTLS(&tls.Config{
//				Certificates: []tls.Certificate{serverCert},
//				ClientCAs:    clientCAs,
//				ClientAuth:   tls.RequireAndVerifyClientCert,
//			}),
//		),
//	)
func WithListenAddress(addr string, options ...grpc.ServerOption) EngineOption {
	if addr == "" {
		panic("listen address must not be empty")
	}

	return func(e *Engine) {
		e.listenAddress = addr
		e.grpcOptions = options
	}
}

// WithRemoteEngineDiscoverer is an [EngineOption] that adds d to the
// discoverers that the engine uses to find remote engines.
//
// It may be provided more than once, adding a discoverer each time. Use
// [StaticRemoteEngines] for a fixed set of remote engines. It panics if d is
// nil.
func WithRemoteEngineDiscoverer(d RemoteEngineDiscoverer) EngineOption {
	if d == nil {
		panic("remote engine discoverer must not be nil")
	}

	return func(e *Engine) {
		e.remoteEngineDiscoverers = append(e.remoteEngineDiscoverers, d)
	}
}

// WithProjectionCompactInterval is an [EngineOption] that sets the minimum time
// between successful projection compactions.
//
// If it is not provided [DefaultProjectionCompactInterval] is used. It panics
// if interval is not positive.
func WithProjectionCompactInterval(interval time.Duration) EngineOption {
	if interval <= 0 {
		panic("projection compaction interval must be positive")
	}

	return func(e *Engine) {
		e.compactInterval = interval
	}
}

// WithProjectionCompactTimeout is an [EngineOption] that sets the maximum time a
// projection message handler is given to compact its data in a single attempt.
//
// The timeout is an upper limit. A handler can impose a shorter deadline of its
// own on the context that it is given, but it can not extend this one. A
// handler should compact incrementally so that it makes progress even when it
// reaches the deadline, because a failed attempt is retried without waiting for
// the compaction interval.
//
// If it is not provided [DefaultProjectionCompactTimeout] is used. It panics if
// timeout is not positive.
func WithProjectionCompactTimeout(timeout time.Duration) EngineOption {
	if timeout <= 0 {
		panic("projection compaction timeout must be positive")
	}

	return func(e *Engine) {
		e.compactTimeout = timeout
	}
}

// WithHandleTimeout is an [EngineOption] that sets the maximum time an
// application message handler is given to handle a single message.
//
// It applies to each call to a process message handler's HandleEvent() and
// HandleDeadline() methods, an integration message handler's HandleCommand()
// method, and a projection message handler's HandleEvent() method. A call that
// reaches the deadline is treated like any other failure, and the message is
// retried later.
//
// The timeout is an upper limit. A handler can impose a shorter deadline of its
// own on the context that it is given, but it can not extend this one.
//
// If it is not provided [DefaultHandleTimeout] is used. It panics if timeout is
// not positive.
func WithHandleTimeout(timeout time.Duration) EngineOption {
	if timeout <= 0 {
		panic("handle timeout must be positive")
	}

	return func(e *Engine) {
		e.handleTimeout = timeout
	}
}

// WithRouteTimeout is an [EngineOption] that sets the maximum time an
// application message handler is given to decide where a message is delivered,
// or where consumption of an event stream resumes.
//
// It applies to each call to a process message handler's RouteEventToInstance()
// method and a projection message handler's CheckpointOffset() method. A call
// that reaches the deadline is treated like any other failure, and is retried
// later.
//
// The timeout is an upper limit. A handler can impose a shorter deadline of its
// own on the context that it is given, but it can not extend this one.
//
// If it is not provided [DefaultRouteTimeout] is used. It panics if timeout is
// not positive.
func WithRouteTimeout(timeout time.Duration) EngineOption {
	if timeout <= 0 {
		panic("route timeout must be positive")
	}

	return func(e *Engine) {
		e.routeTimeout = timeout
	}
}
