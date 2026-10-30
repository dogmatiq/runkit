package runkit

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
)

// EngineOption is a function that configures an [Engine].
type EngineOption func(*Engine)

// WithLogger is an [EngineOption] that sets the logger the engine uses.
//
// If it is not provided [slog.Default] is used.
func WithLogger(logger *slog.Logger) EngineOption {
	return func(e *Engine) {
		e.logger = logger
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
