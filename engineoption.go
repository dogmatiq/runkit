package runkit

import (
	"log/slog"
	"time"
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

// WithListenAddress is an [EngineOption] that sets the address the engine
// listens on for gRPC requests from other engines.
//
// If it is not provided [DefaultListenAddr] is used.
func WithListenAddress(addr string) EngineOption {
	return func(e *Engine) {
		e.listenAddress = addr
	}
}

// WithProjectionCompactInterval is an [EngineOption] that sets the minimum time
// between projection compaction attempts.
//
// If it is not provided [DefaultProjectionCompactInterval] is used.
func WithProjectionCompactInterval(interval time.Duration) EngineOption {
	return func(e *Engine) {
		e.compactInterval = interval
	}
}
