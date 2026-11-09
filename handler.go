package runkit

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/semaphore"
)

// runHandler runs the components that manage the given handler.
func (e *Engine) runHandler(ctx context.Context, handlerConfig config.Handler) {
	const (
		backoffBase  = 10 * time.Millisecond
		backoffCap   = 5 * time.Minute
		pollInterval = 25 * time.Millisecond
	)

	engineLogger := newLoggerForHandler(e.engineLogger, handlerConfig)
	appLogger := newLoggerForHandler(e.appLogger, handlerConfig)
	workers := workerCountForHandler(handlerConfig)

	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		pump := &messagepump.MessagePump{
			Driver: &aggregate.CommandPump{
				DB:                   e.db,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				Packer:               e.packer,
				CommandTypeIDs:       inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				AppLogger:            appLogger,
			},
			DB:           e.db,
			Workers:      workers,
			PollInterval: pollInterval,
			BackoffBase:  backoffBase,
			BackoffCap:   backoffCap,
			EngineLogger: engineLogger,
		}

		e.group.Go(func() error {
			return pump.Run(ctx)
		})

	case *config.Integration:
		pump := &messagepump.MessagePump{
			Driver: &integration.CommandPump{
				DB:                   e.db,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				Concurrency:          handlerConfig.ConcurrencyPreference(),
				Packer:               e.packer,
				CommandTypeIDs:       inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				HandleTimeout:        e.handleTimeout,
				AppLogger:            appLogger,
			},
			DB:           e.db,
			Workers:      workers,
			PollInterval: pollInterval,
			BackoffBase:  backoffBase,
			BackoffCap:   backoffCap,
			EngineLogger: engineLogger,
		}

		e.group.Go(func() error {
			return pump.Run(ctx)
		})

	case *config.Process:
		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &process.Sink{
				DB:                   e.db,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				Packer:               e.packer,
				QueueRouter:          e.queueRouter,
				EventTypes:           inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				HandleTimeout:        e.handleTimeout,
				RouteTimeout:         e.routeTimeout,
				EngineLogger:         engineLogger,
				AppLogger:            appLogger,
			},
			EngineLogger: engineLogger,
		}

		deadlinePump := &messagepump.MessagePump{
			Driver: &process.DeadlinePump{
				DB:                   e.db,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				Packer:               e.packer,
				QueueRouter:          e.queueRouter,
				DeadlineTypeIDs:      inboundMessageTypeIDsForHandler(handlerConfig, message.DeadlineKind),
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				HandleTimeout:        e.handleTimeout,
				AppLogger:            appLogger,
			},
			DB:           e.db,
			Workers:      workers,
			PollInterval: pollInterval,
			BackoffBase:  backoffBase,
			BackoffCap:   backoffCap,
			EngineLogger: engineLogger,
		}

		e.group.Go(func() error {
			return consumer.Run(ctx)
		})

		e.group.Go(func() error {
			return deadlinePump.Run(ctx)
		})

	case *config.Projection:
		var handlerLock *semaphore.Weighted
		if handlerConfig.ConcurrencyPreference() == dogma.MinimizeConcurrency {
			handlerLock = semaphore.NewWeighted(1)
		}

		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &projection.Sink{
				Handler:       handlerConfig.Interface(),
				Identity:      handlerConfig.Identity(),
				Concurrency:   handlerConfig.ConcurrencyPreference(),
				EventTypes:    inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				HandlerLock:   handlerLock,
				HandleTimeout: e.handleTimeout,
				RouteTimeout:  e.routeTimeout,
				EngineLogger:  engineLogger,
				AppLogger:     appLogger,
			},
			EngineLogger: engineLogger,
		}

		compactor := &projection.Compactor{
			DB:           e.db,
			Handler:      handlerConfig.Interface(),
			Identity:     handlerConfig.Identity(),
			Interval:     e.compactInterval,
			Timeout:      e.compactTimeout,
			EngineLogger: engineLogger,
			AppLogger:    appLogger,
		}

		e.group.Go(func() error {
			return consumer.Run(ctx)
		})

		e.group.Go(func() error {
			return compactor.Run(ctx)
		})

	default:
		panic(fmt.Sprintf("unsupported handler type: %T", handlerConfig))
	}
}

// newLoggerForHandler returns the given logger with the attributes that
// identify the given handler.
func newLoggerForHandler(
	logger *slog.Logger,
	handlerConfig config.Handler,
) *slog.Logger {
	return logger.With(
		xslog.Identity(
			"handler",
			handlerConfig.Identity(),
			slog.String("type", handlerConfig.HandlerType().String()),
		),
	)
}

// workerCountForHandler returns the number of worker goroutines to run for the
// given handler's message pump.
func workerCountForHandler(handlerConfig config.Handler) int {
	switch handlerConfig := handlerConfig.(type) {
	case *config.Integration:
		if handlerConfig.ConcurrencyPreference() == dogma.MinimizeConcurrency {
			return 1
		}
	}

	return 10 * runtime.GOMAXPROCS(0)
}
