package runkit

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// runHandler starts the components that manage the given handler, and adds
// its sink, if it has one, to e.queueSinks for the queue consumer.
func (e *Engine) runHandler(ctx context.Context, handlerConfig config.Handler) {
	engineLogger := newLoggerForHandler(e.engineLogger, handlerConfig)
	appLogger := newLoggerForHandler(e.appLogger, handlerConfig)

	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		e.addQueueSink(
			handlerConfig,
			&aggregate.CommandSink{
				Packer:               e.packer,
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
			},
		)

	case *config.Integration:
		e.addQueueSink(
			handlerConfig,
			&integration.CommandSink{
				Packer:                e.packer,
				OutboundMessageTypes:  outboundMessageTypesForHandler(handlerConfig),
				Handler:               handlerConfig.Interface(),
				Identity:              handlerConfig.Identity(),
				ConcurrencyPreference: handlerConfig.ConcurrencyPreference(),
				HandleTimeout:         e.handleTimeout,
			},
		)

	case *config.Process:
		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &process.EventSink{
				DB:                   e.db,
				Packer:               e.packer,
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				QueueRouter:          e.queueRouter,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				EventTypes:           inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				HandleTimeout:        e.handleTimeout,
				RouteTimeout:         e.routeTimeout,
				EngineLogger:         engineLogger,
				AppLogger:            appLogger,
			},
			EngineFailurePolicy:   e.engineFailurePolicy,
			DispatchFailurePolicy: e.eventStreamDispatchFailurePolicy,
			EngineLogger:          engineLogger,
		}

		e.group.Go(func() error {
			return consumer.Run(ctx)
		})

		e.addQueueSink(
			handlerConfig,
			&process.DeadlineSink{
				Packer:               e.packer,
				OutboundMessageTypes: outboundMessageTypesForHandler(handlerConfig),
				QueueRouter:          e.queueRouter,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				HandleTimeout:        e.handleTimeout,
			},
		)

	case *config.Projection:
		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &projection.EventSink{
				Handler:               handlerConfig.Interface(),
				Identity:              handlerConfig.Identity(),
				ConcurrencyPreference: handlerConfig.ConcurrencyPreference(),
				EventTypes:            inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				HandleTimeout:         e.handleTimeout,
				RouteTimeout:          e.routeTimeout,
				EngineLogger:          engineLogger,
				AppLogger:             appLogger,
			},
			EngineFailurePolicy:   e.engineFailurePolicy,
			DispatchFailurePolicy: e.eventStreamDispatchFailurePolicy,
			EngineLogger:          engineLogger,
		}

		compactor := &projection.Compactor{
			DB:                      e.db,
			Handler:                 handlerConfig.Interface(),
			Identity:                handlerConfig.Identity(),
			Interval:                e.compactInterval,
			Timeout:                 e.compactTimeout,
			CompactionFailurePolicy: e.projectionCompactionFailurePolicy,
			EngineLogger:            engineLogger,
			AppLogger:               appLogger,
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
		xslog.HandlerIdentity(
			handlerConfig.Identity(),
			handlerConfig.HandlerType(),
		),
	)
}

// addQueueSink adds the given sink to e.queueSinks, for each command and
// deadline type handled by the given handler.
func (e *Engine) addQueueSink(handlerConfig config.Handler, sink messagequeue.Sink) {
	for _, kind := range []message.Kind{message.CommandKind, message.DeadlineKind} {
		for typeID := range inboundMessageTypeIDsForHandler(handlerConfig, kind).All() {
			e.queueSinks.Set(typeID, sink)
		}
	}
}
