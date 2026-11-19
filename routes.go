package runkit

import (
	"reflect"
	"uuid"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/uuidset"
)

// executableCommandTypes returns the set of command types that the engine
// accepts for execution, which are those that the given application handles.
func executableCommandTypes(appConfig *config.Application) map[reflect.Type]struct{} {
	inboundCommandRoutes := appConfig.
		RouteSet().
		Filter(
			config.FilterByMessageDirection(config.InboundDirection),
			config.FilterByMessageKind(message.CommandKind),
		).
		Routes()

	types := map[reflect.Type]struct{}{}

	for route := range inboundCommandRoutes {
		typ := route.MessageType.Get().ReflectType()
		types[typ] = struct{}{}
	}

	return types
}

// commandHandlersForQueueRouter returns the handler of each command type,
// keyed by the type's ID, for [messagequeue.Router] to choose the queue for
// each command.
func commandHandlersForQueueRouter(appConfig *config.Application) map[uuid.UUID]messagequeue.CommandHandler {
	handlers := map[uuid.UUID]messagequeue.CommandHandler{}

	for _, handlerConfig := range appConfig.Handlers() {
		var handler messagequeue.CommandHandler

		switch handlerConfig := handlerConfig.(type) {
		case *config.Aggregate:
			handler = messagequeue.CommandHandler{
				Key:       handlerConfig.Identity().GetKey().AsStd(),
				Aggregate: handlerConfig.Interface(),
			}
		case *config.Integration:
			handler = messagequeue.CommandHandler{
				Key:                   handlerConfig.Identity().GetKey().AsStd(),
				ConcurrencyPreference: handlerConfig.ConcurrencyPreference(),
			}
		default:
			continue
		}

		commandTypeIDs := inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind)

		for commandTypeID := range commandTypeIDs.All() {
			handlers[commandTypeID] = handler
		}
	}

	return handlers
}

// consumedEventTypeIDs returns the message type IDs of every event type that
// is routed to a non-disabled handler within the given application.
func consumedEventTypeIDs(appConfig *config.Application) *uuidset.Set {
	messageTypeIDs := &uuidset.Set{}

	for _, handlerConfig := range appConfig.Handlers() {
		if handlerConfig.IsDisabled() {
			continue
		}

		eventTypeIDs := inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind)

		for id := range eventTypeIDs.All() {
			messageTypeIDs.Add(id)
		}
	}

	return messageTypeIDs
}

// inboundMessageTypeIDsForHandler returns the message type IDs of all inbound
// messages of the given kind routed to the given handler.
func inboundMessageTypeIDsForHandler(
	handlerConfig config.Handler,
	messageKind message.Kind,
) *uuidset.Set {
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageKind(messageKind)).
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	messageTypeIDs := &uuidset.Set{}

	for route := range inboundRoutes {
		messageTypeID := route.MessageTypeID.Get().AsStd()
		messageTypeIDs.Add(messageTypeID)
	}

	return messageTypeIDs
}

// outboundMessageTypesForHandler returns the reflect.Types of all outbound
// messages routed from the given handler.
func outboundMessageTypesForHandler(handlerConfig config.Handler) map[reflect.Type]struct{} {
	outboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageDirection(config.OutboundDirection)).
		Routes()

	types := map[reflect.Type]struct{}{}

	for route := range outboundRoutes {
		types[route.MessageType.Get().ReflectType()] = struct{}{}
	}

	return types
}
