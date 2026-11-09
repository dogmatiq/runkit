package runkit

import (
	"reflect"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
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

// aggregatesForQueueRouter returns the routes to the aggregates that the queue
// router uses to route commands, keyed by the ID of each command type that
// they handle.
func aggregatesForQueueRouter(appConfig *config.Application) *uuidcollection.Map[messagequeue.AggregateRoute] {
	aggregates := &uuidcollection.Map[messagequeue.AggregateRoute]{}

	for _, handlerConfig := range appConfig.Handlers() {
		aggregate, ok := handlerConfig.(*config.Aggregate)
		if !ok {
			continue
		}

		commandTypeIDs := inboundMessageTypeIDsForHandler(aggregate, message.CommandKind)

		for commandTypeID := range commandTypeIDs.All() {
			aggregates.Set(
				commandTypeID,
				messagequeue.AggregateRoute{
					Key:     aggregate.Identity().GetKey(),
					Handler: aggregate.Interface(),
				},
			)
		}
	}

	return aggregates
}

// consumedEventTypeIDs returns the message type IDs of every event type that
// is routed to a non-disabled handler within the given application.
func consumedEventTypeIDs(appConfig *config.Application) *uuidcollection.Set {
	messageTypeIDs := uuidcollection.NewSet()

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
) *uuidcollection.Set {
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageKind(messageKind)).
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	messageTypeIDs := &uuidcollection.Set{}

	for route := range inboundRoutes {
		messageTypeID := uuidpb.MustParse(route.MessageTypeID.Get())
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
