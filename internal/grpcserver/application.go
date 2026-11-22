package grpcserver

import (
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/protobuf/configpb"
)

// ApplicationToProto returns the protobuf form of an application's
// configuration.
//
// It leaves out the application's version, and whether each projection handler
// supports compaction and reset, because the engine can't tell any of them.
//
// It panics if the configuration is not valid for execution.
func ApplicationToProto(app *config.Application) *configpb.Application {
	messages := map[string]configpb.MessageKind{}

	for t := range app.RouteSet().MessageTypes() {
		messages[string(t.Name())] = messageKindToProto(t.Kind())
	}

	var handlers []*configpb.Handler

	for _, h := range app.Handlers() {
		handlers = append(handlers, handlerToProto(h))
	}

	return configpb.NewApplicationBuilder().
		WithIdentity(app.Identity()).
		WithGoType(goTypeNameOf(app)).
		WithHandlers(handlers).
		WithMessages(messages).
		Build()
}

// handlerToProto returns the protobuf form of a handler's configuration.
func handlerToProto(h config.Handler) *configpb.Handler {
	messages := map[string]*configpb.MessageUsage{}

	for t, direction := range h.RouteSet().MessageTypes() {
		messages[string(t.Name())] = configpb.NewMessageUsageBuilder().
			WithIsConsumed(direction.Has(config.InboundDirection)).
			WithIsProduced(direction.Has(config.OutboundDirection)).
			Build()
	}

	b := configpb.NewHandlerBuilder().
		WithIdentity(h.Identity()).
		WithGoType(goTypeNameOf(h)).
		WithType(handlerTypeToProto(h.HandlerType())).
		WithMessages(messages).
		WithIsDisabled(h.IsDisabled())

	// Only integration and projection handlers have a concurrency preference.
	switch h := h.(type) {
	case *config.Integration:
		b.WithConcurrencyPreference(concurrencyPreferenceToProto(h.ConcurrencyPreference()))
	case *config.Projection:
		b.WithConcurrencyPreference(concurrencyPreferenceToProto(h.ConcurrencyPreference()))
	}

	return b.Build()
}

// goTypeNameOf returns the name of the Go type that implements the entity, or
// an empty string if the configuration doesn't say.
func goTypeNameOf(e config.Entity) string {
	name, _ := e.EntityProperties().TypeName.TryGet()
	return name
}

func messageKindToProto(k message.Kind) configpb.MessageKind {
	return message.MapByKind(
		k,
		configpb.MessageKind_MESSAGE_KIND_COMMAND,
		configpb.MessageKind_MESSAGE_KIND_EVENT,
		configpb.MessageKind_MESSAGE_KIND_DEADLINE,
	)
}

func handlerTypeToProto(t config.HandlerType) configpb.HandlerType {
	return config.MapByHandlerType(
		t,
		configpb.HandlerType_HANDLER_TYPE_AGGREGATE,
		configpb.HandlerType_HANDLER_TYPE_PROCESS,
		configpb.HandlerType_HANDLER_TYPE_INTEGRATION,
		configpb.HandlerType_HANDLER_TYPE_PROJECTION,
	)
}

func concurrencyPreferenceToProto(p dogma.ConcurrencyPreference) configpb.ConcurrencyPreference {
	if p == dogma.MinimizeConcurrency {
		return configpb.ConcurrencyPreference_CONCURRENCY_PREFERENCE_MINIMIZE
	}

	return configpb.ConcurrencyPreference_CONCURRENCY_PREFERENCE_MAXIMIZE
}
