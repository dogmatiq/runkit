package observe

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// Application is what the inspector knows about the application that it is
// linked with. The database holds only the keys of handlers and the IDs of
// message types, so their names and types come from here.
type Application struct {
	// Identity is the application's identity.
	Identity *identitypb.Identity

	// Handlers is the application's handlers, ordered by type and then name.
	Handlers []*Handler

	// handlers maps each handler's key to the handler.
	handlers uuidcollection.Map[*Handler]

	// messageTypes maps the ID of each message type that the application
	// routes to the message type.
	messageTypes uuidcollection.Map[*MessageType]
}

// NewApplication returns what the inspector knows about the given
// application, by reading its configuration.
//
// It calls the Configure method of the application and of each handler. It
// returns an error if a call panics, or if the configuration is invalid.
func NewApplication(app dogma.Application) (*Application, error) {
	appConfig, err := xerrors.RecoverT(
		func() (*config.Application, error) {
			c := runtimeconfig.FromApplication(app)
			return c, config.Validate(c, config.ForExecution())
		},
	)
	if err != nil {
		return nil, fmt.Errorf("invalid application configuration: %w", err)
	}

	a := &Application{
		Identity: appConfig.Identity(),
	}

	for _, handlerConfig := range appConfig.Handlers() {
		h := newHandler(handlerConfig, &a.messageTypes)
		a.Handlers = append(a.Handlers, h)
		a.handlers.Set(h.Key, h)
	}

	slices.SortFunc(
		a.Handlers,
		func(x, y *Handler) int {
			if x.Type != y.Type {
				return cmp.Compare(x.Type, y.Type)
			}
			return cmp.Compare(x.Name, y.Name)
		},
	)

	return a, nil
}

// Handler returns the handler with the given key. It returns false if the
// application has no such handler.
func (a *Application) Handler(key *uuidpb.UUID) (*Handler, bool) {
	return a.handlers.Get(key)
}

// HandlersOfType returns the application's handlers of the given type.
func (a *Application) HandlersOfType(t config.HandlerType) []*Handler {
	var handlers []*Handler

	for _, h := range a.Handlers {
		if h.Type == t {
			handlers = append(handlers, h)
		}
	}

	return handlers
}

// MessageType returns the message type with the given ID.
//
// If the application does not route the type, it looks for the type in
// Dogma's message registry. If the type is not there either, the returned
// type has only an ID.
func (a *Application) MessageType(id *uuidpb.UUID) *MessageType {
	if t, ok := a.messageTypes.Get(id); ok {
		return t
	}

	if r, ok := dogma.RegisteredMessageTypeByID(id.AsString()); ok {
		return newMessageType(id, r.GoType())
	}

	return &MessageType{
		ID: id,
	}
}

// DecodeMessage returns the message that the given data represents, as
// stored in an envelope.
//
// It returns an error if the message's type is not in Dogma's message
// registry, or if the message's UnmarshalBinary method fails or panics.
func (a *Application) DecodeMessage(typeID *uuidpb.UUID, data []byte) (dogma.Message, error) {
	r, ok := dogma.RegisteredMessageTypeByID(typeID.AsString())
	if !ok {
		return nil, fmt.Errorf("message type %s is not in the message registry", typeID.AsString())
	}

	return xerrors.RecoverT(
		func() (dogma.Message, error) {
			m := r.New()
			return m, m.UnmarshalBinary(data)
		},
	)
}

// Handler is one of the application's message handlers.
type Handler struct {
	// Key is the handler's identity key.
	Key *uuidpb.UUID

	// Name is the handler's identity name.
	Name string

	// Type is the handler's type.
	Type config.HandlerType

	// IsDisabled is true if the application has disabled the handler.
	IsDisabled bool

	// Concurrency is the handler's concurrency preference. It is only
	// meaningful for integration and projection handlers.
	Concurrency dogma.ConcurrencyPreference

	// Routes is the handler's message routes, ordered by route type and then
	// message type name.
	Routes []Route

	// Aggregate is the handler, if it is an aggregate message handler.
	Aggregate dogma.AggregateMessageHandler[dogma.AggregateRoot]

	// Process is the handler, if it is a process message handler.
	Process dogma.ProcessMessageHandler[dogma.ProcessRoot]

	// Projection is the handler, if it is a projection message handler.
	Projection dogma.ProjectionMessageHandler
}

// newHandler returns the handler that the given configuration describes, and
// adds the message types of its routes to types.
func newHandler(
	handlerConfig config.Handler,
	types *uuidcollection.Map[*MessageType],
) *Handler {
	h := &Handler{
		Key:        handlerConfig.Identity().GetKey(),
		Name:       handlerConfig.Identity().GetName(),
		Type:       handlerConfig.HandlerType(),
		IsDisabled: handlerConfig.IsDisabled(),
	}

	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		h.Aggregate = handlerConfig.Interface()
	case *config.Process:
		h.Process = handlerConfig.Interface()
	case *config.Integration:
		h.Concurrency = handlerConfig.ConcurrencyPreference()
	case *config.Projection:
		h.Projection = handlerConfig.Interface()
		h.Concurrency = handlerConfig.ConcurrencyPreference()
	}

	for route := range handlerConfig.RouteSet().Routes() {
		id := uuidpb.MustParse(route.MessageTypeID.Get())

		t, ok := types.Get(id)
		if !ok {
			t = newMessageType(id, route.MessageType.Get().ReflectType())
			types.Set(id, t)
		}

		h.Routes = append(
			h.Routes,
			Route{
				Type:        route.RouteType.Get(),
				MessageType: t,
			},
		)
	}

	slices.SortFunc(
		h.Routes,
		func(x, y Route) int {
			if x.Type != y.Type {
				return cmp.Compare(x.Type, y.Type)
			}
			return cmp.Compare(x.MessageType.Name, y.MessageType.Name)
		},
	)

	return h
}

// MessageTypeIDs returns the IDs of the message types that the handler
// receives through routes of the given type.
func (h *Handler) MessageTypeIDs(t config.RouteType) []*uuidpb.UUID {
	var ids []*uuidpb.UUID

	for _, r := range h.Routes {
		if r.Type == t {
			ids = append(ids, r.MessageType.ID)
		}
	}

	return ids
}

// HasInstances returns true if the handler has instances, which is true of
// aggregate and process handlers.
func (h *Handler) HasInstances() bool {
	return h.Type == config.AggregateHandlerType || h.Type == config.ProcessHandlerType
}

// MinimizesConcurrency returns true if the handler prefers to minimize
// concurrency, which only integration and projection handlers can prefer.
func (h *Handler) MinimizesConcurrency() bool {
	return h.Concurrency == dogma.MinimizeConcurrency
}

// ConsumesEventStreams returns true if the engine dispatches events from event
// streams to the handler, which is true of process and projection handlers.
func (h *Handler) ConsumesEventStreams() bool {
	return h.Type == config.ProcessHandlerType || h.Type == config.ProjectionHandlerType
}

// StreamWorkloadKey returns the rendezvous key of the workload of consuming
// the event stream with the given ID on behalf of the handler.
//
// It matches the keys that the engine's process and projection event sinks
// use.
func (h *Handler) StreamWorkloadKey(streamID *uuidpb.UUID) *uuidpb.UUID {
	// A projection that minimizes concurrency consumes every stream on one
	// node.
	if h.Type == config.ProjectionHandlerType && h.MinimizesConcurrency() {
		return h.Key
	}

	return uuidpb.Derive(h.Key, streamID.AsString())
}

// Route is a message route of a handler.
type Route struct {
	// Type is the type of route, such as "handles-command".
	Type config.RouteType

	// MessageType is the type of the routed messages.
	MessageType *MessageType
}

// MessageType is a type of message.
type MessageType struct {
	// ID is the message type's ID.
	ID *uuidpb.UUID

	// Name is the name of the Go type that implements the message, or empty
	// if the type is not known.
	Name string

	// Kind is the kind of message. It is only meaningful if Name is not empty.
	Kind message.Kind
}

// newMessageType returns the message type with the given ID, implemented by
// the given Go type.
func newMessageType(id *uuidpb.UUID, goType reflect.Type) *MessageType {
	name := goType.String()
	if goType.Kind() == reflect.Pointer {
		name = goType.Elem().String()
	}

	return &MessageType{
		ID:   id,
		Name: name,
		Kind: message.TypeFromReflect(goType).Kind(),
	}
}
