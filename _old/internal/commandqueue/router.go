package commandqueue

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Router is a function that routes a command to the appropriate handler and
// optional aggregate instance ID.
type Router func(dogma.Command) (
	handlerKey *uuidpb.UUID,
	aggregateInstanceID *string,
	ok bool,
)

// NewRouter creates a new command router for the given application.
func NewRouter(app *config.Application) Router {
	commandRoutes := app.
		RouteSet().
		Filter(config.FilterByRouteType(config.HandlesCommandRouteType)).
		Routes()

	routers := map[reflect.Type]Router{}

	for route, handler := range commandRoutes {
		key := handler.Identity().GetKey()
		typ := route.MessageType.Get().ReflectType()

		switch handler := handler.(type) {
		case *config.Aggregate:
			impl := handler.Interface()
			routers[typ] = func(m dogma.Command) (*uuidpb.UUID, *string, bool) {
				return key, new(impl.RouteCommandToInstance(m)), true
			}
		case *config.Integration:
			routers[typ] = func(dogma.Command) (*uuidpb.UUID, *string, bool) {
				return handler.Identity().GetKey(), nil, true
			}
		}
	}

	return func(c dogma.Command) (*uuidpb.UUID, *string, bool) {
		typ := reflect.TypeOf(c)

		if router, ok := routers[typ]; ok {
			return router(c)
		}

		return nil, nil, false
	}
}

func Reroute(
	ctx context.Context,
	tx *sql.Tx,
	router Router,
	commandEnvelope *envelopepb.Envelope,
) error {
	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		return fmt.Errorf("unable to unpack command: %w", err)
	}

	handlerKey, aggregateInstanceID, ok := router(command)
	if !ok {
		return Backoff(
			ctx,
			tx,
			commandEnvelope.GetBody().GetMessageId(),
		)
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE pending_commands SET
			handler_key = $1,
			aggregate_instance_id = $2
		WHERE message_id = $3`,
		database.MarshalUUID(handlerKey),
		aggregateInstanceID,
		database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
	); err != nil {
		return fmt.Errorf("unable to update pending command: %w", err)
	}

	return nil
}
