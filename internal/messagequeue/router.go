package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Router chooses the queue for each new message.
//
// It chooses the first queue only. A stored queue ID can be changed at any
// time, and later messages for a process instance follow its stored queue.
type Router struct {
	// NodeID is the ID of the local node.
	NodeID *uuidpb.UUID

	// Aggregates maps the ID of each command type that an aggregate handles to
	// the route to that aggregate.
	Aggregates *uuidcollection.Map[AggregateRoute]
}

// AggregateRoute is the aggregate that handles a particular command type.
//
// [Router.RouteCommandToQueue] uses the handler to find the instance that a
// command targets, and the key to find that instance's event stream.
type AggregateRoute struct {
	// Key is the aggregate's identity key.
	Key *uuidpb.UUID

	// Handler is the aggregate's message handler.
	Handler dogma.AggregateMessageHandler[dogma.AggregateRoot]
}

// RouteCommandToQueue returns the ID of the queue for the command in the given
// envelope.
//
// A command that an aggregate handles is added to the queue whose ID is the ID
// of the target instance's event stream, so commands for instances whose
// events share a stream are delivered by the same node. Any other command,
// including one for an instance that isn't bound to a stream yet, is added to
// the local node's queue. Routing a command never creates or binds an instance.
func (r *Router) RouteCommandToQueue(
	ctx context.Context,
	q xsql.Querier,
	env *envelopepb.Envelope,
) (*uuidpb.UUID, error) {
	aggregate, ok := r.Aggregates.Get(env.GetBody().GetMessage().GetTypeId())
	if !ok {
		return r.NodeID, nil
	}

	instanceID, err := xerrors.RecoverT(
		func() (string, error) {
			command, err := envelope.Unpack[dogma.Command](env)
			if err != nil {
				return "", err
			}

			return aggregate.Handler.RouteCommandToInstance(command), nil
		},
	)

	// A command that can't be routed now can't be routed when it's delivered
	// either, and delivery reports why. Until then, it's added to the local
	// node's queue.
	if err != nil {
		return r.NodeID, nil
	}

	if instanceID == "" {
		return r.NodeID, nil
	}

	row := q.QueryRowContext(
		ctx,
		`SELECT stream_id
		FROM aggregate.instances
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(aggregate.Key),
		instanceID,
	)

	streamID := &uuidpb.UUID{}

	if err := row.Scan(xsql.UUID(streamID)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return r.NodeID, nil
		}

		return nil, fmt.Errorf("unable to load aggregate instance's event stream: %w", err)
	}

	// A NULL stream_id scans as the nil UUID, whose bits are all zero.
	if streamID.Equal(nil) {
		return r.NodeID, nil
	}

	return streamID, nil
}

// RouteDeadlinesToQueue returns the ID of the queue for the deadlines of the
// given process instance.
//
// It chooses one queue for all of the instance's deadlines. The instance is
// bound to the queue when it's first stored, and every deadline it schedules
// is added to that queue, even if the router would now choose another. Every
// instance's deadlines are added to the local node's queue.
func (r *Router) RouteDeadlinesToQueue(
	ctx context.Context,
	q xsql.Querier,
	handlerKey *uuidpb.UUID,
	instanceID string,
) (*uuidpb.UUID, error) {
	return r.NodeID, nil
}
