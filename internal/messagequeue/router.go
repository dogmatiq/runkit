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

	// CommandHandlers maps the ID of each command type to the handler for
	// commands of that type.
	CommandHandlers *uuidcollection.Map[CommandHandler]
}

// CommandHandler is the handler for a particular command type, with the values
// that [Router.RouteCommandToQueue] reads from its configuration.
type CommandHandler struct {
	// Key is the handler's identity key.
	Key *uuidpb.UUID

	// Aggregate is the application's aggregate message handler, or nil if an
	// integration handles the command type.
	Aggregate dogma.AggregateMessageHandler[dogma.AggregateRoot]

	// Concurrency is the handler's concurrency preference.
	//
	// It's [dogma.MaximizeConcurrency], the zero value, for an aggregate,
	// which has no concurrency preference.
	Concurrency dogma.ConcurrencyPreference
}

// RouteCommandToQueue returns the ID of the queue for the command in the given
// envelope.
//
// The ID that it returns depends on the handler for the command's type:
//
//   - For an aggregate, it's the ID of the target instance's event stream, so
//     one node delivers the commands for all of the instances whose events
//     share that stream.
//   - For an integration that prefers [dogma.MinimizeConcurrency], it's the
//     integration's identity key, so one node delivers all of the
//     integration's commands.
//   - Otherwise, it's the ID of the local node. That includes a command for an
//     aggregate instance that isn't bound to an event stream yet.
//
// While nodes see different live nodes, two of them may briefly both deliver
// the commands in one queue, as described by [cluster.FindWorkloadOwner].
//
// Routing a command never creates or binds an instance.
func (r *Router) RouteCommandToQueue(
	ctx context.Context,
	q xsql.Querier,
	env *envelopepb.Envelope,
) (*uuidpb.UUID, error) {
	handler, ok := r.CommandHandlers.Get(env.GetBody().GetMessage().GetTypeId())

	// No handler is known for the command's type.
	if !ok {
		return r.NodeID, nil
	}

	if handler.Aggregate == nil {
		// One node delivers all of the commands of an integration that
		// minimizes concurrency.
		if handler.Concurrency == dogma.MinimizeConcurrency {
			return handler.Key, nil
		}

		// Any other integration's commands go to the local node's queue.
		return r.NodeID, nil
	}

	instanceID, err := xerrors.RecoverT(
		func() (string, error) {
			command, err := envelope.Unpack[dogma.Command](env)
			if err != nil {
				return "", err
			}

			return handler.Aggregate.RouteCommandToInstance(command), nil
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
		xsql.UUID(handler.Key),
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
