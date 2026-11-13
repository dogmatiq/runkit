package observe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// countLimit is the largest number of rows that the inspector counts. A count
// that reaches it means "at least this many".
const countLimit = 10_000

// AggregateInstance is an instance of an aggregate handler.
type AggregateInstance struct {
	// HandlerKey is the key of the instance's handler.
	HandlerKey *uuidpb.UUID

	// ID is the instance's ID.
	ID string

	// StreamID is the ID of the event stream that the instance's events are
	// appended to, or nil if the instance has never recorded an event.
	StreamID *uuidpb.UUID

	// Snapshot is the most recent snapshot of the instance's root, as
	// returned by its MarshalBinary method, or nil if there is none.
	Snapshot []byte

	// SnapshotOffset is the offset of the last event that Snapshot includes.
	// It is only meaningful if Snapshot is not nil.
	SnapshotOffset uint64
}

// ProcessInstance is an instance of a process handler.
type ProcessInstance struct {
	// HandlerKey is the key of the instance's handler.
	HandlerKey *uuidpb.UUID

	// ID is the instance's ID.
	ID string

	// HasEnded is true if the process instance has ended.
	HasEnded bool

	// State is the binary representation of the instance's root, as returned
	// by its MarshalBinary method, or nil if the instance has ended or has no
	// state.
	State []byte

	// DeadlineQueueID is the ID of the queue for the instance's deadlines, or
	// nil if the instance has none.
	DeadlineQueueID *uuidpb.UUID
}

// InstanceCount is the number of instances of a handler.
type InstanceCount struct {
	// Count is the number of instances, up to [countLimit].
	Count int

	// IsAtLeast is true if there are at least Count instances, because the
	// count stopped at the limit.
	IsAtLeast bool
}

// AggregateInstances returns the instances of the aggregate handler with the
// given key whose IDs sort after the given ID, ordered by ID, up to the given
// limit.
func (d *Database) AggregateInstances(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	after string,
	limit int,
) ([]AggregateInstance, error) {
	return d.aggregateInstances(
		ctx,
		`SELECT i.handler_key, i.instance_id, i.stream_id, i.snapshot, i.snapshot_offset
		FROM aggregate.instances AS i
		WHERE i.handler_key = $1
			AND i.instance_id > $2
		ORDER BY i.instance_id
		LIMIT $3`,
		xsql.UUID(handlerKey),
		after,
		limit,
	)
}

// AggregateInstance returns the aggregate instance with the given handler key
// and ID. It returns false if there is no such instance.
func (d *Database) AggregateInstance(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	id string,
) (AggregateInstance, bool, error) {
	instances, err := d.aggregateInstances(
		ctx,
		`SELECT i.handler_key, i.instance_id, i.stream_id, i.snapshot, i.snapshot_offset
		FROM aggregate.instances AS i
		WHERE i.handler_key = $1
			AND i.instance_id = $2`,
		xsql.UUID(handlerKey),
		id,
	)
	if err != nil || len(instances) == 0 {
		return AggregateInstance{}, false, err
	}

	return instances[0], true, nil
}

// CountAggregateInstances returns the number of instances of the aggregate
// handler with the given key.
func (d *Database) CountAggregateInstances(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
) (InstanceCount, error) {
	return d.count(
		ctx,
		`SELECT count(*) FROM (
			SELECT 1
			FROM aggregate.instances AS i
			WHERE i.handler_key = $1
			LIMIT $2
		) AS x`,
		xsql.UUID(handlerKey),
		countLimit,
	)
}

// ProcessInstances returns the instances of the process handler with the given
// key whose IDs sort after the given ID, ordered by ID, up to the given
// limit.
func (d *Database) ProcessInstances(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	after string,
	limit int,
) ([]ProcessInstance, error) {
	return d.processInstances(
		ctx,
		`SELECT i.handler_key, i.instance_id, i.ended, i.state, i.deadline_queue_id
		FROM process.instances AS i
		WHERE i.handler_key = $1
			AND i.instance_id > $2
		ORDER BY i.instance_id
		LIMIT $3`,
		xsql.UUID(handlerKey),
		after,
		limit,
	)
}

// ProcessInstance returns the process instance with the given handler key and
// ID. It returns false if there is no such instance.
func (d *Database) ProcessInstance(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	id string,
) (ProcessInstance, bool, error) {
	instances, err := d.processInstances(
		ctx,
		`SELECT i.handler_key, i.instance_id, i.ended, i.state, i.deadline_queue_id
		FROM process.instances AS i
		WHERE i.handler_key = $1
			AND i.instance_id = $2`,
		xsql.UUID(handlerKey),
		id,
	)
	if err != nil || len(instances) == 0 {
		return ProcessInstance{}, false, err
	}

	return instances[0], true, nil
}

// CountProcessInstances returns the number of instances of the process
// handler with the given key, including those that have ended.
func (d *Database) CountProcessInstances(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
) (InstanceCount, error) {
	return d.count(
		ctx,
		`SELECT count(*) FROM (
			SELECT 1
			FROM process.instances AS i
			WHERE i.handler_key = $1
			LIMIT $2
		) AS x`,
		xsql.UUID(handlerKey),
		countLimit,
	)
}

// AggregateRoot returns the current root of the given aggregate instance.
//
// It calls the handler's New method, restores the instance's snapshot if it
// has one, and applies the events recorded after it. It returns an error if
// the instance recorded more than maxEvents events after the snapshot, or if
// application code fails or panics. None of this changes anything outside
// the returned root.
func (d *Database) AggregateRoot(
	ctx context.Context,
	app *Application,
	h *Handler,
	inst AggregateInstance,
	maxEvents int,
) (dogma.AggregateRoot, error) {
	if h.Aggregate == nil {
		return nil, fmt.Errorf("%s is not an aggregate handler", h.Name)
	}

	root, err := xerrors.RecoverT(
		func() (dogma.AggregateRoot, error) {
			return h.Aggregate.New(), nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create aggregate root: %w", err)
	}

	from := uint64(0)

	if inst.Snapshot != nil {
		err := xerrors.Recover(
			func() error {
				return root.UnmarshalBinary(inst.Snapshot)
			},
		)
		if err == nil {
			from = inst.SnapshotOffset + 1
		} else {
			// A snapshot that can not be restored is ignored, as the engine
			// ignores it, and the root is rebuilt from every event.
			root, err = xerrors.RecoverT(
				func() (dogma.AggregateRoot, error) {
					return h.Aggregate.New(), nil
				},
			)
			if err != nil {
				return nil, fmt.Errorf("unable to create aggregate root: %w", err)
			}
		}
	}

	events, err := d.events(
		ctx,
		`SELECT `+eventColumns+`
		FROM eventstream.events AS e
		WHERE e.aggregate_handler_key = $1
			AND e.aggregate_instance_id = $2
			AND e.stream_offset >= $3
		ORDER BY e.stream_offset
		LIMIT $4`,
		xsql.UUID(inst.HandlerKey),
		inst.ID,
		from,
		maxEvents+1,
	)
	if err != nil {
		return nil, err
	}

	if len(events) > maxEvents {
		return nil, fmt.Errorf(
			"the instance recorded more than %d events after its most recent snapshot",
			maxEvents,
		)
	}

	for _, e := range events {
		typeID := e.Envelope.GetBody().GetMessage().GetTypeId()
		data := e.Envelope.GetBody().GetMessage().GetData()

		m, err := app.DecodeMessage(typeID, data)
		if err != nil {
			return nil, fmt.Errorf("unable to decode event at offset %d: %w", e.Offset, err)
		}

		event, ok := m.(dogma.Event)
		if !ok {
			return nil, fmt.Errorf("message at offset %d is not an event", e.Offset)
		}

		if err := xerrors.Recover(
			func() error {
				root.ApplyEvent(event)
				return nil
			},
		); err != nil {
			return nil, fmt.Errorf("unable to apply event at offset %d: %w", e.Offset, err)
		}
	}

	return root, nil
}

// ProcessRoot returns the root of the given process instance, restored from
// its state.
//
// It calls the handler's New method, then the root's UnmarshalBinary method.
// It returns an error if either fails or panics.
func ProcessRoot(h *Handler, inst ProcessInstance) (dogma.ProcessRoot, error) {
	if h.Process == nil {
		return nil, fmt.Errorf("%s is not a process handler", h.Name)
	}

	return xerrors.RecoverT(
		func() (dogma.ProcessRoot, error) {
			root := h.Process.New()

			if inst.State == nil {
				return root, nil
			}

			return root, root.UnmarshalBinary(inst.State)
		},
	)
}

// RootDescription returns the description of an aggregate or process root, as
// returned by its description method. It returns an error if the method
// panics.
func RootDescription(root any, hasEnded bool) (string, error) {
	return xerrors.RecoverT(
		func() (string, error) {
			switch root := root.(type) {
			case dogma.AggregateRoot:
				return root.AggregateInstanceDescription(), nil
			case dogma.ProcessRoot:
				return root.ProcessInstanceDescription(hasEnded), nil
			default:
				return "", errors.New("not an aggregate or process root")
			}
		},
	)
}

func (d *Database) aggregateInstances(
	ctx context.Context,
	query string,
	args ...any,
) ([]AggregateInstance, error) {
	var instances []AggregateInstance

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var (
					inst           AggregateInstance
					streamID       sql.Null[string]
					snapshotOffset sql.Null[int64]
				)

				inst.HandlerKey = &uuidpb.UUID{}

				if err := rows.Scan(
					xsql.UUID(inst.HandlerKey),
					&inst.ID,
					&streamID,
					&inst.Snapshot,
					&snapshotOffset,
				); err != nil {
					return err
				}

				if streamID.Valid {
					id, err := uuidpb.Parse(streamID.V)
					if err != nil {
						return err
					}
					inst.StreamID = id
				}

				inst.SnapshotOffset = uint64(snapshotOffset.V)
				instances = append(instances, inst)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read aggregate instances: %w", err)
	}

	return instances, nil
}

func (d *Database) processInstances(
	ctx context.Context,
	query string,
	args ...any,
) ([]ProcessInstance, error) {
	var instances []ProcessInstance

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var (
					inst    ProcessInstance
					queueID sql.Null[string]
				)

				inst.HandlerKey = &uuidpb.UUID{}

				if err := rows.Scan(
					xsql.UUID(inst.HandlerKey),
					&inst.ID,
					&inst.HasEnded,
					&inst.State,
					&queueID,
				); err != nil {
					return err
				}

				if queueID.Valid {
					id, err := uuidpb.Parse(queueID.V)
					if err != nil {
						return err
					}
					inst.DeadlineQueueID = id
				}

				instances = append(instances, inst)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read process instances: %w", err)
	}

	return instances, nil
}

func (d *Database) count(ctx context.Context, query string, args ...any) (InstanceCount, error) {
	var c InstanceCount

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, args...).Scan(&c.Count)
		},
	)
	if err != nil {
		return InstanceCount{}, fmt.Errorf("unable to count instances: %w", err)
	}

	c.IsAtLeast = c.Count >= countLimit

	return c, nil
}
