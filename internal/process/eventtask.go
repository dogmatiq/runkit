package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

type eventTask struct {
	Tx                      *sql.Tx
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	StreamID                *uuidpb.UUID
	EventOffset             uint64
	EnvelopeBytes           []byte
	BackoffBase, BackoffCap time.Duration
	ParentLogger, Logger    *slog.Logger
}

var errFailed = errors.New("unable to handle event")

// Execute processes the task by handling its event and committing the
// transaction.
func (t *eventTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	err := t.handleEvent(ctx)

	if errors.Is(err, errFailed) {
		err = t.failAndPostpone(ctx)
	}

	if err != nil {
		return err
	}

	if err := t.Tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// handleEvent unpacks the event envelope and dispatches it to the handler.
func (t *eventTask) handleEvent(ctx context.Context) error {
	var (
		eventEnvelope = &envelopepb.Envelope{}
		event         dogma.Event
	)

	if err := xmessage.Unpack(
		t.EnvelopeBytes,
		eventEnvelope,
		&event,
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to unmarshal event",
			xslog.Error(err),
		)
		return errFailed
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("event", eventEnvelope),
	)

	instanceID, ok, err := t.routeEventToInstance(ctx, event)
	if err != nil {
		return err
	}

	if !ok {
		return t.advanceCheckpoint(ctx)
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("event", eventEnvelope),
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
	)

	root, ok, err := t.loadInstance(ctx, instanceID)
	if err != nil {
		return err
	}

	if !ok {
		return t.advanceCheckpoint(ctx)
	}

	scope := &messageScope{
		instanceID: instanceID,
		root:       root,
		packer:     &envelopepb.EffectPacker{},
		time:       eventEnvelope.GetBody().GetCreatedAt().AsTime(),
		logger:     t.Logger,
	}

	if err := xerrors.ConvertPanicToError(
		func() error {
			return t.Handler.HandleEvent(
				ctx,
				root,
				scope,
				event,
			)
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)
		return errFailed
	}

	if scope.ended {
		if err := t.endInstance(ctx, instanceID); err != nil {
			return err
		}
	} else if scope.mutated {
		if err := t.saveInstance(ctx, instanceID, root); err != nil {
			return err
		}
	}

	return t.advanceCheckpoint(ctx)
}

// loadInstance loads the process instance with the given ID and returns its
// root. If the instance has been ended, it returns a nil root and false.
func (t *eventTask) loadInstance(
	ctx context.Context,
	instanceID string,
) (dogma.ProcessRoot, bool, error) {
	root := t.Handler.New()

	// Upsert with a dummy update to acquire a row lock, guaranteeing
	// serialized access even for brand-new instances.
	row := t.Tx.QueryRowContext(
		ctx,
		`INSERT INTO process.instances (handler_key, instance_id)
		VALUES ($1, $2)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			handler_key = EXCLUDED.handler_key
		RETURNING ended, state`,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	)

	var (
		ended bool
		state []byte
	)

	if err := row.Scan(&ended, &state); err != nil {
		return nil, false, fmt.Errorf("unable to load process instance: %w", err)
	}

	if ended {
		return nil, false, nil
	}

	if state != nil {
		if err := xerrors.ConvertPanicToError(
			func() error {
				return root.UnmarshalBinary(state)
			},
		); err != nil {
			t.Logger.ErrorContext(
				ctx,
				"unable to unmarshal process instance state",
				xslog.Error(err),
			)
			return nil, false, errFailed
		}
	}

	return root, true, nil
}

// endInstance marks the process instance as ended and clears its state.
func (t *eventTask) endInstance(
	ctx context.Context,
	instanceID string,
) error {
	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE process.instances SET
			ended = true,
			state = NULL
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to end process instance: %w", err)
	}

	return nil
}

// saveInstance persists the process root state for the given instance.
func (t *eventTask) saveInstance(
	ctx context.Context,
	instanceID string,
	root dogma.ProcessRoot,
) error {
	var state []byte

	if err := xerrors.ConvertPanicToError(
		func() error {
			var err error
			state, err = root.MarshalBinary()
			return err
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to marshal process instance state",
			xslog.Error(err),
		)
		return errFailed
	}

	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE process.instances SET
			state = $1
		WHERE handler_key = $2
		AND instance_id = $3`,
		state,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to save process instance: %w", err)
	}

	return nil
}

// advanceCheckpoint updates the handler's checkpoint offset for this stream.
func (t *eventTask) advanceCheckpoint(ctx context.Context) error {
	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
		AND stream_id = $3`,
		t.EventOffset+1,
		xsql.UUID(t.Identity.GetKey()),
		xsql.UUID(t.StreamID),
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return nil
}

// routeEventToInstance routes the event to a process instance by calling the
// handler's RouteEventToInstance() method.
func (t *eventTask) routeEventToInstance(
	ctx context.Context,
	event dogma.Event,
) (instanceID string, ok bool, err error) {
	if err := xerrors.ConvertPanicToError(
		func() error {
			instanceID, ok, err = t.Handler.RouteEventToInstance(ctx, event)
			if err != nil {
				return err
			}

			if ok && instanceID == "" {
				return fmt.Errorf("handler returned empty instance ID")
			}

			return nil
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to route event to instance",
			xslog.Error(err),
		)

		return "", false, errFailed
	}

	return instanceID, ok, nil
}

// failAndPostpone increments the failure counter and postpones the stream so
// that consumption is retried after an exponential backoff period.
func (t *eventTask) failAndPostpone(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT eventstream.fail_and_postpone($1, $2, $3, $4)`,
		xsql.UUID(t.Identity.GetKey()),
		xsql.UUID(t.StreamID),
		t.BackoffBase,
		t.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone stream consumption after failure: %w", err)
	}

	return nil
}
