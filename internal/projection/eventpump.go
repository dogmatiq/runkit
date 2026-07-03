package projection

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/concurrency"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// EventPump is a [messagepump.Driver] that delivers pending events to a
// projection message handler.
type EventPump struct {
	DB           *sql.DB
	Handler      dogma.ProjectionMessageHandler
	Identity     *identitypb.Identity
	Concurrency  dogma.ConcurrencyPreference
	EventTypeIDs *uuidpb.Set
	Logger       *slog.Logger
}

// AcquireDelivery attempts to acquire the next pending event for the handler on
// one of its tracked event streams.
//
// If there are no relevant events available on the chosen stream, it advances
// the checkpoint offset to the end of the stream so that the stream is not
// re-acquired until new events arrive.
func (p *EventPump) AcquireDelivery(
	ctx context.Context,
	tx *sql.Tx,
) (messagepump.Delivery, bool, error) {
	return messagepump.AcquireEventDelivery(
		ctx,
		tx,
		p.Logger,
		p.Identity.GetKey(),
		p.EventTypeIDs,
	)
}

// PostponeDelivery reschedules consumption of the stream after delay.
func (p *EventPump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	return messagepump.PostponeEventDelivery(
		ctx,
		tx,
		p.Identity.GetKey(),
		delivery,
		delay,
	)
}

// HandleDelivery dispatches an event to the projection handler.
func (p *EventPump) HandleDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	envelope *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var event dogma.Event

	if err := xmessage.UnpackMessage(
		envelope,
		&event,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal event",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	var nextCheckpointOffset uint64

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		tx,
		p.Identity.GetKey(),
		p.Concurrency,
		func() error {
			var err error
			nextCheckpointOffset, err = xerrors.RecoverT(
				func() (uint64, error) {
					return p.Handler.HandleEvent(
						ctx,
						&eventScope{
							streamID:         delivery.Stream.ID.AsString(),
							offset:           delivery.Stream.EventOffset,
							recordedAt:       envelope.GetBody().GetCreatedAt().AsTime(),
							checkpointOffset: delivery.Stream.CheckpointOffset,
							logger:           logger,
						},
						event,
					)
				},
			)
			return err
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)

		return messagepump.ErrFailed
	}

	if nextCheckpointOffset == delivery.Stream.EventOffset+1 {
		return messagepump.AdvanceStreamCheckpoint(
			ctx,
			tx,
			p.Logger,
			p.Identity.GetKey(),
			delivery.Stream.ID,
			delivery.Stream.CheckpointOffset,
			nextCheckpointOffset,
		)
	}

	logger.WarnContext(
		ctx,
		"optimistic concurrency conflict",
		slog.Uint64("expected_checkpoint_offset", delivery.Stream.EventOffset+1),
		slog.Uint64("actual_checkpoint_offset", nextCheckpointOffset),
	)

	return messagepump.SetCheckpointOffset(
		ctx,
		tx,
		p.Identity.GetKey(),
		delivery.Stream.ID,
		delivery.Stream.CheckpointOffset,
		nextCheckpointOffset,
	)
}
