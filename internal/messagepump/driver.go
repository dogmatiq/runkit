package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// ErrFailed is returned by [Driver.HandleDelivery] to indicate that the
// delivery failed and should be postponed for redelivery after an exponential
// backoff.
var ErrFailed = errors.New("delivery failed")

// ErrBusy is returned by [Driver.HandleDelivery] to indicate that the delivery
// target is temporarily unavailable (e.g. locked by another transaction) and
// should be retried.
var ErrBusy = errors.New("delivery target is busy")

// Driver implements the underlying messaging operations orchestrated by a
// [MessagePump].
type Driver interface {
	// AcquireDelivery attempts to acquire the next pending [Delivery] within
	// tx.
	//
	// If there are no pending deliveries, ok is false.
	AcquireDelivery(ctx context.Context, tx *sql.Tx) (del Delivery, ok bool, err error)

	// HandleDelivery processes a [Delivery] within tx.
	//
	// It returns [ErrFailed] to indicate that the delivery should be postponed
	// for redelivery with an incremented failure count, or [ErrBusy] to
	// indicate that the delivery should be retried without incrementing the
	// failure count.
	HandleDelivery(
		ctx context.Context,
		tx *sql.Tx,
		del Delivery,
		envelope *envelopepb.Envelope,
		logger *slog.Logger,
	) error

	// PostponeDelivery schedules redelivery after the specified delay.
	//
	// The delivery's failure count may be greater than the value returned by
	// AcquireDelivery, indicating that the postponement is due to a failure.
	PostponeDelivery(
		ctx context.Context,
		tx *sql.Tx,
		del Delivery,
		delay time.Duration,
	) error
}
