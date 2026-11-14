package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/failure"
)

// Sink is the destination to which a [Consumer] passes queued messages.
//
// Each sink delivers messages to one of the application's message handlers.
type Sink interface {
	// Deliver delivers the message in the given envelope to the sink's message
	// handler.
	//
	// A message that no longer needs to be delivered, such as a deadline whose
	// process instance has ended, is deemed "canceled". It returns nil for a
	// canceled message without calling the handler.
	//
	// It must make all of its changes to the database within tx. When it
	// returns nil, the consumer removes the message within tx and commits tx,
	// so the changes and the removal are committed together. The sink may
	// remove the message itself. For example, when a process handler ends an
	// instance while handling one of the instance's deadlines, the sink removes
	// that deadline itself.
	//
	// It may return [ErrBusy] instead of waiting if it needs a lock while
	// another transaction, or another call to [Sink.Deliver], holds that lock.
	// Any other error is a failure of the delivery. After any error, the
	// consumer rolls back tx, which discards any changes that the sink made.
	// Unless the consumer is stopping, it then postpones the delivery, and
	// after a failure, it extends the delivery's failure streak.
	//
	// It returns a deterministic error for a failure that happens again on
	// every attempt, such as a message that can't be unmarshaled. It doesn't
	// log its failures, because the consumer logs them.
	//
	// See [xerrors.IsDeterministic].
	//
	// engineLogger is the target for the engine's records about the delivery,
	// and appLogger is the target for the application's records from the
	// handler. Both loggers already have the sink's [Sink.LogAttr], and the
	// consumer writes the application's records only if the delivery
	// completes.
	Deliver(
		ctx context.Context,
		tx *sql.Tx,
		d Delivery,
		env *envelopepb.Envelope,
		engineLogger *slog.Logger,
		appLogger *slog.Logger,
	) error

	// LogAttr returns an attribute that describes the sink in log records.
	//
	// The consumer adds it to its own records about deliveries to the sink,
	// and to the loggers that it passes to [Sink.Deliver].
	LogAttr() slog.Attr
}

// Delivery is the job of getting a queued message to its handler.
//
// A delivery may take many attempts, and is complete when the message is
// delivered or canceled. A Delivery value describes the delivery at the time
// that the consumer claims the message for an attempt.
type Delivery struct {
	// MessageID is the ID of the message being delivered.
	MessageID *uuidpb.UUID

	// MessageTypeID is the ID of the message's type.
	MessageTypeID *uuidpb.UUID

	// QueueID is the ID of the message's queue.
	QueueID *uuidpb.UUID

	// Streak is the delivery's failure streak, as it was when the consumer
	// claimed the message.
	Streak failure.Streak
}

// ErrBusy is returned by [Sink.Deliver] when the sink needs a lock while
// another transaction, or another call to [Sink.Deliver], holds that lock.
var ErrBusy = errors.New("sink is busy")
