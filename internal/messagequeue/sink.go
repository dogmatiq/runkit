package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
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
	// It returns [ErrFailed] if it can't deliver the message because of a
	// problem with the message or the handler, such as when the message can't
	// be unmarshaled or the handler panics. It may return [ErrBusy] instead of
	// waiting if it needs a lock while another transaction, or another call to
	// [Sink.Deliver], holds that lock. After either error, the consumer rolls
	// back tx, which discards any changes that the sink made, then postpones
	// the delivery. Only [ErrFailed] increments [Delivery.Failures].
	//
	// It returns any other error, such as a database error, if the problem
	// isn't specific to the message. The consumer then rolls back tx, leaves
	// the message unchanged, and waits before it starts any more attempts.
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

	// Failures is the number of the delivery's attempts that have failed.
	Failures uint64

	// FailingSince is the time of the first of those failures, or zero if
	// there are no failures.
	FailingSince time.Time
}

var (
	// ErrFailed is returned by [Sink.Deliver] when it can't deliver a message
	// because of a problem with the message or the handler.
	ErrFailed = errors.New("unable to deliver message")

	// ErrBusy is returned by [Sink.Deliver] when the sink needs a lock while
	// another transaction, or another call to [Sink.Deliver], holds that lock.
	ErrBusy = errors.New("sink is busy")
)
