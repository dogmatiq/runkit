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

// Handler delivers queued messages to one of the application's message
// handlers, such as an aggregate or a process.
//
// [Consumer.Handlers] maps each message type to the Handler that delivers the
// messages of that type.
type Handler interface {
	// Deliver delivers the message in the given envelope within tx.
	//
	// It returns [ErrFailed] if this attempt to deliver the message failed, so
	// that the delivery is postponed and its failure count is incremented. It
	// returns [ErrBusy] if it needs a row and another transaction holds that
	// row's lock, so that the delivery is postponed without a failure. Any
	// other error ends the attempt, so tx is rolled back and the message is
	// left for another attempt.
	Deliver(
		ctx context.Context,
		tx *sql.Tx,
		d Delivery,
		env *envelopepb.Envelope,
		engineLogger *slog.Logger,
	) error
}

// Delivery is the job of getting a queued message to its handler.
//
// A delivery may take many attempts, and is complete when the message is
// delivered. A Delivery value describes the delivery at the time that the
// consumer claims the message for an attempt.
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
	// ErrFailed is returned by [Handler.Deliver] when an attempt to deliver a
	// message fails.
	ErrFailed = errors.New("unable to deliver message")

	// ErrBusy is returned by [Handler.Deliver] when the handler needs a row and
	// another transaction holds that row's lock.
	ErrBusy = errors.New("delivery handler is busy")
)
