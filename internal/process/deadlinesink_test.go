package process_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	. "github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestDeadlineSink(t *testing.T) {
	// setup returns a sink whose handler fails the test if it's called, and an
	// envelope containing a deadline scheduled by the "<instance>" instance.
	setup := func(t testing.TB) (*DeadlineSink, *envelopepb.Envelope) {
		identity := identitypb.New("<handler>", uuidpb.Generate())
		packer := &envelope.Packer{
			Application: identitypb.New("<app>", uuidpb.Generate()),
		}

		sink := &DeadlineSink{
			Handler: &stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]{
				NewFunc: func() dogma.ProcessRoot { return &stubs.ProcessRootStub{} },
				HandleDeadlineFunc: func(
					context.Context,
					dogma.ProcessRoot,
					dogma.ProcessDeadlineScope[dogma.ProcessRoot],
					dogma.Deadline,
				) error {
					t.Error("the handler was called")
					return nil
				},
			},
			Identity: identity,
			Packer:   packer,
			QueueRouter: &messagequeue.Router{
				NodeID:          uuidpb.Generate(),
				CommandHandlers: &uuidcollection.Map[messagequeue.CommandHandler]{},
			},
			HandleTimeout: time.Minute,
		}

		env := packer.
			PackEffects(
				xtesting.PackCommand(t, stubs.CommandA1),
				identity,
				envelope.WithInstanceID("<instance>"),
			).
			PackDeadline(
				stubs.DeadlineA1,
				envelope.WithScheduledFor(time.Now()),
			)

		return sink, env
	}

	// insertInstance inserts the "<instance>" instance of the sink's handler.
	insertInstance := func(t testing.TB, db *sql.DB, sink *DeadlineSink, ended bool) {
		t.Helper()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO process.instances (
				handler_key,
				instance_id,
				ended
			) VALUES ($1, '<instance>', $2)`,
			sink.Identity.GetKey().AsString(),
			ended,
		); err != nil {
			t.Fatal(err)
		}
	}

	// deliver calls sink.Deliver within a new transaction, then rolls the
	// transaction back.
	//
	// It cancels Deliver's context after five seconds, so a sink that waits for
	// a lock returns an error instead of hanging the test. Unless Deliver
	// returns some other error, it fails the test if the transaction is no
	// longer usable, because the consumer then removes the message or postpones
	// the delivery within it.
	deliver := func(
		t testing.TB,
		db *sql.DB,
		sink *DeadlineSink,
		env *envelopepb.Envelope,
	) error {
		t.Helper()

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		err = sink.Deliver(
			ctx,
			tx,
			messagequeue.Delivery{},
			env,
			spruce.NewTestLogger(t),
			spruce.NewTestLogger(t),
		)

		postponed := errors.Is(err, messagequeue.ErrBusy) || errors.Is(err, messagequeue.ErrFailed)

		if err == nil || postponed {
			if _, err := tx.ExecContext(ctx, `SELECT 1`); err != nil {
				t.Fatalf("the transaction is unusable: %s", err)
			}
		}

		return err
	}

	t.Run("when another transaction holds the lock on the process instance", func(t *testing.T) {
		t.Run("it returns ErrBusy without calling the handler", func(t *testing.T) {
			t.Parallel()

			db := xtesting.NewDatabase(t)
			sink, env := setup(t)
			insertInstance(t, db, sink, false)

			lockHolder, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer lockHolder.Rollback()

			if _, err := lockHolder.ExecContext(
				t.Context(),
				`SELECT 1
				FROM process.instances
				WHERE handler_key = $1
				AND instance_id = '<instance>'
				FOR UPDATE`,
				sink.Identity.GetKey().AsString(),
			); err != nil {
				t.Fatal(err)
			}

			err = deliver(t, db, sink, env)

			if !errors.Is(err, messagequeue.ErrBusy) {
				t.Fatalf("unexpected error: got %v, want %v", err, messagequeue.ErrBusy)
			}
		})
	})

	t.Run("when the process instance has ended", func(t *testing.T) {
		t.Run("it returns nil without calling the handler", func(t *testing.T) {
			t.Parallel()

			db := xtesting.NewDatabase(t)
			sink, env := setup(t)
			insertInstance(t, db, sink, true)

			if err := deliver(t, db, sink, env); err != nil {
				t.Fatal(err)
			}
		})
	})
}
