package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCommandSink(t *testing.T) {
	t.Run("when the handler prefers to minimize concurrency", func(t *testing.T) {
		setup := func(
			t *testing.T,
			handle func(context.Context, dogma.IntegrationCommandScope, dogma.Command) error,
		) (deliver func() error) {
			db := xtesting.NewDatabase(t)

			sink := &CommandSink{
				Handler: &stubs.IntegrationMessageHandlerStub{
					HandleCommandFunc: handle,
				},
				Identity: identitypb.New("<handler>", uuidpb.Generate()),
				Packer: &envelope.Packer{
					Application: identitypb.New("<app>", uuidpb.Generate()),
				},
				Concurrency:   dogma.MinimizeConcurrency,
				HandleTimeout: time.Minute,
			}

			env := xtesting.PackCommand(t, stubs.CommandA1)
			logger := spruce.NewTestLogger(t)

			return func() error {
				tx, err := db.BeginTx(t.Context(), nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()

				return sink.Deliver(
					t.Context(),
					tx,
					messagequeue.Delivery{},
					env,
					logger,
					logger,
				)
			}
		}

		t.Run("it returns ErrBusy without calling the handler while the handler is handling another command", func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			firstCallStarted := make(chan struct{})
			releaseFirstCall := make(chan struct{})

			deliver := setup(
				t,
				func(context.Context, dogma.IntegrationCommandScope, dogma.Command) error {
					// Only the first call blocks, so that the test fails rather
					// than hangs if the sink calls the handler a second time.
					if calls.Add(1) == 1 {
						close(firstCallStarted)
						<-releaseFirstCall
					}
					return nil
				},
			)

			firstResult := make(chan error, 1)
			go func() {
				firstResult <- deliver()
			}()

			select {
			case <-firstCallStarted:
			case err := <-firstResult:
				t.Fatalf("Deliver returned before calling the handler: %v", err)
			}

			err := deliver()

			close(releaseFirstCall)
			firstErr := <-firstResult

			if !errors.Is(err, messagequeue.ErrBusy) {
				t.Fatalf("unexpected error: got %v, want %v", err, messagequeue.ErrBusy)
			}

			if n := calls.Load(); n != 1 {
				t.Fatalf("unexpected number of calls to the handler: got %d, want 1", n)
			}

			if firstErr != nil {
				t.Fatal(firstErr)
			}
		})

		t.Run("it calls the handler again after the handler handles a command", func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32

			deliver := setup(
				t,
				func(context.Context, dogma.IntegrationCommandScope, dogma.Command) error {
					calls.Add(1)
					return nil
				},
			)

			for range 2 {
				if err := deliver(); err != nil {
					t.Fatal(err)
				}
			}

			if n := calls.Load(); n != 2 {
				t.Fatalf("unexpected number of calls to the handler: got %d, want 2", n)
			}
		})
	})
}
