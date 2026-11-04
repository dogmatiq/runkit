package remoteengine_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/remoteengine"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
)

func TestConnector(t *testing.T) {
	t.Run("it calls the discoverer again if it fails", func(t *testing.T) {
		var calls atomic.Int32

		connector := &Connector{
			Discover: func(
				ctx context.Context,
				_ func(grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					return errors.New("<error>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
			Logger: spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, connector.Run)

		deadline := time.After(xtesting.WaitTimeout)
		for calls.Load() < 2 {
			select {
			case <-deadline:
				t.Fatal("timeout waiting for the discoverer to be called again")
			case <-time.After(10 * time.Millisecond):
			}
		}
	})

	t.Run("it calls the discoverer again if it panics", func(t *testing.T) {
		var calls atomic.Int32

		connector := &Connector{
			Discover: func(
				ctx context.Context,
				_ func(grpc.ClientConnInterface) func(),
			) error {
				if calls.Add(1) == 1 {
					panic("<panic>")
				}

				<-ctx.Done()
				return ctx.Err()
			},
			Logger: spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, connector.Run)

		deadline := time.After(xtesting.WaitTimeout)
		for calls.Load() < 2 {
			select {
			case <-deadline:
				t.Fatal("timeout waiting for the discoverer to be called again")
			case <-time.After(10 * time.Millisecond):
			}
		}
	})
}
