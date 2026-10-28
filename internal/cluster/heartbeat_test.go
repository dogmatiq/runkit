package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestHeartbeater(t *testing.T) {
	t.Run("it sends heartbeats on a topic named after the node", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, signals.Run)

		heartbeats := make(chan string, 10)
		defer signals.Subscribe("heartbeat", heartbeats)()

		h := &Heartbeater{
			NodeID:   uuidpb.Generate(),
			DB:       db,
			Interval: 10 * time.Millisecond,
			Logger:   logger,
		}

		xtesting.GoE(t, h.Run)

		// The first signal may be the listener starting, which carries no
		// node ID; after that, every signal is a heartbeat from this node.
		want := h.NodeID.AsString()

		for i := 0; i < 3; {
			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for heartbeat")
			case got := <-heartbeats:
				if got == "" {
					continue
				}

				if got != want {
					t.Fatalf("unexpected heartbeat: got %q, want %q", got, want)
				}

				i++
			}
		}
	})

	t.Run("it keeps running when a heartbeat can not be sent", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		db.Close()

		h := &Heartbeater{
			NodeID:   uuidpb.Generate(),
			DB:       db,
			Interval: 10 * time.Millisecond,
			Logger:   logger,
		}

		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		// Run must not return until ctx is canceled, despite every attempt
		// to send a heartbeat failing.
		err := h.Run(ctx)

		if ctx.Err() == nil {
			t.Fatal("heartbeater stopped before context was canceled")
		}

		if !errors.Is(err, ctx.Err()) {
			t.Fatalf("unexpected error: got %v, want %v", err, ctx.Err())
		}
	})
}
