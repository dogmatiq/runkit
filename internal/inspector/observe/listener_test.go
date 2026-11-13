package observe_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/inspector/demoapp"
	. "github.com/dogmatiq/runkit/internal/inspector/observe"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestListener(t *testing.T) {
	t.Run("it reports the nodes of a running cluster as live", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		app := &demoapp.App{
			StatementInterval: time.Hour,
		}

		cluster := xtesting.StartCluster(t, db, app, 3)

		l := &Listener{
			DB:               db,
			HeartbeatTimeout: 2 * time.Second,
			RetryDelay:       100 * time.Millisecond,
			OnHeartbeat:      func(*uuidpb.UUID) {},
			OnChange:         func() {},
		}

		xtesting.Go(
			t,
			func(ctx context.Context) {
				l.Run(ctx)
			},
		)

		xtesting.WaitFor(
			t,
			"three live nodes",
			func() bool {
				live := l.LiveNodes(time.Now())
				return live.Known && len(live.Nodes) == 3
			},
		)

		cluster.StopNode(cluster.Nodes()[0])

		xtesting.WaitFor(
			t,
			"two live nodes",
			func() bool {
				live := l.LiveNodes(time.Now())
				return live.Known && len(live.Nodes) == 2
			},
		)
	})

	t.Run("it does not know the live nodes until it has listened for a whole heartbeat timeout", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		app := &demoapp.App{
			StatementInterval: time.Hour,
		}

		xtesting.StartCluster(t, db, app, 1)

		heard := make(chan struct{}, 1)

		l := &Listener{
			DB:               db,
			HeartbeatTimeout: time.Hour,
			RetryDelay:       100 * time.Millisecond,
			OnHeartbeat: func(*uuidpb.UUID) {
				select {
				case heard <- struct{}{}:
				default:
				}
			},
			OnChange: func() {},
		}

		xtesting.Go(
			t,
			func(ctx context.Context) {
				l.Run(ctx)
			},
		)

		// A heartbeat arrives only once the listener is listening.
		xtesting.ExpectToReceiveN(t, "a heartbeat", heard, struct{}{})

		live := l.LiveNodes(time.Now())

		if live.Known {
			t.Fatal("expected the live nodes to be unknown")
		}

		if live.Reason == nil {
			t.Fatal("expected a reason")
		}

		if _, ok := live.FindWorkloadOwner(uuidpb.Generate()); ok {
			t.Fatal("expected no workload owner")
		}
	})
}
