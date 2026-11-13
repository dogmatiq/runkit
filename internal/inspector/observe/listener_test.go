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
				m := l.Membership(time.Now())
				return m.Known && len(m.Nodes) == 3
			},
		)

		cluster.StopNode(cluster.Nodes()[0])

		xtesting.WaitFor(
			t,
			"two live nodes",
			func() bool {
				m := l.Membership(time.Now())
				return m.Known && len(m.Nodes) == 2
			},
		)
	})

	t.Run("it does not know the live nodes until it has listened for a whole heartbeat timeout", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		l := &Listener{
			DB:               db,
			HeartbeatTimeout: time.Hour,
			RetryDelay:       100 * time.Millisecond,
		}

		xtesting.Go(
			t,
			func(ctx context.Context) {
				l.Run(ctx)
			},
		)

		m := l.Membership(time.Now())
		if m.Known {
			t.Fatal("expected the live nodes to be unknown")
		}

		if _, ok := m.FindWorkloadOwner(uuidpb.Generate()); ok {
			t.Fatal("expected no workload owner")
		}
	})
}
