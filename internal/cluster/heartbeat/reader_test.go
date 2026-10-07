package heartbeat_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster/heartbeat"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	db := xtesting.NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	var group sync.WaitGroup

	reader := &Reader{
		DB:     db,
		Logger: logger,
	}

	group.Go(func() {
		reader.Run(ctx)
	})

	startWriter := func() (*uuidpb.UUID, func()) {
		w := &Writer{
			DB:     db,
			Logger: logger,
			NodeID: uuidpb.Generate(),
		}

		ctxW, cancelW := context.WithCancel(ctx)

		group.Go(func() {
			w.Run(ctxW)
		})

		return w.NodeID, cancelW
	}

	await := func(ch <-chan *uuidpb.Set, want ...*uuidpb.UUID) {
		t.Helper()

		wantSet := uuidpb.NewSet(want...)

		select {
		case got := <-ch:
			if !got.IsEqual(wantSet) {
				t.Fatalf("unexpected live nodes: got %s, want %s", got, wantSet)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for live nodes")
		}
	}

	// Start an observer *before* there are any nodes.
	observer1 := make(chan *uuidpb.Set)
	stopObserver1 := reader.Observe(observer1)
	defer stopObserver1()

	node1ID, stopWriter1 := startWriter()
	defer stopWriter1()

	await(observer1, node1ID)

	// Start a second observer *after* a node already exists.
	observer2 := make(chan *uuidpb.Set)
	stopObserver2 := reader.Observe(observer2)
	defer stopObserver2()

	await(observer2, node1ID)

	node2ID, stopWriter2 := startWriter()
	defer stopWriter2()

	await(observer1, node1ID, node2ID)
	await(observer2, node1ID, node2ID)

	stopWriter1()

	await(observer1, node2ID)
	await(observer2, node2ID)

	stopWriter2()

	await(observer1)
	await(observer2)

	cancel()
	group.Wait()
}
