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

func TestObserver(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	db := xtesting.NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	var group sync.WaitGroup

	observer := &Observer{
		DB:     db,
		Logger: logger,
	}

	group.Go(func() {
		observer.Run(ctx)
	})

	startNode := func() (*uuidpb.UUID, func()) {
		w := &Heart{
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

	// Start a subscription *before* there are any nodes.
	subcription1 := make(chan *uuidpb.Set)
	unsubscribe1 := observer.Subscribe(subcription1)
	defer unsubscribe1()

	node1ID, stopNode1 := startNode()
	defer stopNode1()

	await(subcription1, node1ID)

	// Start a second observer *after* a node already exists.
	subscription2 := make(chan *uuidpb.Set)
	unsubscribe2 := observer.Subscribe(subscription2)
	defer unsubscribe2()

	await(subscription2, node1ID)

	node2ID, stopNode2 := startNode()
	defer stopNode2()

	await(subcription1, node1ID, node2ID)
	await(subscription2, node1ID, node2ID)

	stopNode1()

	await(subcription1, node2ID)
	await(subscription2, node2ID)

	stopNode2()

	await(subcription1)
	await(subscription2)

	cancel()
	group.Wait()
}
