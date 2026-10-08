package cluster_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestMembershipObserver(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var group sync.WaitGroup
	db := xtesting.NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	await := func(want MembershipChange, channels ...<-chan MembershipChange) {
		t.Helper()

		for i, ch := range channels {
			select {
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for change notification on channel %d", i)

			case got := <-ch:
				if !got.Live.IsEqual(want.Live) {
					t.Fatalf(
						"unexpected live nodes on channel %d: got %s, want %s",
						i,
						got.Live,
						want.Live,
					)
				}

				if !got.Added.IsEqual(want.Added) {
					t.Fatalf(
						"unexpected added nodes on channel %d: got %s, want %s",
						i,
						got.Added,
						want.Added,
					)
				}

				if !got.Removed.IsEqual(want.Removed) {
					t.Fatalf(
						"unexpected removed nodes on channel %d: got %s, want %s",
						i,
						got.Removed,
						want.Removed,
					)
				}
			}
		}
	}

	startNode := func() (*uuidpb.UUID, func()) {
		h := &Heartbeater{
			DB:     db,
			Logger: logger,
			NodeID: uuidpb.Generate(),
		}

		ctx, cancel := context.WithCancel(ctx)

		group.Go(func() {
			h.Run(ctx)
		})

		return h.NodeID, cancel
	}

	notifications := &notification.Listener{
		DB:     db,
		Logger: logger,
	}

	group.Go(func() {
		notifications.Run(ctx)
	})

	observer := &MembershipObserver{
		Notifications: notifications,
		Logger:        logger,
	}

	group.Go(func() {
		observer.Run(ctx)
	})

	// Start a subscription *before* there are any nodes.
	subscription1 := make(chan MembershipChange)
	unsubscribe1 := observer.Subscribe(subscription1)
	defer unsubscribe1()

	node1ID, stopNode1 := startNode()
	defer stopNode1()

	await(
		MembershipChange{
			Added: uuidpb.NewSet(node1ID),
			Live:  uuidpb.NewSet(node1ID),
		},
		subscription1,
	)

	// Start a second observer *after* a node already exists.
	subscription2 := make(chan MembershipChange)
	unsubscribe2 := observer.Subscribe(subscription2)
	defer unsubscribe2()

	await(
		MembershipChange{
			Added: uuidpb.NewSet(node1ID),
			Live:  uuidpb.NewSet(node1ID),
		},
		subscription2,
	)

	node2ID, stopNode2 := startNode()
	defer stopNode2()

	await(
		MembershipChange{
			Added: uuidpb.NewSet(node2ID),
			Live:  uuidpb.NewSet(node1ID, node2ID),
		},
		subscription1,
		subscription2,
	)

	stopNode1()

	await(
		MembershipChange{
			Removed: uuidpb.NewSet(node1ID),
			Live:    uuidpb.NewSet(node2ID),
		},
		subscription1,
		subscription2,
	)

	stopNode2()

	await(
		MembershipChange{
			Removed: uuidpb.NewSet(node2ID),
		},
		subscription1,
		subscription2,
	)

	cancel()
	group.Wait()
}
