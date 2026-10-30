package cluster_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestMembershipObserver(t *testing.T) {
	const (
		heartbeatInterval = 20 * time.Millisecond
		heartbeatTimeout  = 5 * heartbeatInterval
	)

	db := xtesting.NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	startNode := func() (*uuidpb.UUID, func()) {
		h := &Heartbeater{
			DB:       db,
			NodeID:   uuidpb.Generate(),
			Interval: heartbeatInterval,
			Logger:   logger,
		}

		return h.NodeID, xtesting.GoE(t, h.Run)
	}

	signals := &signal.Listener{
		DB:     db,
		Logger: logger,
	}

	xtesting.GoE(t, signals.Run)

	observer := &MembershipObserver{
		Signals:           signals,
		HeartbeatInterval: heartbeatInterval,
		HeartbeatTimeout:  heartbeatTimeout,
		Logger:            logger,
	}

	xtesting.GoE(t, observer.Run)

	expectLiveNodeIDs(t, observer)

	node1ID, stopNode1 := startNode()
	defer stopNode1()

	expectLiveNodeIDs(t, observer, node1ID)

	node2ID, stopNode2 := startNode()
	defer stopNode2()

	expectLiveNodeIDs(t, observer, node1ID, node2ID)

	stopNode1()

	expectLiveNodeIDs(t, observer, node2ID)

	stopNode2()

	expectLiveNodeIDs(t, observer)
}

func expectLiveNodeIDs(
	t testing.TB,
	observer *MembershipObserver,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidpb.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := observer.LiveNodeIDs()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected live node IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
