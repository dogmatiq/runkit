package cluster_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestNotificationObserver(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var group sync.WaitGroup
	db := xtesting.NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	await := func(want string, channels ...<-chan string) {
		t.Helper()

		for i, ch := range channels {
			select {
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for notification on channel %d", i)

			case got := <-ch:
				if got != want {
					t.Fatalf(
						"unexpected notification on channel %d: got %s, want %s",
						i,
						got,
						want,
					)
				}
			}
		}
	}

	observer := &cluster.NotificationListener{
		DB:     db,
		Logger: logger,
	}

	group.Go(func() {
		observer.Run(ctx)
	})

	subscription1 := make(chan string)
	unsubscribe1a := observer.Subscribe("topic-a", subscription1)
	defer unsubscribe1a()

	unsubscribe1b := observer.Subscribe("topic-b", subscription1)
	defer unsubscribe1b()

	subscription2 := make(chan string)
	unsubscribe2a := observer.Subscribe("topic-a", subscription2)
	defer unsubscribe2a()

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-1')`)

	await(
		"payload-1",
		subscription1,
		subscription2,
	)

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-b', 'payload-2')`)

	await(
		"payload-2",
		subscription1,
	)

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-3')`)

	await(
		"payload-3",
		subscription1,
		subscription2,
	)

	unsubscribe1a()
	unsubscribe2a()

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-4')`)

	select {
	case <-time.After(100 * time.Millisecond):
	case <-subscription1:
		t.Fatalf("received unexpected notification on subscription1")
	case <-subscription2:
		t.Fatalf("received unexpected notification on subscription2")
	}

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-b', 'payload-5')`)

	await(
		"payload-5",
		subscription1,
	)

	cancel()
	group.Wait()
}
