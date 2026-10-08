package notification_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestListener(t *testing.T) {
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

	listener := &Listener{
		DB:     db,
		Logger: logger,
	}

	group.Go(func() {
		listener.Run(ctx)
	})

	ch1 := make(chan string)
	ch2 := make(chan string)

	unsubscribe1a := listener.Subscribe("topic-a", ch1)
	defer unsubscribe1a()

	unsubscribe1b := listener.Subscribe("topic-b", ch1)
	defer unsubscribe1b()

	unsubscribe2a := listener.Subscribe("topic-a", ch2)
	defer unsubscribe2a()

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-1')`)

	await(
		"payload-1",
		ch1,
		ch2,
	)

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-b', 'payload-2')`)

	await(
		"payload-2",
		ch1,
	)

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-3')`)

	await(
		"payload-3",
		ch1,
		ch2,
	)

	unsubscribe1a()
	unsubscribe2a()

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', 'payload-4')`)

	select {
	case <-time.After(100 * time.Millisecond):
	case <-ch1:
		t.Fatalf("received unexpected notification on subscription1")
	case <-ch2:
		t.Fatalf("received unexpected notification on subscription2")
	}

	xtesting.ExecOne(t, db, `SELECT pg_notify('topic-b', 'payload-5')`)

	await(
		"payload-5",
		ch1,
	)

	cancel()
	group.Wait()
}
