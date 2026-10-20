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
	t.Run("it dispatches notifications to subscribers", func(t *testing.T) {
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
	})

	t.Run("it skips notifications to lossy subscribers that are blocked", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var group sync.WaitGroup
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		group.Go(func() {
			listener.Run(ctx)
		})

		lossy := make(chan string, 1)
		reliable := make(chan string)

		unsubscribeLossy := listener.SubscribeLossy("topic-a", lossy)
		defer unsubscribeLossy()

		unsubscribeReliable := listener.Subscribe("topic-a", reliable)
		defer unsubscribeReliable()

		for _, payload := range []string{"payload-1", "payload-2", "payload-3"} {
			xtesting.ExecOne(t, db, `SELECT pg_notify('topic-a', '`+payload+`')`)

			// The reliable subscriber receiving each payload proves that the
			// blocked lossy subscriber does not hold up dispatching.
			select {
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for %s on reliable subscriber", payload)

			case got := <-reliable:
				if got != payload {
					t.Fatalf(
						"unexpected notification on reliable subscriber: got %s, want %s",
						got,
						payload,
					)
				}
			}
		}

		// Only the first payload fit in the lossy subscriber's buffer.
		select {
		case got := <-lossy:
			if got != "payload-1" {
				t.Fatalf(
					"unexpected notification on lossy subscriber: got %s, want payload-1",
					got,
				)
			}
		default:
			t.Fatalf("expected lossy subscriber to receive the first notification")
		}

		select {
		case got := <-lossy:
			t.Fatalf("unexpected notification on lossy subscriber: %s", got)
		default:
		}

		cancel()
		group.Wait()
	})
}
