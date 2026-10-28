package signal_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestListener(t *testing.T) {
	t.Run("it delivers signals to subscribers of the topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, listener.Run)
		waitForListener(t, db, listener)

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer listener.Subscribe("topic-a", a)()
		defer listener.Subscribe("topic-b", b)()

		send(t, db, "topic-a")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectNoReceive(t, "signal on b", b)

		send(t, db, "topic-b")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
		xtesting.ExpectNoReceive(t, "signal on a", a)
	})

	t.Run("it delivers signals to every subscriber of the topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, listener.Run)
		waitForListener(t, db, listener)

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer listener.Subscribe("topic", a)()
		defer listener.Subscribe("topic", b)()

		send(t, db, "topic")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})

	t.Run("it delivers signals on topics beneath the subscribed topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, listener.Run)
		waitForListener(t, db, listener)

		ch := make(chan string, 10)
		defer listener.Subscribe("a.b", ch)()

		send(t, db, "a.b.c")
		send(t, db, "a.bc")
		send(t, db, "a")
		send(t, db, "x.a.b")
		send(t, db, "a.b")
		send(t, db, "a.b.c.d")

		// The value received is the part of the topic beneath the subscribed
		// topic.
		xtesting.ExpectToReceiveN(t, "signals", ch, "c", "", "c.d")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it stops delivering signals after unsubscribing", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, listener.Run)
		waitForListener(t, db, listener)

		ch := make(chan string, 10)
		witness := make(chan string, 10)

		unsubscribe := listener.Subscribe("topic", ch)
		defer listener.Subscribe("topic", witness)()

		send(t, db, "topic")
		xtesting.ExpectToReceiveN(t, "signals", ch, "")

		unsubscribe()

		// The witness receiving the signal proves that it was dispatched after
		// the subscription was removed.
		send(t, db, "topic")
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "", "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it drops signals when the subscriber's channel is full", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, listener.Run)
		waitForListener(t, db, listener)

		ch := make(chan string, 1)
		witness := make(chan string, 10)

		defer listener.Subscribe("topic", ch)()
		defer listener.Subscribe("topic", witness)()

		send(t, db, "topic")
		send(t, db, "topic")
		send(t, db, "topic")

		// The witness receiving every signal proves that the full channel did
		// not hold up dispatching.
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "", "", "")

		// Only the first signal fit in the subscriber's buffer.
		xtesting.ExpectToReceiveN(t, "signals", ch, "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it signals every subscriber when it starts listening", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		listener := &Listener{
			DB:     db,
			Logger: logger,
		}

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer listener.Subscribe("a", a)()
		defer listener.Subscribe("a.b", b)()

		xtesting.GoE(t, listener.Run)

		// Nothing is sent; the signal comes from the listener itself, as
		// though it were on the subscribed topic, so that a subscriber that
		// read state before the listener was listening can read it again.
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})
}

// send sends a signal on the given topic.
func send(t testing.TB, db *sql.DB, topic string) {
	t.Helper()

	if err := Notify(t.Context(), db, topic); err != nil {
		t.Fatal(err)
	}
}

// waitForListener blocks until the listener is receiving signals.
//
// Signals sent before the listener has started listening are lost, so a probe
// signal is sent on a unique topic until it is received.
func waitForListener(t testing.TB, db *sql.DB, listener *Listener) {
	t.Helper()

	topic := "probe." + uuidpb.Generate().AsString()
	probes := make(chan string, 1)
	defer listener.Subscribe(topic, probes)()

	deadline := time.After(xtesting.WaitTimeout)

	for {
		send(t, db, topic)

		select {
		case <-deadline:
			t.Fatal("timeout waiting for listener to start listening")
		case <-probes:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}
