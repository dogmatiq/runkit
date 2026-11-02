package signal_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestBus(t *testing.T) {
	t.Run("it delivers signals to subscribers of the topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer bus.Subscribe("topic-a", a)()
		defer bus.Subscribe("topic-b", b)()

		send(t, bus, "topic-a")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectNoReceive(t, "signal on b", b)

		send(t, bus, "topic-b")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
		xtesting.ExpectNoReceive(t, "signal on a", a)
	})

	t.Run("it delivers signals to every subscriber of the topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer bus.Subscribe("topic", a)()
		defer bus.Subscribe("topic", b)()

		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})

	t.Run("it delivers signals on topics beneath the subscribed topic", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		ch := make(chan string, 10)
		defer bus.Subscribe("a.b", ch)()

		send(t, bus, "a.b.c")
		send(t, bus, "a.bc")
		send(t, bus, "a")
		send(t, bus, "x.a.b")
		send(t, bus, "a.b")
		send(t, bus, "a.b.c.d")

		// The value received is the part of the topic beneath the subscribed
		// topic.
		xtesting.ExpectToReceiveN(t, "signals", ch, "c", "", "c.d")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it stops delivering signals after unsubscribing", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		ch := make(chan string, 10)
		witness := make(chan string, 10)

		unsubscribe := bus.Subscribe("topic", ch)
		defer bus.Subscribe("topic", witness)()

		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals", ch, "")

		unsubscribe()

		// The witness receiving the signal proves that it was dispatched after
		// the subscription was removed.
		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "", "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it drops signals when the subscriber's channel is full", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		ch := make(chan string, 1)
		witness := make(chan string, 10)

		defer bus.Subscribe("topic", ch)()
		defer bus.Subscribe("topic", witness)()

		send(t, bus, "topic")
		send(t, bus, "topic")
		send(t, bus, "topic")

		// The witness receiving every signal proves that the full channel did
		// not hold up dispatching.
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "", "", "")

		// Only the first signal fit in the subscriber's buffer.
		xtesting.ExpectToReceiveN(t, "signals", ch, "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it sends signals to every bus", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}
		other := &Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, bus.Run)
		xtesting.GoE(t, other.Run)
		waitForBus(t, bus)
		waitForBus(t, other)

		own := make(chan string, 10)
		received := make(chan string, 10)

		defer bus.Subscribe("topic", own)()
		defer other.Subscribe("topic", received)()

		if err := bus.Send(t.Context(), "topic.a"); err != nil {
			t.Fatal(err)
		}

		if err := bus.Send(t.Context(), "topic.b"); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(t, "signals on the sending bus", own, "a", "b")
		xtesting.ExpectToReceiveN(t, "signals on the other bus", received, "a", "b")
	})

	t.Run("it signals every subscriber when it starts listening", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		bus := &Bus{
			DB:     db,
			Logger: logger,
		}

		a := make(chan string, 10)
		b := make(chan string, 10)

		defer bus.Subscribe("a", a)()
		defer bus.Subscribe("a.b", b)()

		xtesting.GoE(t, bus.Run)

		// Nothing is sent; the signal comes from the bus itself, as
		// though it were on the subscribed topic, so that a subscriber that
		// read state before the bus was listening can read it again.
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})
}

// send sends a signal on the given topic.
func send(t testing.TB, bus *Bus, topic string) {
	t.Helper()

	if err := bus.Send(t.Context(), topic); err != nil {
		t.Fatal(err)
	}
}

// waitForBus blocks until the bus is receiving signals.
//
// Signals sent before the bus has started listening are lost, so a probe
// signal is sent on a unique topic until it is received.
func waitForBus(t testing.TB, bus *Bus) {
	t.Helper()

	topic := "probe." + uuidpb.Generate().AsString()
	probes := make(chan string, 1)
	defer bus.Subscribe(topic, probes)()

	deadline := time.After(xtesting.WaitTimeout)

	for {
		send(t, bus, topic)

		select {
		case <-deadline:
			t.Fatal("timeout waiting for bus to start listening")
		case <-probes:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}
